# Phase 3 : outbox transactionnel et relais (`cotisation-service`)

Environnement (18/11/2026, heure locale +01:00) : Spring Boot 4.1.2, spring-kafka 4.1.2, kafka-clients 4.2.2, H2 2.4.240 (versions : voir [versions.md](versions.md)). Producteur avant l'outbox : [phase-1-producteur.md](phase-1-producteur.md). Consommateur idempotent : [phase-3-idempotence.md](phase-3-idempotence.md).

Pour chaque point : ce qui est **constaté** (sortie réelle ou source lue), **déduit** et **non vérifié**.

## 1. Objectif
- Sortir Kafka du chemin HTTP : le `POST` répond 201 quand l'événement est **écrit en base** (table `outbox`), pas quand il est publié. Un relais planifié publie ensuite, avec une livraison **au moins une fois**.
- Garantie visée : écrire l'état métier et l'événement dans **une seule transaction**. **Limite actuelle** : le service n'a pas de table `cotisation` (option A, outbox seule) ; la transaction ne porte qu'un `INSERT` et l'atomicité conjointe n'est pas démontrable ici (section 8).
- **Commits** : `0e3f18c` (`refactor`, `idEvenement` généré dans le cas d'usage), `50eaf5c` (`feat`), `bbbab13` (`test`), puis ce journal.
- **Phase** : l'outbox est traité ici en phase 3 (apprentissage, H2). La phase 6 « Outbox Oracle » du brief et du README reste celle de la migration Oracle.

## 2. Problème résolu
- **Avant** : le `POST` publiait lui-même (`send(...).get(délai)`). Constaté ([phase-1-producteur.md](phase-1-producteur.md), section 7) : broker arrêté, 503 après 5,07 s et 5,02 s ; le 503 est **ambigu** : après un délai dépassé le message a pu être écrit, et un client qui rejoue crée un doublon.
- **Après** : la publication n'est plus dans la requête. Un échec de publication est rejoué par le relais ; un doublon possible porte le **même `idEvenement`** et est absorbé par l'idempotence du consommateur.
- **Dual-write (déduit)** : écrire une table métier **et** publier sur Kafka sans transaction commune perd ou duplique l'événement en cas de plantage entre les deux. Ce cas n'existe pas encore dans ce service (pas de table métier) ; l'outbox l'anticipe.

## 3. Modèle
```sql
CREATE TABLE IF NOT EXISTS outbox (
    id_evenement VARCHAR(36) PRIMARY KEY,
    topic VARCHAR(100) NOT NULL,
    cle VARCHAR(100) NOT NULL,
    payload CLOB NOT NULL,
    cree_le TIMESTAMP WITH TIME ZONE NOT NULL,
    envoye_le TIMESTAMP WITH TIME ZONE
);
```
- **`EvenementOutbox(UUID idEvenement, String topic, String cle, String payload)`** : ce que le relais lit. `payload` est le JSON **déjà sérialisé** ; le relais ne connaît pas le contrat.
- `cree_le` = `emiseLe` de la cotisation (pas l'instant de l'`INSERT`) ; `envoye_le` reste `NULL` tant que l'événement n'est pas publié ; `cle` = `idAdherent`.
- **Constaté** : `cree_le` et `envoye_le` sont relus à l'identique, millisecondes comprises (`OutboxJdbcTest`).

## 4. Technique
- **`@Transactional`** sur `EmettreCotisation.emettre` : l'`INSERT` unique (`JdbcClient`) participe à la transaction du `DataSourceTransactionManager` de Boot. **Constaté** : transaction active pendant `enregistrer` via le cas d'usage, inactive en appel direct (section 7).
- **Relais** : `@Scheduled(fixedDelayString = "${mutuelle.cotisation.outbox.delai-relais-ms}")`, 10 000 ms, lot de 50. Source : Javadoc de `@Scheduled` (« a fixed period between the end of the last invocation and the start of the next »), page lue en **7.1.0** alors que le projet résout la 7.0.10.
- **Lecture** : `WHERE envoye_le IS NULL ORDER BY cree_le, id_evenement FETCH FIRST ? ROWS ONLY`. Pas de `SKIP LOCKED`, pas de purge (décisions de l'étude).
- **Producteur** : `KafkaTemplate<String, String>` avec `StringSerializer` ; le payload n'est pas resérialisé (**constaté** : égalité exacte avec la valeur lue sur le topic).

## 5. Architecture
- **Ports** (`application/port/out`) : `Outbox` (`enregistrer`, `lireNonEnvoyes`, `marquerEnvoye`) et `PublicationCotisation` (`publier(topic, cle, payload)`).
- **Adaptateurs** : `OutboxJdbc` (convertit `Cotisation` en `CotisationEmise` et sérialise en JSON), `PublicationCotisationKafka`, `RelaiOutbox` (`adapter/in/planification`, déclenche le relais).
- **Cas d'usage** : `EmettreCotisation` (`Outbox`, `Clock`, générateur d'`UUID`) et `RelayerOutbox` (`Outbox`, `PublicationCotisation`, `Clock`, taille de lot).
- `domain/` et `application/` n'importent ni Jackson, ni `events-contract`, ni le nom du topic. `@Transactional` dans `application/` est une fuite pragmatique acceptée. `relais-actif=false` supprime le bean `RelaiOutbox` (démonstrations, tests).

## 6. Relais
- `relayer()` lit au plus `taille-lot` événements, puis pour chacun : `publier` (bloque jusqu'à l'accusé du broker, délai 10 s), **puis** `marquerEnvoye`.
- **Arrêt sur échec** : la première exception remonte et le reste du lot n'est pas tenté ; l'ordre par clé est conservé, les événements déjà marqués le restent (**constaté**, `RelayerOutboxTest`). `RelaiOutbox` journalise en WARN (`PublicationImpossibleException`) ou ERROR (autre `RuntimeException`) et le planificateur continue (**lu dans le code, non testé**).
- **Au moins une fois** : le marquage vient après l'accusé. Si le marquage échoue, l'événement reste « non envoyé » et est republié au passage suivant. **Constaté** (échec de marquage simulé) : 2 messages pour la clé, même partition (2), offsets 0 et 1, même `idEvenement`, même payload.
- **Lien avec le consommateur** : `prelevement-service` déduplique sur `idEvenement` (clé primaire). Il lit un JSON sans en-tête de type (`spring.json.use.type.headers: false`, type par défaut `CotisationEmise`, **constaté** dans son `application.yml`). La lecture du payload de l'outbox par ce consommateur n'est **pas testée** de bout en bout par un test automatisé ; elle est **relevée** une fois en démonstration 3 (section 10).

## 7. Tests (`mvn -B test -pl cotisation-service -am`, 18/11 : `BUILD SUCCESS`, 39 tests, 11 pour `events-contract`)
| Classe | Tests | Constate |
|---|---|---|
| `OutboxJdbcTest` | 7 | H2 mémoire, `schema.sql` de production : colonnes écrites ; format du payload (`montant` 50.00, `emiseLe` ISO) ; tri par `cree_le` puis `id_evenement` ; limite `FETCH FIRST ? ROWS ONLY` ; marquage ; `DuplicateKeyException` sur la clé primaire |
| `RelayerOutboxTest` | 3 | Mockito sur les deux ports : publication puis marquage dans l'ordre ; arrêt du lot au premier échec ; 0 sans rien à relayer |
| `OutboxIntegrationTest` | 1 | Relais **planifié** réel (200 ms), `@EmbeddedKafka`, sans espion : cas d'usage, base, relais, topic ; `idEvenement` de la ligne ; format du `JsonMapper` du contexte |
| `OutboxTransactionIntegrationTest` | 2 | Relais coupé, `relayer()` appelé par le test : transaction active `[true, false]` ; doublon par échec de marquage |
| `EmettreCotisationTest`, `PublicationCotisationKafka*Test` | 2, 10 | Adaptés au commit 2 (payload tel quel, sans en-tête de type) |
- **Choix (déduit, non expérimenté)** : deux classes d'intégration, car un stub posé sur l'espion `Outbox` pendant que le relais planifié l'appelle serait une course.

## 8. Ce qui est constaté, déduit, non vérifié
| | Constaté | Déduit | Non vérifié |
|---|---|---|---|
| SQL / JDBC | `FETCH FIRST ? ROWS ONLY` avec paramètre (limites 1, 2, 10) ; tri ; `Instant` aller-retour exact | | Mêmes requêtes sous Oracle |
| Clé primaire | `DuplicateKeyException`, ligne d'origine intacte | | Oracle |
| Format JSON | 4 propriétés ; `montant` à 50.00 ; `emiseLe` en chaîne ISO-8601 ; identique avec un `JsonMapper` nu et avec celui de Spring | | Autres réglages des deux mappers ; lecture par `prelevement-service` dans un test automatisé (relevée en démonstration 3, section 10) |
| Transaction | Active via le cas d'usage, inactive en appel direct ; 2 lignes validées | | Rollback conjoint avec une table métier (inexistante) |
| Relais | Le relais planifié publie puis marque ; arrêt du lot au premier échec | Latence max = `delai-relais-ms` + temps du lot | Relais sous charge ; `catch` de `RelaiOutbox` |
| Au moins une fois | Doublon par échec de marquage simulé (mêmes `idEvenement` et payload) ; relevé en démonstration 3 : republication provoquée par un `UPDATE`, doublon ignoré par `prelevement-service` | Un vrai plantage donne le même résultat | Vrai plantage de processus |
| Multi-instance | | Deux instances publieraient les mêmes lignes | `FOR UPDATE SKIP LOCKED` |
| Purge | | La table grossit | Purge des lignes envoyées |
| Broker | Preuves automatisées sur le broker embarqué 4.2.2 ; publication vers le broker 4.4.0 du compose relevée en démonstrations 2 et 3 (section 10) | | Broker réellement arrêté (la démonstration 2 vise un port fictif) |
| Oracle | | | Tout comportement Oracle |

## 9. Notes et limites
- **Latence** : un événement est publié au passage suivant, soit jusqu'à 10 s plus le temps de publier le lot. **Une seule instance**, pas de purge.
- **Base en mémoire par défaut** (aucune propriété `datasource` dans `application.yml`, **constaté**) : les lignes non envoyées disparaissent au redémarrage. L'outbox n'est durable qu'avec une base fichier ou Oracle (phase 6).
- **`cle` limitée à 100 caractères** : un `idAdherent` plus long fait échouer l'`INSERT` ; **déduit** : 500, `GestionErreursRest` ne traite pas les erreurs de base (non testé ici).
- **Gestionnaire 503** : plus atteint depuis le cas d'usage (il ne publie plus) ; conservé et testé avec un mock.
- **Ordre par clé** : celui de `cree_le` (horloge du service). Deux transactions concurrentes du même adhérent pourraient être validées dans l'ordre inverse (déduit, non exercé).
- **Démonstrations** : les trois démonstrations prévues (relais coupé, broker injoignable, doublon par replay) ont été exécutées par l'utilisateur le 18/11/2026 ; relevés et commandes à la section 10.

## 10. Démonstrations live
Exécutées par l'utilisateur le 18/11/2026 (heure locale +01:00) : jar de `cotisation-service` construit depuis le dépôt (code de production inchangé depuis `50eaf5c`), base H2 **fichier** `cotisation-service/target/demo-outbox` (`AUTO_SERVER=TRUE`), broker `apache/kafka:4.4.0` du compose. **Tout ce qui suit est un relevé de l'utilisateur** : je n'ai pas rejoué les services, et les sorties brutes (logs, `curl`, console) ne sont pas reproduites ici. Les commandes de la dernière sous-section sont reconstituées à partir du plan (section 9) et du relevé, et n'ont pas été rejouées non plus.

### Prérequis et pièges rencontrés

#### PowerShell et `curl` : 400 Bad Request
- Sur PowerShell, `curl` est un alias de `Invoke-WebRequest` : il faut utiliser `curl.exe` explicitement.
- PowerShell transforme aussi les guillemets des arguments : `curl.exe -d "{\"key\":\"value\"}"` envoie les antislashs littéralement au serveur, qui répond 400 « Unexpected character ('\' (code 92)) » (relevé).
- Contournement : écrire le corps JSON dans un fichier temporaire avec `Set-Content`, puis `curl.exe -d "@body.json"` :
```powershell
Set-Content -Path body.json -Value '{"idAdherent":"ADH-DEMO-001","montant":150.00}' -NoNewline
curl.exe -s -w "`n%{http_code}" -X POST http://localhost:8081/cotisations -H "Content-Type: application/json" -d "@body.json"
```
- **Constaté dans le code** : `POST /cotisations` (port 8081), corps `idAdherent` et `montant`, statut 201. `body.json` est un fichier jetable, créé à la racine du dépôt et non versionné.

#### Maven `spring-boot:run` et URL JDBC
- Relevé : avec `spring-boot:run`, l'URL `jdbc:h2:file:…` passée en argument pose problème. L'explication de l'utilisateur est que Maven interprète les `:` comme des coordonnées d'artefact ; elle n'est **pas vérifiée** dans la documentation de Maven.
- Contournement (relevé) : construire le jar avec `mvn -B package`, puis lancer `java -jar`.
- **Déduit, non rencontré** : sous PowerShell, une URL contenant `;AUTO_SERVER=TRUE` doit être entourée de guillemets, sinon `;` sépare deux commandes.

### Démo 1 : relais désactivé (`relais-actif=false`)
Service lancé avec `--mutuelle.cotisation.outbox.relais-actif=false` et la base H2 fichier. Relevé :
- `POST /cotisations` répond 201 ;
- la table `outbox` contient une ligne avec `envoye_le = NULL` ;
- le topic `cotisations.emises` ne contient aucun nouveau message.
- Conclusion : sans relais, la cotisation est enregistrée mais jamais envoyée à Kafka.

### Démo 2 : broker injoignable, puis rétabli
Service lancé avec `--spring.kafka.bootstrap-servers=localhost:9999` (port fictif) et le relais actif. Relevé :
- le relais tente de publier la ligne restante de la démo 1 et échoue à chaque passage ;
- logs WARN `Connection to node -1 (localhost/127.0.0.1:9999) could not be established` ;
- après 5 s, un `TimeoutException`, puis le relais journalise `outbox : publication impossible, nouvel essai au prochain passage` (message **constaté dans le code**, `RelaiOutbox`) ;
- le service reste debout et le cycle se répète (`fixedDelay` de 10 s) ;
- après arrêt et relance avec le vrai broker (`localhost:9092`), le relais rattrape : `outbox : 1 événement(s) publié(s)` ; `envoye_le` passe de `NULL` à un horodatage ;
- Conclusion : le client HTTP est isolé de la panne de Kafka, et le relais rattrape dès le retour du broker.
- **Précision (déduit, non mesuré)** : `fixedDelay` compte 10 s entre la **fin** d'un passage et le début du suivant. Un passage en échec dure environ 5 s (`max.block.ms`), donc le début-à-début serait plutôt de l'ordre de 15 s que de 10 s.
- **Déduit** : la ligne a survécu à l'arrêt du service parce que la base est un fichier (avec la base en mémoire par défaut, elle aurait disparu, section 9).

### Démo 3 : doublon par replay (au moins une fois + idempotence)
`cotisation-service` (relais actif, vrai broker) et `prelevement-service` lancés en même temps. Manipulation : `UPDATE outbox SET envoye_le = NULL` sur l'événement déjà envoyé (`72ea2e51-c068-4925-8222-0e391d7ef03c`). Relevé :
- `cotisation-service` : le relais republie l'événement, `outbox : 1 événement(s) publié(s)` ;
- `prelevement-service` : premier traitement à `offset=2`, `decision=REJETE`, `INSERT` réussi ;
- `prelevement-service` : seconde réception à `offset=3`, `doublon ignoré idEvenement=72ea2e51…` (`DuplicateKeyException` absorbée) ;
- Conclusion : le consommateur idempotent (étude 1) absorbe les doublons que le mécanisme au moins une fois de l'outbox (étude 2) peut produire.
- La partition n'est pas relevée ; même clé donc même partition (**déduit**).
- Le message produit par l'outbox a donc été lu et traité par le vrai consommateur (type par défaut `CotisationEmise`, sans en-tête de type).

### Ce que ces démonstrations ne prouvent pas
- **Un vrai plantage** entre l'accusé du broker et le marquage : le doublon est provoqué par un `UPDATE` manuel, pas par un arrêt brutal du processus.
- **Un broker réellement arrêté** : la démo 2 vise un port fictif (le symptôme côté client est le même, **déduit**).
- **La périodicité mesurée du relais**, le comportement sous charge, le multi-instance, Oracle.

### Commandes utilisées
Depuis la racine du dépôt, sous PowerShell. Build :
```powershell
mvn -B package -pl cotisation-service -am -DskipTests
```
Lancement de `cotisation-service` (démo 1 ; démo 2 : retirer `relais-actif=false`, car le relais est actif par défaut, et ajouter `--spring.kafka.bootstrap-servers=localhost:9999` ; puis relancer sans cette option) :
```powershell
java -jar cotisation-service/target/cotisation-service-0.1.0-SNAPSHOT.jar `
  "--spring.datasource.url=jdbc:h2:file:./cotisation-service/target/demo-outbox;AUTO_SERVER=TRUE" `
  --spring.datasource.username=sa `
  --spring.sql.init.mode=always `
  --mutuelle.cotisation.outbox.relais-actif=false
```
Création d'une cotisation : voir le `curl.exe` avec `body.json` plus haut. Lecture de l'outbox et replay (même répertoire de travail, car l'URL est relative) :
```powershell
$h2 = "$env:USERPROFILE\.m2\repository\com\h2database\h2\2.4.240\h2-2.4.240.jar"
$url = "jdbc:h2:file:./cotisation-service/target/demo-outbox;AUTO_SERVER=TRUE"
java -cp $h2 org.h2.tools.Shell -url $url -user sa -sql "SELECT id_evenement, cle, cree_le, envoye_le FROM outbox ORDER BY cree_le"
java -cp $h2 org.h2.tools.Shell -url $url -user sa -sql "UPDATE outbox SET envoye_le = NULL WHERE id_evenement = '72ea2e51-c068-4925-8222-0e391d7ef03c'"
```
Lecture du topic :
```powershell
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server :9092 --topic cotisations.emises --from-beginning --timeout-ms 5000
```
