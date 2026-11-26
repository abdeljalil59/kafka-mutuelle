# Phase 3 : outbox transactionnel et relais (`cotisation-service`)

> **Mise à jour du 24/11/2026.** L'outbox de `cotisation-service` est passé de H2 à Oracle Free 23ai (`schema.sql`, variables `COTISATION_DB_URL`, `COTISATION_DB_USER`, `COTISATION_DB_PASSWORD`). **L'outbox est le seul état du service** : il n'a pas de table `cotisation`, la transaction ne porte qu'un `INSERT`. Ce journal décrit l'état du 18/11/2026 (H2) ; les sections 3, 4, 7, 8 et 9 sont mises à jour, la section 10 reste le relevé daté du 18/11 sur H2, la section 11 consigne la migration vers Oracle, la section 12 l'index de lecture du relais, la section 13 la démonstration sur l'Oracle local (24/11/2026) et la section 14 le relais à plusieurs instances (`SKIP LOCKED`, 26/11/2026). Les sections 4, 5, 6, 7, 8, 9 et 11 sont mises à jour en conséquence.

Environnement (18/11/2026, heure locale +01:00) : Spring Boot 4.1.2, spring-kafka 4.1.2, kafka-clients 4.2.2, H2 2.4.240 (versions : voir [versions.md](versions.md)). Producteur avant l'outbox : [phase-1-producteur.md](phase-1-producteur.md). Consommateur idempotent : [phase-3-idempotence.md](phase-3-idempotence.md). Environnement de la migration Oracle (24/11/2026) : section 11.

Pour chaque point : ce qui est **constaté** (sortie réelle ou source lue), **déduit** et **non vérifié**.

## 1. Objectif
- Sortir Kafka du chemin HTTP : le `POST` répond 201 quand l'événement est **écrit en base** (table `outbox`), pas quand il est publié. Un relais planifié publie ensuite, avec une livraison **au moins une fois**.
- Garantie visée : écrire l'état métier et l'événement dans **une seule transaction**. **Limite actuelle** : le service n'a pas de table `cotisation` (option A, outbox seule) ; la transaction ne porte qu'un `INSERT` et l'atomicité conjointe n'est pas démontrable ici (section 8).
- **Commits** : `0e3f18c` (`refactor`, `idEvenement` généré dans le cas d'usage), `50eaf5c` (`feat`), `bbbab13` (`test`), puis ce journal.
- **Phase** : l'outbox est traité ici en phase 3 (apprentissage, sur H2) ; il a été migré sur Oracle le 24/11/2026 (phase 6 du brief, section 11).

## 2. Problème résolu
- **Avant** : le `POST` publiait lui-même (`send(...).get(délai)`). Constaté ([phase-1-producteur.md](phase-1-producteur.md), section 7) : broker arrêté, 503 après 5,07 s et 5,02 s ; le 503 est **ambigu** : après un délai dépassé le message a pu être écrit, et un client qui rejoue crée un doublon.
- **Après** : la publication n'est plus dans la requête. Un échec de publication est rejoué par le relais ; un doublon possible porte le **même `idEvenement`** et est absorbé par l'idempotence du consommateur.
- **Dual-write (déduit)** : écrire une table métier **et** publier sur Kafka sans transaction commune perd ou duplique l'événement en cas de plantage entre les deux. Ce cas n'existe pas encore dans ce service (pas de table métier) ; l'outbox l'anticipe.

## 3. Modèle
```sql
CREATE TABLE IF NOT EXISTS outbox (
    id_evenement VARCHAR2(36 CHAR)  NOT NULL,
    topic        VARCHAR2(100 CHAR) NOT NULL,
    cle          VARCHAR2(100 CHAR) NOT NULL,
    payload      CLOB               NOT NULL,
    cree_le      TIMESTAMP WITH LOCAL TIME ZONE NOT NULL,
    envoye_le    TIMESTAMP WITH LOCAL TIME ZONE,
    CONSTRAINT pk_outbox PRIMARY KEY (id_evenement)
);

CREATE INDEX IF NOT EXISTS idx_outbox_a_envoyer ON outbox (envoye_le, cree_le, id_evenement);
```
- DDL H2 du 18/11/2026, remplacé : `VARCHAR(36) PRIMARY KEY`, dates en `TIMESTAMP WITH TIME ZONE`, pas d'index. Raison du type des dates : section 12.
- **`EvenementOutbox(UUID idEvenement, String topic, String cle, String payload)`** : ce que le relais lit. `payload` est le JSON **déjà sérialisé** ; le relais ne connaît pas le contrat.
- `cree_le` = `emiseLe` de la cotisation (pas l'instant de l'`INSERT`) ; `envoye_le` reste `NULL` tant que l'événement n'est pas publié ; `cle` = `idAdherent`.
- **Constaté** : `cree_le` et `envoye_le` sont relus à l'identique, millisecondes comprises (`OutboxJdbcTest`, sur H2 le 18/11, sur Oracle le 24/11/2026).

## 4. Technique
- **`@Transactional`** sur `EmettreCotisation.emettre` : l'`INSERT` unique (`JdbcClient`) participe à la transaction du `DataSourceTransactionManager` de Boot. **Constaté** : transaction active pendant `enregistrer` via le cas d'usage, inactive en appel direct (section 7).
- **Relais** : `@Scheduled(fixedDelayString = "${mutuelle.cotisation.outbox.delai-relais-ms}")`, 10 000 ms, lot de 50 ; un déclenchement enchaîne des passages tant que le précédent a réservé des événements, au plus `passages-max` (100) : section 14.4. Source : Javadoc de `@Scheduled` (« a fixed period between the end of the last invocation and the start of the next »), page lue en **7.1.0** alors que le projet résout la 7.0.10.
- **Lecture** : jusqu'au 24/11/2026, `WHERE envoye_le IS NULL ORDER BY cree_le, id_evenement FETCH FIRST ? ROWS ONLY`, sans verrou. Depuis `acc5d5d` : réservation en deux temps avec `FOR UPDATE SKIP LOCKED` (section 14.2). Pas de purge ; index de lecture : section 12.
- **Producteur** : `KafkaTemplate<String, String>` avec `StringSerializer` ; le payload n'est pas resérialisé (**constaté** : égalité exacte avec la valeur lue sur le topic).

## 5. Architecture
- **Ports** (`application/port/out`) : `Outbox` (`enregistrer`, `reserverNonEnvoyes`, `marquerEnvoye` ; `reserverNonEnvoyes` remplace `lireNonEnvoyes` depuis `acc5d5d`) et `PublicationCotisation` (`publier(topic, cle, payload)`).
- **Adaptateurs** : `OutboxJdbc` (convertit `Cotisation` en `CotisationEmise` et sérialise en JSON), `PublicationCotisationKafka`, `RelaiOutbox` (`adapter/in/planification`, déclenche le relais et enchaîne les passages, section 14.4).
- **Cas d'usage** : `EmettreCotisation` (`Outbox`, `Clock`, générateur d'`UUID`) et `RelayerOutbox` (`Outbox`, `PublicationCotisation`, `Clock`, taille de lot).
- `domain/` et `application/` n'importent ni Jackson, ni `events-contract`, ni le nom du topic. `@Transactional` dans `application/` est une fuite pragmatique acceptée. `relais-actif=false` supprime le bean `RelaiOutbox` (démonstrations, tests).

## 6. Relais
- `relayer()` réserve au plus `taille-lot` événements (au plus un par clé, verrouillés jusqu'à la fin de la transaction du passage), puis pour chacun : `publier` (bloque jusqu'à l'accusé du broker, délai 10 s), **puis** `marquerEnvoye`.
- **Arrêt sur échec** : la première exception remonte et le reste du lot n'est pas tenté ; l'ordre par clé est conservé, les événements déjà marqués le restent (**constaté**, `RelayerOutboxTest`, et `OutboxTransactionIntegrationTest` sur Oracle : la transaction est validée, `noRollbackFor`). `RelaiOutbox` journalise en WARN (`PublicationImpossibleException`) ou ERROR (autre `RuntimeException`), arrête la boucle de relance, et le planificateur continue (**constaté**, `RelaiOutboxTest`).
- **Au moins une fois** : le marquage vient après l'accusé. Si le marquage échoue, l'événement reste « non envoyé » et est republié au passage suivant. **Constaté** (échec de marquage simulé) : 2 messages pour la clé, même partition (2), offsets 0 et 1, même `idEvenement`, même payload. **Depuis `e0b04f1`** : un plantage du processus avant la validation du passage republie tout le lot, et non plus seulement l'événement en cours (section 14.3, déduit).
- **Lien avec le consommateur** : `prelevement-service` déduplique sur `idEvenement` (clé primaire). Il lit un JSON sans en-tête de type (`spring.json.use.type.headers: false`, type par défaut `CotisationEmise`, **constaté** dans son `application.yml`). La lecture du payload de l'outbox par ce consommateur n'est **pas testée** de bout en bout par un test automatisé ; elle est **relevée** une fois en démonstration 3 (section 10).

## 7. Tests (`mvn -B test -pl cotisation-service -am`, 18/11 : `BUILD SUCCESS`, 39 tests, 11 pour `events-contract` ; 24/11/2026 : `BUILD SUCCESS`, 56 tests pour `cotisation-service`, 21 pour `events-contract` ; 26/11/2026 : `BUILD SUCCESS`, 71 tests pour `cotisation-service`, commande sans `-am`)
| Classe | Tests | Constate |
|---|---|---|
| `OutboxJdbcTest` | 10 (26/11/2026 ; 8 au lancement 7) | Oracle Free dans un conteneur (`OracleIntegrationTest`), `schema.sql` de production : colonnes écrites ; format du payload (`montant` 50.00, `emiseLe` ISO) ; tri par `cree_le` puis `id_evenement` ; limite `FETCH FIRST ? ROWS ONLY` ; marquage ; `DuplicateKeyException` sur la clé primaire ; structure de l'index (colonnes et positions 1 à 3 via `USER_IND_COLUMNS`). Au 18/11 : 7 tests sur H2 mémoire ; depuis `acc5d5d` : au plus un événement par clé et par réservation, refus de réserver hors transaction |
| `RelayerOutboxTest` | 3 | Mockito sur les deux ports : publication puis marquage dans l'ordre ; arrêt du lot au premier échec ; 0 sans rien à relayer |
| `OutboxIntegrationTest` | 2 (lancement 7) | Relais **planifié** réel (200 ms), `@EmbeddedKafka`, sans espion : cas d'usage, base, relais, topic ; `idEvenement` de la ligne ; format du `JsonMapper` du contexte ; export Prometheus (`/actuator/prometheus`). Sur Oracle, un `@BeforeEach` vide `outbox` (déduit, non testé sans lui : sans lui, le vrai relais publierait les lignes laissées par une autre classe sur le même conteneur) |
| `OutboxTransactionIntegrationTest` | 6 (26/11/2026 ; 2 au lancement 7) | Sur Oracle. Relais coupé, `relayer()` appelé par le test : transaction active `[true, false]` ; doublon par échec de marquage ; depuis `e0b04f1` et `acc5d5d` : état de transaction du relais, validation des événements publiés avant l'échec (2 cas), réservabilité de l'événement en échec au passage suivant |
| `EmettreCotisationTest`, `PublicationCotisationKafka*Test` | 2, 10 | Adaptés au commit 2 (payload tel quel, sans en-tête de type) |
| `RelaisPlusieursInstancesTest` | 2 | Sur Oracle, deux transactions sur deux connexions : réservations disjointes puis chaque événement réservé une fois ; ordre de publication par clé entre deux instances |
| `RelaiOutboxTest` | 7 | Mockito : boucle de relance, plafond, arrêt à la première exception (journalisation inchangée), au moins un passage |
- **Choix (déduit, non expérimenté)** : deux classes d'intégration, car un stub posé sur l'espion `Outbox` pendant que le relais planifié l'appelle serait une course.
- **Correction du chiffre du 18/11** : « 1 » pour `OutboxIntegrationTest` était périmé depuis le 19/11/2026, date du commit qui a ajouté le test d'export Prometheus (`d672866`, constaté par `git log -S`) ; ce n'est pas un effet de la migration.
- **Les trois `@SpringBootTest`** héritent d'`OracleIntegrationTest` : un conteneur Oracle par JVM de test, `@DynamicPropertySource` pour l'URL, l'utilisateur et le mot de passe ; un contexte Spring par classe (`@DirtiesContext`).
- **Détail au 24/11/2026 (lancement 7)** : `CotisationTest` 18, `OutboxJdbcTest` 8, `CotisationControllerTest` 4, `ConvertisseurAvroTest` 6, `PublicationCotisationKafkaTest` 6, `PublicationCotisationKafkaIntegrationTest` 3, `PublicationCotisationKafkaSansBrokerTest` 2, `RelayerOutboxTest` 3, `EmettreCotisationTest` 2, `OutboxIntegrationTest` 2, `OutboxTransactionIntegrationTest` 2, soit 56. Les autres chiffres du tableau sont ceux du 18/11.
- **Détail au 26/11/2026** : `CotisationTest` 18, `OutboxJdbcTest` 10, `CotisationControllerTest` 4, `ConvertisseurAvroTest` 6, `PublicationCotisationKafkaTest` 6, `PublicationCotisationKafkaIntegrationTest` 3, `PublicationCotisationKafkaSansBrokerTest` 2, `RelayerOutboxTest` 3, `EmettreCotisationTest` 2, `OutboxIntegrationTest` 2, `OutboxTransactionIntegrationTest` 6, `RelaisPlusieursInstancesTest` 2, `RelaiOutboxTest` 7, soit 71.

## 8. Ce qui est constaté, déduit, non vérifié
| | Constaté | Déduit | Non vérifié |
|---|---|---|---|
| SQL / JDBC | `FETCH FIRST ? ROWS ONLY` avec paramètre (limites 1, 2, 10) ; tri ; `Instant` aller-retour exact (H2, 18/11) ; mêmes requêtes sous Oracle Free 23.26.4 avec `OffsetDateTime` UTC (`OutboxJdbcTest`, 24/11/2026) | | |
| Clé primaire | `DuplicateKeyException`, ligne d'origine intacte (H2 le 18/11, Oracle le 24/11/2026) | | |
| Format JSON | 4 propriétés ; `montant` à 50.00 ; `emiseLe` en chaîne ISO-8601 ; identique avec un `JsonMapper` nu et avec celui de Spring | | Autres réglages des deux mappers ; lecture par `prelevement-service` dans un test automatisé (relevée en démonstration 3, section 10) |
| Transaction | Active via le cas d'usage, inactive en appel direct ; 2 lignes validées | | Rollback conjoint avec une table métier (inexistante) |
| Relais | Le relais planifié publie puis marque ; arrêt du lot au premier échec ; `catch` et boucle de relance de `RelaiOutbox` (`RelaiOutboxTest`) | Latence max = `delai-relais-ms` + temps du lot | Relais sous charge prolongée (une démonstration de 10 000 événements : section 14.7) |
| Au moins une fois | Doublon par échec de marquage simulé (mêmes `idEvenement` et payload) ; relevé en démonstration 3 : republication provoquée par un `UPDATE`, doublon ignoré par `prelevement-service` (relevé du 18/11 sur le code d'alors : le journal « doublon ignoré » n'existe plus, section 13) ; rejoué sur Oracle le 24/11/2026 : une seule ligne en base, un second résultat publié, compteur à 1 (section 13) | Un vrai plantage donne le même résultat | Vrai plantage de processus |
| Multi-instance | `RelaisPlusieursInstancesTest` (2 tests sur Oracle 23.26.4) ; démonstration à deux instances sur Oracle 23.4.0.24.05 : 10 000 événements publiés une fois, ordre par clé conservé, aucune erreur Oracle (section 14.7) | Sans `SKIP LOCKED`, deux instances publieraient les mêmes lignes | Arrêt brutal d'une instance ; lot par défaut de 50 ; course entre les deux temps ; plusieurs brokers (section 14.10) |
| Purge | | La table grossit | Purge des lignes envoyées |
| Broker | Preuves automatisées sur le broker embarqué 4.2.2 ; publication vers le broker 4.4.0 du compose relevée en démonstrations 2 et 3 (section 10, 18/11) ; broker réellement arrêté puis rétabli (`docker stop kafka`, `docker start kafka`), relais rattrapant (section 13, 24/11/2026) | | |
| Oracle | Conteneur 23.26.4 : `OutboxJdbcTest`, flux complet, transaction, doublon par échec de marquage (lancement 7, 56 tests verts) ; sonde du 24/11/2026 : `FETCH FIRST … FOR UPDATE SKIP LOCKED` accepté sur 23.26.4.0.0 (section 14.5) ; Oracle local 23.4.0.24.05 : démonstration en direct, 3 scénarios, types des colonnes et colonnes de l'index relevés (24/11/2026, section 13), `ORA-02014` pour cette forme (relevé de session, section 11), démonstration à deux instances sans erreur Oracle (26/11/2026, section 14.7) | | Oracle local : aucun test automatisé |

## 9. Notes et limites
- **Latence** : un événement est publié au passage suivant, soit jusqu'à 10 s plus le temps de publier le lot (le relais enchaîne ensuite les passages tant qu'il reste des têtes de clé : section 14.4). **Plusieurs instances possibles** (section 14), pas de purge.
- **Base Oracle** (`spring.datasource.*` de `application.yml`, variables `COTISATION_DB_*`) : la table survit au redémarrage. Au 18/11/2026, la base était H2 en mémoire par défaut (aucune propriété `datasource` dans `application.yml`, constaté alors) et les lignes non envoyées disparaissaient au redémarrage.
- **`cle` limitée à 100 caractères** : un `idAdherent` plus long fait échouer l'`INSERT` ; **déduit** : 500, `GestionErreursRest` ne traite pas les erreurs de base (non testé ici).
- **Gestionnaire 503** : plus atteint depuis le cas d'usage (il ne publie plus) ; conservé et testé avec un mock.
- **Ordre par clé** : celui de `cree_le` (horloge du service). Deux transactions concurrentes du même adhérent pourraient être validées dans l'ordre inverse (déduit, non exercé). Plusieurs instances du service ne changent pas cette limite (section 14.9).
- **Démonstrations** : les trois démonstrations prévues (relais coupé, broker injoignable, doublon par replay) ont été exécutées par l'utilisateur le 18/11/2026 ; relevés et commandes à la section 10. Les trois mêmes scénarios ont été rejoués sur Oracle le 24/11/2026 : section 13.

## 10. Démonstrations live
**Démonstrations du 18/11/2026 sur une base H2 fichier ; relevés conservés tels quels.** Les démonstrations du 24/11/2026 sur Oracle sont à la section 13 ; la démonstration 3 ci-dessous n'est plus reproductible telle quelle (le code d'idempotence a changé le 19/11/2026, section 13). Exécutées par l'utilisateur le 18/11/2026 (heure locale +01:00) : jar de `cotisation-service` construit depuis le dépôt (code de production inchangé depuis `50eaf5c`), base H2 **fichier** `cotisation-service/target/demo-outbox` (`AUTO_SERVER=TRUE`), broker `apache/kafka:4.4.0` du compose. **Tout ce qui suit est un relevé de l'utilisateur** : je n'ai pas rejoué les services, et les sorties brutes (logs, `curl`, console) ne sont pas reproduites ici. Les commandes de la dernière sous-section sont reconstituées à partir du plan (section 9) et du relevé, et n'ont pas été rejouées non plus.

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

## 11. Migration vers Oracle (24/11/2026)

Environnement : Oracle Free 23ai ; tests sur le conteneur `gvenzl/oracle-free:23.26.4-slim-faststart` (Testcontainers) ; Oracle local de l'utilisateur en 23.4.0.24.05 ; `ojdbc17` (version gérée par le BOM de Spring Boot). Résultat : `mvn -B -pl cotisation-service -am test` : 56 tests pour `cotisation-service`, `BUILD SUCCESS` (lancement 7 du 24/11/2026, journal non versionné ; conteneur démarré en 2 min 17 s).

### Ce qui change (constaté dans les sources)
- `application.yml` : `spring.datasource.url|username|password` = `${COTISATION_DB_URL:jdbc:oracle:thin:@localhost:1522/freepdb1}`, `${COTISATION_DB_USER:kafka_mutuelle}`, `${COTISATION_DB_PASSWORD:…}` (valeurs par défaut de développement, les mêmes que pour `prelevement-service` ; les commandes `sqlplus` de la section 13 les reprennent) ; `driver-class-name: oracle.jdbc.OracleDriver` ; `spring.sql.init.mode: always`. **Non testé sans `always`** : le commentaire équivalent de `prelevement-service/src/main/resources/application.yml` (lignes 42 à 45, « Oracle n'est pas une base embarquée : sans "always", schema.sql ne serait pas appliqué ») n'est pas non plus sourcé dans la documentation de Spring Boot.
- `schema.sql` : schéma Oracle (section 3), appliqué à chaque démarrage. **Constaté** : `CREATE TABLE IF NOT EXISTS` et `CREATE INDEX IF NOT EXISTS` passent sans erreur sur 23.26.4 (aucun `ORA-` dans le lancement 7, où chaque contexte réapplique le script ; journal non versionné) ; relevé de l'utilisateur : acceptés aussi sur 23.4.0.24.05.
- **Une table existante n'est pas modifiée** (déduit du sens de `IF NOT EXISTS`, non testé) : une ancienne table `OUTBOX` aux types `TIMESTAMP WITH TIME ZONE` resterait telle quelle et l'index se poserait dessus (section 12). Elle est à supprimer avant le premier démarrage.
- La dépendance H2 est retirée : aucune référence restante dans `cotisation-service/src` (recherche faite avant le retrait).

### Piège : `java.time.Instant` en paramètre (constaté)
`ojdbc17` refuse un `Instant` lié comme paramètre. Premier lancement d'`OutboxJdbcTest` sur Oracle (lancement 1, journal non versionné) : 7 tests sur 7 en erreur, message exact :
```
PreparedStatementCallback; uncategorized SQLException for SQL [INSERT INTO outbox (id_evenement, topic, cle, payload, cree_le) VALUES (?, ?, ?, ?, ?)]; SQL state [99999]; error code [17004]; ORA-17004: Type de colonne non valide
```
**Solution (constatée, tests verts)** : lier `OffsetDateTime.ofInstant(instant, ZoneOffset.UTC)` pour `cree_le` et `envoye_le` (`OutboxJdbc`) ; les tests relisent avec `rs.getObject(colonne, OffsetDateTime.class)`. Même découverte que pour `prelevement-service` ([phase-5-etape-1-oracle.md](phase-5-etape-1-oracle.md)). Avec le correctif, les `@SpringBootTest` encore sur H2 à cette étape passaient aussi (constaté, 55 tests, lancement 2, journal non versionné).

### `FOR UPDATE SKIP LOCKED` et `ORA-02014` (traité à la section 14)
- **Relevé de session sur Oracle local 23.4.0.24.05** : `… FETCH FIRST 2 ROWS ONLY FOR UPDATE SKIP LOCKED` donne `ORA-02014`.
- **Constaté depuis (sonde du 24/11/2026)** : la même forme est **acceptée** sur 23.26.4.0.0, mais ne garantit pas l'ordre par clé entre sessions ; la sous-sélection `IN` laisse une seconde session sans aucune ligne alors que des lignes sont libres. La forme retenue est décrite à la section 14.2.
- **Traité** par les commits `e0b04f1`, `acc5d5d` et `f383fd5` : voir la section 14. La lecture est désormais une réservation verrouillée ; tout reste « au moins une fois ».

### Ce que prouve `[true, false]` (`OutboxTransactionIntegrationTest`)
- **Prouvé sur Oracle** : pendant `enregistrer`, une transaction est active quand l'appel passe par le cas d'usage (`@Transactional`, `EmettreCotisation.java:25`) et inactive en appel direct ; les 2 lignes sont validées.
- **Déduit, non exécuté** : sans `@Transactional`, la liste serait `[false, false]` et l'assertion échouerait.
- **Non prouvé** : l'annulation de l'`INSERT` si le cas d'usage échoue après `enregistrer`. Il n'y a pas de test de rollback ; **déduit** : peu de valeur ici, car la transaction ne porte qu'un `INSERT` et un tel test ne vérifierait que le gestionnaire de transactions de Spring. Il en faudrait un dès qu'une table métier existe.

## 12. Index de lecture du relais (mesuré le 24/11/2026 sur des copies)

### Décision
`cree_le` et `envoye_le` en `TIMESTAMP WITH LOCAL TIME ZONE` ; index `idx_outbox_a_envoyer (envoye_le, cree_le, id_evenement)` (`schema.sql`). `OutboxJdbcTest.l_index_de_lecture_porte_les_colonnes_dans_l_ordre_du_relais` vérifie la **structure** (colonnes et positions 1 à 3 via `USER_IND_COLUMNS`), pas le plan (trop dépendant des statistiques et de la version).

**Constaté : ce test a d'abord été rouge** (lancement 4, journal non versionné), avec des colonnes `TIMESTAMP WITH TIME ZONE` : `USER_IND_COLUMNS` renvoyait `["1:SYS_NC00007$", "2:SYS_NC00008$", "3:ID_EVENEMENT"]` au lieu de `["1:ENVOYE_LE", "2:CREE_LE", "3:ID_EVENEMENT"]`. **C'est ce rouge qui a révélé que l'index n'était pas posé sur les colonnes brutes** ; la sonde ci-dessous a ensuite mesuré les conséquences. Avec `TIMESTAMP WITH LOCAL TIME ZONE`, le même test, non modifié, passe (lancement 7, 56 tests).

### Protocole (refaisable)
Image `gvenzl/oracle-free:23.26.4-slim-faststart`, conteneur jetable ; test jetable, non versionné, supprimé après usage, sur des **copies** de tables, jamais sur `outbox`. Relevé brut : [plan-relais-sonde.txt](plan-relais-sonde.txt). **†** : d'après le code du test jetable (supprimé), non vérifiable dans `plan-relais-sonde.txt` ni dans `lancement6.log`.
1. **Volume** : 50 000 lignes par table (lisible : `NUM_ROWS=50000`) ; payload d'environ 200 octets † ; `INSERT … SELECT … FROM dual CONNECT BY LEVEL <= 50000` † ; 500 lignes non envoyées (lisible : `NUM_NULLS(envoye_le)=500`, 5 000 pour la dernière variante), **les plus récentes par `cree_le`** † ; les autres ont `envoye_le` = `cree_le` + 1 s † ; dates en UTC (`TIMESTAMP '2026-11-01 00:00:00 +00:00' + NUMTODSINTERVAL(LEVEL, 'SECOND')`) †.
2. **Statistiques** calculées avant chaque plan, y compris après la création de l'index (le fichier montre `NUM_ROWS` avant et après l'index) ; appel `DBMS_STATS.GATHER_TABLE_STATS(USER, table, cascade => TRUE)` †.
3. **Plan** : `EXPLAIN PLAN FOR` la requête du relais avec liaison (`?` = 50), puis `DBMS_XPLAN.DISPLAY(NULL, NULL, 'TYPICAL')` (lisible dans les en-têtes du fichier), **dans la même connexion** †. Avec une connexion par instruction : `Error: cannot fetch last explain plan from PLAN_TABLE` (constaté au lancement 4, journal non conservé).
4. **Exécution réelle** avec `/*+ GATHER_PLAN_STATISTICS */` (lisible dans le texte SQL des curseurs), puis `DBMS_XPLAN.DISPLAY_CURSOR(NULL, NULL, 'ALLSTATS LAST')` (lisible dans les en-têtes). Aucun hint de forçage d'index (lisible dans les textes SQL).
5. **Lectures logiques** : `consistent gets` lus dans `V$MYSTAT` joint à `V$STATNAME` †, avant et après une exécution précédée d'une première exécution d'analyse † ; surcoût de la lecture elle-même mesuré à 0 (lisible : `surcout de la mesure seule=0`).
6. **Droits** : une connexion `SYSTEM` du conteneur jetable accorde `SELECT_CATALOG_ROLE` à `kafka_mutuelle` (lisible : première ligne du fichier) **avant** l'ouverture de la connexion applicative †. Sans ce droit : `User has no SELECT privilege on V$SESSION` (lancement 5, journal non versionné).
7. **Variantes** (cinq tables, lisibles dans les titres du fichier) : `TIMESTAMP WITH TIME ZONE` (requête d'origine, puis requête réécrite avec `SYS_EXTRACT_UTC`), `TIMESTAMP(6)`, `TIMESTAMP WITH LOCAL TIME ZONE`, colonne drapeau `envoye NUMBER(1)` (`WHERE envoye = 0`, index `(envoye, cree_le, id_evenement)`, lisible), `TIMESTAMP(6)` avec 5 000 lignes non envoyées. Même DDL que `outbox` hors type des dates ou colonne drapeau †.

### Constaté (copies de 50 000 lignes, 50 lignes lues)
| Variante | Lectures avant l'index | Lectures après | Plan après l'index |
|---|---|---|---|
| `TIMESTAMP WITH TIME ZONE`, requête d'origine | 3 595 | **3 595** | inchangé : `TABLE ACCESS FULL` (même `Plan hash value` 2730945086) |
| même table, requête réécrite `SYS_EXTRACT_UTC(...)` | non mesuré | **41** | `INDEX RANGE SCAN` |
| `TIMESTAMP(6)` | 3 595 | **41** | `INDEX RANGE SCAN`, `access("ENVOYE_LE" IS NULL)` |
| **`TIMESTAMP WITH LOCAL TIME ZONE`** (retenu) | 3 652 | **41** | `INDEX RANGE SCAN`, `access("ENVOYE_LE" IS NULL)` |
| colonne drapeau `envoye = 0` | 3 595 | 41 | `INDEX RANGE SCAN`, `access("ENVOYE"=0)` : aucun gain, écartée |
| `TIMESTAMP(6)`, 5 000 lignes non envoyées | 3 595 | **383** | `INDEX RANGE SCAN` (25 lectures d'index au lieu de 4) |
- **Piège** : sur `TIMESTAMP WITH TIME ZONE`, Oracle indexe `SYS_EXTRACT_UTC("ENVOYE_LE")` et `SYS_EXTRACT_UTC("CREE_LE")` (`USER_IND_EXPRESSIONS`, colonnes virtuelles `SYS_NC00007$`, `SYS_NC00008$`). La requête compare la colonne brute : **l'index n'est pas utilisé, sans aucune erreur**. Sur `TIMESTAMP WITH LOCAL TIME ZONE` et `TIMESTAMP(6)`, l'index porte les colonnes brutes.
- **Un tri subsiste** dans tous les plans avec index (`SORT ORDER BY STOPKEY`), y compris avec la colonne drapeau (condition d'égalité) : l'hypothèse « `IS NULL` n'est pas traité comme une égalité » est écartée. Le tri porte sur les lignes en attente (500 lues à l'accès par rowid pour 50 renvoyées ; 5 000 pour 383 lectures), en mémoire. **Cause inexpliquée, non creusée.**
- Les segments de table et d'index ont les mêmes tailles pour toutes les variantes (30 408 704 et 3 145 728 octets) : **déduit** que ce sont des tailles d'étendues, qui ne départagent pas les variantes.

### Déduit (non mesuré)
- Le plan de la **vraie** table `outbox` avec la liaison de `OutboxJdbc` n'est pas mesuré : seule la copie `TIMESTAMP WITH LOCAL TIME ZONE` de la sonde l'est (même DDL).
- Le coût de lecture suit l'**arriéré**, pas la taille du lot (41 lectures pour 500 lignes en attente, 383 pour 5 000).
- **Compromis de l'index** : il couvre aussi les lignes déjà envoyées (la table n'est pas purgée), donc il croît avec elle ; chaque `marquerEnvoye` déplace l'entrée de l'index (la ligne quitte la zone `envoye_le` NULL).

## 13. Démonstration sur Oracle local (24/11/2026)

Démonstration exécutée le 24/11/2026 (heure locale +01:00) sur l'Oracle local 23.4.0.24.05, avec `kafka` et `schema-registry` du compose ; les sorties sont reproduites par extraits. Trois terminaux : T1 (`cotisation-service`), T2 (commandes), T3 (`prelevement-service`). Les commandes `sqlplus` utilisent les valeurs de développement de l'`application.yml`.

### Préparation
- **Constaté** : mémoire disponible avant : 1 978 Mo (1 816 Mo avant le scénario 3). Avant le premier démarrage, `SELECT … FROM user_tab_columns WHERE table_name = 'OUTBOX'` : « aucune ligne sélectionnée » (table absente). `CREATE TABLE IF NOT EXISTS` et `CREATE INDEX IF NOT EXISTS` de `schema.sql` ont été acceptés sur 23.4 au premier démarrage : la table et l'index existent ensuite, et aucune erreur n'a empêché le service de démarrer et d'écrire.
- **Constaté (relevé après les trois scénarios, `user_tab_columns` et `user_ind_columns`, 6 lignes pour la table)** : `ID_EVENEMENT`, `TOPIC`, `CLE` en `VARCHAR2`, `PAYLOAD` en `CLOB`, `CREE_LE` et `ENVOYE_LE` en `TIMESTAMP(6) WITH LOCAL TIME ZONE`. Index `IDX_OUTBOX_A_ENVOYER` : position 1 `ENVOYE_LE`, position 2 `CREE_LE`, position 3 `ID_EVENEMENT` (colonnes brutes, sans colonne virtuelle `SYS_NC…`) ; `PK_OUTBOX` : position 1 `ID_EVENEMENT`. C'est exactement ce que `schema.sql` déclare et ce que vérifie `OutboxJdbcTest.l_index_de_lecture_porte_les_colonnes_dans_l_ordre_du_relais`.
- **Non vérifié** : les longueurs des `VARCHAR2` (la sortie de `sqlplus` a été coupée par la largeur de colonne) ; le plan d'exécution sur cet Oracle (non relevé).
```powershell
mvn -B package -pl cotisation-service -am -DskipTests
```
```powershell
(Get-CimInstance Win32_PerfFormattedData_PerfOS_Memory).AvailableMBytes
```
Lecture de l'outbox :
```powershell
@("SET LINESIZE 200", "SELECT id_evenement, cle, cree_le, envoye_le FROM outbox ORDER BY cree_le;") | sqlplus -s kafka_mutuelle/kafka_mutuelle@localhost:1522/freepdb1
```
Lecture du topic (valeurs Avro binaires ; clé, partition et offset lisibles) :
```powershell
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic cotisations.emises --from-beginning --timeout-ms 10000 --formatter-property print.key=true --formatter-property print.partition=true --formatter-property print.offset=true
```

### Scénario 1 : relais désactivé
T1 :
```powershell
java -jar cotisation-service/target/cotisation-service-0.1.0-SNAPSHOT.jar --mutuelle.cotisation.outbox.relais-actif=false
```
T2 :
```powershell
curl.exe -s -w "`n%{http_code}" -X POST http://localhost:8081/cotisations -H "Content-Type: application/json" -d "@cotisation-demo.json"
```
- **Constaté** : HTTP 201, corps `{"idAdherent":"ADH-DEMO-01","montant":42.50,"emiseLe":"2026-11-24T14:45:20.711919900Z"}`. `OUTBOX` : `id_evenement` 6f554e03-b580-4eec-9d7f-304513ab4cb5, `cle` ADH-DEMO-01, `CREE_LE` 24/11/26 15:45:20,711920, `ENVOYE_LE` vide (une seule lecture). La ligne a ensuite été supprimée (`DELETE FROM outbox; COMMIT;`, 1 ligne, validation effectuée), pour que le relais du scénario 2 ne la publie pas.
- **Constaté indirectement** : l'identifiant 6f554e03 n'apparaît pas dans le relevé complet de `cotisations.emises` fait ensuite (scénario 2).
- **Non vérifié** : le topic n'a pas été relevé avant et après ce scénario ; la lecture de l'outbox 20 s plus tard n'a pas été capturée (`ENVOYE_LE` toujours vide : déduit du relais désactivé, non constaté).
- **(d) Fuseau (constaté)** : `CREE_LE` s'affiche à 15:45:20,711920 alors que `emiseLe` vaut 14:45:20.7119199Z. **Déduit** : même instant, affiché dans le fuseau de la session Oracle (+01:00, Europe/Paris) ; une valeur `TIMESTAMP WITH LOCAL TIME ZONE` est restituée dans le fuseau de la session (documentation d'Oracle non consultée). Les nanosecondes sont arrondies à la microseconde (711919900 ns affichées 711920 µs).

### Scénario 2 : Kafka réellement arrêté, puis rétabli
T1 :
```powershell
java -jar cotisation-service/target/cotisation-service-0.1.0-SNAPSHOT.jar
```
T2 :
```powershell
docker stop kafka
```
T2 (même `curl.exe` que ci-dessus), puis :
```powershell
docker start kafka
```
- **Constaté** : `docker stop kafka` affiche `kafka`. Le `POST` répond **201** pendant l'arrêt (`emiseLe` 2026-11-24T14:47:02.835477500Z). `OUTBOX` : `id_evenement` e0ae7c52-44cf-4607-8479-b0b13cd5f3af, `CREE_LE` 24/11/26 15:47:02,835478, `ENVOYE_LE` vide.
- **Constaté (journal de T1, 15:47)** : producteur déconnecté de `localhost:9092` dès 15:47:19, des dizaines de lignes INFO `Rebootstrapping with [localhost/127.0.0.1:9092]` et WARN `Connection to node -1 … could not be established. Node may not be available.` ; 15:47:31.197 ERROR `LoggingProducerListener` : `Exception thrown when sending a message with key='ADH-DEMO-01' and payload='{"idEvenement": "e0ae7c52-…` to topic cotisations.emises: `org.apache.kafka.common.errors.TimeoutException: Topic cotisations.emises not present in metadata after 5000 ms.` ; 15:47:31.218 WARN `RelaiOutbox` : `outbox : publication impossible, nouvel essai au prochain passage : Échec de la publication de l'événement de ADH-DEMO-01`. Les INFO de reconnexion reprennent à 15:47:41.257. 15:47:45.151 `Cluster ID: 4L6g3nShT-eMCtK--X86sw` (Kafka de retour) ; 15:47:45.281 `ProducerId set to 5000 with epoch 0` ; 15:47:48.110 INFO `RelaiOutbox` : `outbox : 1 événement(s) publié(s)` (un seul : la table avait été vidée).
- **Constaté (topic, relevé complet vers 15:48)** : partition 1, offset 7, clé ADH-DEMO-01, valeur Avro commençant par l'identifiant e0ae7c52-… ; les offsets 0 à 6 de la partition 1 sont intacts après `docker stop` puis `docker start` (données conservées) ; 6f554e03 est absent.
- **Déduit** : le passage suivant du relais était à 15:47:41 (délai fixe de 10 s). `ENVOYE_LE` n'a **pas** été lu directement après la publication ; entre 15:47:48 et le replay du scénario 3 (15:51:38, environ 230 s, une vingtaine de passages), le topic ne contient qu'un exemplaire (offset 7) : la ligne a donc été marquée au premier passage.
- **Non vérifié** : l'heure du `docker start kafka` n'a pas été relevée, donc le délai entre le retour du broker et la publication n'est pas mesuré.
- **(a) Message trompeur** : `Topic cotisations.emises not present in metadata after 5000 ms` suggère un topic absent. Le topic existait (offsets 0 à 6 intacts) : la cause réelle est le broker injoignable, que seules les lignes WARN `Connection to node -1` montrent.
- **(b) Une panne, deux lignes** : une même tentative produit un ERROR de `LoggingProducerListener` (spring-kafka) puis, 21 ms plus tard, un WARN de `RelaiOutbox`.
- **(c) Bruit** : les INFO `Rebootstrapping` se répètent par dizaines pendant l'arrêt et noient les deux lignes utiles.

### Scénario 3 : replay et doublon
T3 (avant la remise à NULL) :
```powershell
mvn -pl prelevement-service spring-boot:run
```
T2 :
```powershell
@("UPDATE outbox SET envoye_le = NULL WHERE id_evenement = (SELECT id_evenement FROM outbox WHERE cree_le = (SELECT MAX(cree_le) FROM outbox));", "COMMIT;") | sqlplus -s kafka_mutuelle/kafka_mutuelle@localhost:1522/freepdb1
```
- **Constaté** : mémoire avant : 1 816 Mo. Journal de `prelevement-service` : 15:50:58 `Invoking InitProducerId … transactionalId=tx-prelevement-0`, `ProducerId set to 3002 with epoch 7` ; 15:50:59.102 `CotisationEmiseListener` : `partition=1 offset=7 cle=ADH-DEMO-01 idEvenement=e0ae7c52-44cf-4607-8479-b0b13cd5f3af montant=42.50 decision=ACCEPTE`.
- **Constaté** : `UPDATE` : « 1 ligne mise à jour », validation effectuée. `OUTBOX` : `CREE_LE` 24/11/26 15:47:02,835478 (inchangé), `ENVOYE_LE` 24/11/26 15:51:38,299211. Journal : 15:51:38.313 `CotisationEmiseListener` : `partition=1 offset=8 … idEvenement=e0ae7c52-44cf-4607-8479-b0b13cd5f3af … decision=ACCEPTE` (14 ms après le marquage). Le fichier de journal s'arrête là : **aucune ligne « doublon »**.
- **Constaté** : `cotisations.emises` : deux messages de même clé et même `idEvenement`, partition 1, offsets 7 et 8. `DECISION_PRELEVEMENT` : `COUNT` = 1 pour cet `idEvenement` ; `id_adherent` ADH-DEMO-01, `montant` 42,5, `decision` ACCEPTE, `decide_le` 24/11/26 14:50:58,861613 +00:00 (la **première** décision ; ce type, `TIMESTAMP WITH TIME ZONE`, garde le décalage +00:00, contrairement à `OUTBOX`, voir le scénario 1). Compteur Prometheus (`curl.exe http://localhost:8083/actuator/prometheus`) : `prelevement_doublons_ignores_total 1.0`. `prelevements.resultats` (`--from-beginning`, filtre sur e0ae7c52) : **deux** messages, même clé, partition 1, offsets 2 et 4, valeurs Avro binaires commençant par e0ae7c52-… et dont les fins d'octets diffèrent.
- **Déduit, non vérifié** : l'offset 3 manquant est un marqueur de commit de transaction (le producteur de `prelevement-service` est transactionnel, `transactionalId=tx-prelevement-0`, alors que `cotisations.emises`, produit sans transaction, a des offsets consécutifs) ; vérifiable avec `kafka-dump-log`, non fait. **Déduit** : les fins d'octets diffèrent parce que `decideLe` est recalculé par `clock.instant()` à chaque traitement (`TraiterDemandePrelevement.java:34`) ; non vérifié par décodage Avro.
- **(e) La démonstration 3 du 18/11 n'est plus reproductible** : la ligne `doublon ignoré idEvenement=…` a été supprimée par `88b9ac2` (19/11/2026 13:51, `MERGE`), après le relevé du 18/11 (section 10, conservé tel quel). **Constaté (code lu)** : `JournalDecisionsJdbc.java:17-23` (`MERGE … WHEN NOT MATCHED THEN INSERT`, sans `WHEN MATCHED`), `:57-65` (résultat 0 = doublon, seul signal : le compteur `prelevement.doublons.ignores` ; la classe n'a pas de `Logger`). Le seul INFO du listener (`CotisationEmiseListener.java:34-36`) est émis pour chaque message, doublon compris.
- **(f) Un doublon produit un second résultat** : `TraiterDemandePrelevement.java:35-36` appelle `enregistrer` puis `publier` sans condition (commentaires `:27-30`). Le résultat republié a le même `idEvenement` (`PublicationResultats.java:10` : republier la même décision « donne le même `idEvenement`, que le consommateur aval utilise pour dédoublonner »). Les deux messages de `prelevements.resultats` le confirment. **Constaté en direct le 24/11/2026** (relevé de session, sans journal conservé ; `stats-service` en marche, résultat republié avec le même `idEvenement`, séance distincte de cette démonstration) : `ADH-DEMO-01` compte 2 acceptés pour un montant de 85.0 alors que le topic contient trois résultats, dont un doublon ([kafka-streams.md](kafka-streams.md), §8 ter A). **Constaté (code lu)** : `impaye-service` ignore un `idEvenement` déjà enregistré (`PersistanceImpayeJdbc.java:32-49` : `INSERT` et capture de `DuplicateKeyException`, compteur `impaye.doublons.ignores` ; contrainte `uq_impaye_id_evenement`, `schema.sql:15`). **Non exercé en direct** : `impaye-service` et `stats-service` n'ont pas tourné pendant cette démonstration (l'exercice de `stats-service` a eu lieu séparément) ; `impaye-service` n'enregistre que les rejets (README, tableau des modules) et la décision de la démonstration était ACCEPTE.

### Ce que cette démonstration ne prouve pas
- **Un vrai plantage** entre l'accusé du broker et le marquage : le replay est provoqué par un `UPDATE` manuel.
- **Le plan d'exécution** de la requête du relais sur la vraie table `outbox` : non relevé (section 12).
- **Plusieurs instances du relais** et `SKIP LOCKED` : cette démonstration leur est antérieure ; ils sont traités à la section 14 (relevé du 26/11/2026).

## 14. Plusieurs instances du relais (24 au 26/11/2026)

Trois commits : `e0b04f1` (une transaction par passage), `acc5d5d` (réservation avec `FOR UPDATE SKIP LOCKED`, au plus un événement par clé et par passage), `f383fd5` (relance immédiate du relais). **Constaté** : `mvn -B -pl cotisation-service test` du 26/11/2026 : `BUILD SUCCESS`, 71 tests pour `cotisation-service`.

Dans cette section : **constaté** = sortie réelle ou source lue ; **déduit** = raisonnement non expérimenté ; **non vérifié** = ni l'un ni l'autre. Les relevés de sonde et de démonstration sont des **relevés de session** : les sondes (code jetable) et les journaux ne sont pas dans le dépôt.

### 14.1 Objectif
- Avant : un seul relais. Deux instances lisaient les mêmes lignes non envoyées et publiaient chaque événement deux fois (déduit, section 8).
- Visé : plusieurs instances sur la même base, **sans doublon** et **en conservant l'ordre des événements d'une même clé** (Kafka ne garantit l'ordre que dans une partition ; la clé de l'événement détermine la partition).
- Hors objectif : débit maximal, purge de la table, prise en charge d'un événement non convertible (section 14.9).

### 14.2 Forme retenue : deux temps
Source : `OutboxJdbc.reserverNonEnvoyes`.

1. **Temps 1, sans verrou** : les « têtes de clé », c'est-à-dire le plus ancien événement non envoyé de chaque clé, du plus ancien au plus récent, au plus `taille-lot × 3` (constante `FACTEUR_CANDIDATS`).
```sql
SELECT id_evenement FROM (SELECT id_evenement, cree_le, ROW_NUMBER() OVER (PARTITION BY cle ORDER BY cree_le, id_evenement) AS rang FROM outbox WHERE envoye_le IS NULL) WHERE rang = 1 ORDER BY cree_le, id_evenement FETCH FIRST ? ROWS ONLY
```
2. **Temps 2, un verrou par candidat**, jusqu'à `taille-lot` lignes obtenues.
```sql
SELECT id_evenement, topic, cle, payload FROM outbox WHERE id_evenement = ? AND envoye_le IS NULL FOR UPDATE SKIP LOCKED
```
Une ligne déjà verrouillée par une autre instance est sautée (aucune attente). Les verrous durent jusqu'à la fin de la transaction du passage (section 14.3).

**Pourquoi cette forme (constaté, section 14.5)**
- Au plus une tête par clé et par passage : tant qu'un événement n'est pas marqué envoyé, son verrou empêche une autre instance de publier un événement plus récent de la même clé. L'ordre par clé est conservé entre instances.
- `FETCH FIRST` n'est jamais combiné avec `FOR UPDATE` : cette combinaison est refusée sur Oracle 23.4.0.24.05 (`ORA-02014`, relevé de session) et ne garantit pas l'ordre par clé (section 14.5).
- Aucun nouvel index : la requête du temps 1 coûte autant de lectures que la requête d'origine (section 14.6).

**Jusqu'où cela parallélise-t-il (déduit)** : au plus une tête par clé et par passage. Avec 10 clés et un lot de 50, une instance verrouille les 10 têtes ; l'autre n'obtient rien tant que la première n'a pas validé. Le parallélisme est borné par le nombre de clés distinctes en attente et par la taille du lot. `FACTEUR_CANDIDATS = 3` couvre jusqu'à 3 instances ; au-delà, seul le débit baisse (une instance peut ne rien réserver), jamais l'exactitude (déduit du code).

### 14.3 Une transaction par passage
`RelayerOutbox.relayer()` est `@Transactional` : réservation, publications et marquages sont validés ensemble. `OutboxJdbc.reserverNonEnvoyes` refuse de s'exécuter hors transaction (`IllegalStateException`, constaté par `OutboxJdbcTest`), car les verrous seraient relâchés avant le marquage.

Conséquences :
- **Constaté** (`OutboxTransactionIntegrationTest`) : au premier échec de publication (`PublicationImpossibleException`) ou de conversion (`IllegalArgumentException`), la transaction est **validée** (`noRollbackFor`) avant que l'exception remonte. Les événements déjà publiés et marqués le restent, les verrous des suivants sont libérés, et ils sont réservables au passage suivant.
- **Verrous tenus pendant la publication** (lu dans le code) : une autre instance saute ces lignes pendant tout le passage.
- **Pas de timeout de transaction** (décision, lue dans le code). **Déduit** : la durée d'un passage est bornée par le délai de publication par événement (`delai-publication-ms`, 10 s) multiplié par la taille du lot, soit jusqu'à environ 500 s au lot de 50 si chaque publication réussit juste avant son délai. Verrous et connexion de base de données sont tenus d'autant. **Non mesuré.**
- **Fenêtre de doublons plus large** (déduit) : un plantage du processus **avant la validation** annule tout le passage, y compris les marquages des événements déjà publiés dans ce passage. Ils sont republiés (même `idEvenement`) au passage suivant. Avant ce changement, seul l'événement en cours pouvait l'être. Le consommateur doit donc être idempotent (section 6 : `prelevement-service` déduplique sur `idEvenement`, `impaye-service` aussi). **Non exercé** : aucun plantage réel n'a été provoqué.
- Une exception autre (erreur SQL au marquage, par exemple) annule le passage. **Constaté** pour l'événement concerné : il est republié (`republie_l_evenement_quand_le_marquage_echoue_une_fois`, un seul événement dans ce test). **Déduit** : l'annulation des marquages précédents du même passage, qui découle de la transaction ; elle n'est pas testée avec plusieurs événements.

### 14.4 Boucle de relance
`RelaiOutbox.relayer()` (déclenché par `@Scheduled`, délai fixe `delai-relais-ms` inchangé) exécute des passages **tant que le précédent a réservé au moins un événement**, au plus `mutuelle.cotisation.outbox.passages-max` (100 par défaut). Il fait toujours au moins un passage, même si la propriété est inférieure à 1 (aucune validation de propriété, comme les autres). Au plafond ou à la première exception, la boucle s'arrête ; la journalisation d'une exception est inchangée (WARN pour `PublicationImpossibleException`, ERROR pour le reste) ; le déclenchement suivant reprend après le délai fixe.

**Pourquoi** : un passage ne publie qu'une tête par clé. **Déduit** : 5 000 événements sur 10 clés demandent 500 passages. Sans la boucle, un passage par déclenchement : 500 × 10 s, soit environ 83 minutes. Avec la boucle : 5 déclenchements de 100 passages, soit environ 4 pauses de 10 s. Au plus `passages-max × taille-lot` événements par déclenchement (5 000 par défaut).

**Constaté** (`RelaiOutboxTest`, 7 tests) : un passage à 0 donne un seul passage ; les passages 3, 2, 0 donnent exactement 3 passages ; des passages toujours positifs s'arrêtent au plafond ; une exception au 2e passage arrête la boucle (3 types d'exception) ; un plafond nul donne un passage. Chaque variante fautive (un seul passage, boucle seulement sur lot plein, sans plafond, relance après exception, boucle `for`) a fait échouer au moins un test (relevés du commit `f383fd5`).

**Journal** : une ligne INFO `outbox : n événement(s) publié(s)` par passage non vide, soit jusqu'à 100 lignes par déclenchement (constaté en démonstration : 1 200 lignes pour 6 000 événements).

### 14.5 Ce qui a été constaté sur Oracle (formes essayées)
Sondes jetables sur le conteneur `gvenzl/oracle-free:23.26.4-slim-faststart` (« Oracle Database 23ai Free Release 23.26.4.0.0 »), 24/11/2026 : 6 lignes puis 12 lignes, 3 à 4 sessions, payload `CLOB` de 12 000 caractères. Les sondes ne sont pas versionnées.

| Forme | Résultat sur 23.26.4.0.0 (constaté) | Sur 23.4.0.24.05 |
|---|---|---|
| `ORDER BY … FETCH FIRST n ROWS ONLY FOR UPDATE SKIP LOCKED` | Acceptée. Mais la session 3 obtient l'événement 7 (clé A) pendant que la session 1 tient l'événement 1 (clé A) : **l'ordre par clé est inversé** | `ORA-02014` (relevé de session) |
| `WHERE id IN (SELECT … FETCH FIRST n) FOR UPDATE SKIP LOCKED` | La session 1 réserve 3 lignes ; la session 2 n'obtient **rien** alors que 3 lignes sont libres | Non refait |
| Vue triée + `ROWNUM <= n` + `FOR UPDATE` | `ORA-02014` | Non refait |
| `FOR UPDATE SKIP LOCKED` sans limite, lecture arrêtée après n lignes | **Tout le résultat** est verrouillé : la session 2 n'obtient rien, la session 3 non plus | Non refait |
| `IN (liste de candidats)` | Verrouille plus de lignes que le lot (5 pour un lot de 3) | Non refait |
| Un verrou par ligne candidate (**forme retenue, temps 2**) | Sessions 1 et 2 : 3 lignes chacune, disjointes, sans inversion | Réservation et requête analytique vérifiées à la main sur 23.4 (relevé de session) ; **le code Java a tourné sur 23.4 lors de la démonstration (section 14.7)** |

- Le code de production n'emploie que la dernière forme. La forme `NOT EXISTS + FETCH FIRST + FOR UPDATE`, essayée aussi, est refusée sur 23.4 (relevé de session) ; elle n'a pas non plus été retenue pour son coût (section 14.6).
- **Non vérifié, source externe** : `ROWNUM` compterait les lignes verrouillées par d'autres sessions ; je n'ai pas relu cette affirmation dans la documentation d'Oracle.
- **Constaté** : la lecture du `CLOB` sous charge n'a donné aucun payload vide ou différent (600 passages, 4 fils, forme `IN`).

### 14.6 Mesures sur copies (24/11/2026)
Copies de 50 000 lignes (payload de 300 caractères), lot de 50, statistiques collectées avant chaque mesure, Oracle 23.26.4.0.0. Unité : lectures cohérentes (`consistent gets`).
Scénarios : **A** 500 événements non envoyés sur 500 clés ; **B** 5 000 sur 10 clés ; **C** 5 000 sur 5 000 clés.

| Étape 1 (250 candidats) | A | B | C |
|---|---|---|---|
| Requête d'origine (50 lignes, référence) | 57 | 543 | 543 |
| `NOT EXISTS`, index actuel seul | 14 783 | 10 572 | **1 363 343** |
| `NOT EXISTS`, avec index `(cle, envoye_le, cree_le, id_evenement)` | 1 561 | 6 791 | 6 080 |
| **Analytique (retenue), index actuel seul** | **57** | **543** | **543** |
| Analytique, avec l'index sur la clé | 57 | 527 | 530 |

- **Constaté** : la requête analytique coûte autant que la requête d'origine, sans nouvel index. Elle a été mesurée avec 250 candidats (5 × le lot) ; la production en lit 3 × le lot.
- **Constaté, étape 2** : 50 lignes verrouillées une à une = 200 lectures (environ 4 par ligne) et 51 allers-retours (environ un par ligne) ; 10 lignes en scénario B : 40 lectures et 11 allers-retours.
- **Constaté, contention** (100 têtes, lot de 50, **étape 1 en `NOT EXISTS`**, index actuel seul) : le relais seul coûte 979 lectures et 55 allers-retours ; le second relais, dont 50 candidats sont verrouillés par le premier, coûte 1 814 lectures et 105 allers-retours. **Déduit** : une ligne sautée coûte environ 17 lectures et 1 aller-retour. Cette mesure n'a pas été refaite avec la requête analytique.
- **Constaté, coût d'`INSERT`** (50 000 lignes, lots de 100) avec l'index sur la clé en plus : 29 158 ms contre 22 923 ms, 15 810 contre 24 407, 16 271 contre 18 353 (index actuel seul contre index actuel + index sur la clé, trois répétitions alternées), soit de −21 % à +54 % selon la répétition : non concluant. L'index n'a pas été ajouté.
- **Non mesuré** : la forme retenue avec la contention de deux instances ; un plan sur la vraie table `outbox`.

### 14.7 Démonstration à deux instances (26/11/2026)
**Relevé de session**, journaux temporaires supprimés après lecture.
- **Montage (constaté)** : Oracle Database 23ai Free 23.4.0.24.05 local ; backlog de **10 000 événements insérés en SQL** (le chemin d'écriture de l'outbox, API REST et cas d'usage, n'est donc **pas** exercé), 20 clés `DEMO-01` à `DEMO-20`, `cree_le` croissant avec le numéro de l'événement, topic dédié de 3 partitions. Deux instances de `cotisation-service` sur la même machine et la même base : ports 8081 (A) et 8091 (B), `taille-lot=5`, `delai-relais-ms=2000`, `passages-max` par défaut (100). Le lot par défaut (50) n'a **pas** été exercé : avec 50 têtes de clé ou moins, une seule instance réserve tout (déduit, section 14.2).
- **Version du code (déduit des journaux, non contrôlé par une sortie conservée)** : 1 200 passages en 153 s avec un délai de 2 s ne sont possibles qu'avec la boucle de `f383fd5` ; zéro doublon avec deux instances n'est possible qu'avec la réservation de `acc5d5d`.
- **Journaux (constaté)** : A, 1 200 lignes `outbox : n événement(s) publié(s)`, 6 000 événements, de 09:48:53,689 à 09:51:26,547 ; B, 800 lignes, 4 000 événements, de 09:49:50,159 à 09:51:26,580 (heure locale +01:00). Total : 10 000. Chaque ligne indique 5 événements (aucun passage partiel). B a publié pour la première fois environ 56 s après A ; recouvrement d'environ 96 s.
- **Base après coup (constaté)** : 10 000 lignes envoyées, 0 non envoyée.
- **Topic (constaté)** : somme des offsets de fin = 10 000 exactement, donc aucun doublon (le producteur n'est pas transactionnel, aucun marqueur de transaction ne s'ajoute aux offsets).
- **Ordre (constaté)** : 20 clés, 500 messages chacune, ordre croissant. Pour la clé `DEMO-k`, le premier événement est k et le dernier 9 980 + k (lecture brute du topic avec `kafka-console-consumer`, identifiant lu dans les octets du message).
- **Journaux d'application (constaté)** : aucune ligne `ORA-`, `ERROR` ni `WARN`, donc aucun `ORA-02014` : le code Java de production fonctionne sur 23.4.0.24.05 avec deux relais concurrents.
- **Verdict** : concluant, selon un critère fixé avant l'exécution (les deux instances publient, la première publication de B précède la dernière de A, comptes et ordre conformes, aucune erreur Oracle).
- **Non exercé** : lot par défaut de 50 ; arrêt brutal d'une instance (délai de libération des verrous, doublons) ; chemin d'écriture de l'outbox ; course entre les deux temps de la réservation (déduit, non testé) ; broker multiple ; relecture des valeurs Avro par un consommateur (`kafka-avro-console-consumer` a échoué avec `Unknown magic byte!` ; **non vérifié** : cause probable, la clé de type chaîne décodée comme de l'Avro).

### 14.8 Reproduire la démonstration
Sous PowerShell, depuis la racine du dépôt, avec `kafka` et `schema-registry` du compose. Les commandes `sqlplus` utilisent les valeurs de développement de l'`application.yml`.

Mémoire disponible, build, connexion :
```powershell
(Get-CimInstance Win32_PerfFormattedData_PerfOS_Memory).AvailableMBytes
```
```powershell
mvn -B package -pl cotisation-service -am -DskipTests
```
```powershell
$c = "kafka_mutuelle/kafka_mutuelle@localhost:1522/freepdb1"
```
Avant tout, l'outbox ne doit contenir aucune ligne non envoyée (sinon les instances les publieraient sur leurs topics réels) :
```powershell
@("SELECT COUNT(*) - COUNT(envoye_le) AS non_envoyees FROM outbox;") | sqlplus -s $c
```
Topic de démonstration (3 partitions) :
```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic cotisations.demo-relais --partitions 3 --replication-factor 1
```
Backlog (instances non démarrées ; attendu : `10000 rows created.`) :
```powershell
@("INSERT INTO outbox (id_evenement, topic, cle, payload, cree_le) SELECT id, 'cotisations.demo-relais', cle, JSON_OBJECT('idEvenement' VALUE id, 'idAdherent' VALUE cle, 'montant' VALUE 10.00, 'emiseLe' VALUE '2026-11-25T10:00:00Z'), TIMESTAMP '2026-11-25 00:00:00' + NUMTODSINTERVAL(n, 'SECOND') FROM (SELECT LEVEL AS n, 'dddddddd-0000-0000-0000-' || LPAD(LEVEL, 12, '0') AS id, 'DEMO-' || LPAD(MOD(LEVEL - 1, 20) + 1, 2, '0') AS cle FROM dual CONNECT BY LEVEL <= 10000);", "COMMIT;") | sqlplus -s $c
```
Deux consoles (A puis, une fois la mémoire contrôlée, B) :
```powershell
java -jar cotisation-service/target/cotisation-service-0.1.0-SNAPSHOT.jar --server.port=8081 --mutuelle.cotisation.outbox.taille-lot=5 --mutuelle.cotisation.outbox.delai-relais-ms=2000 "--logging.file.name=$env:TEMP\demo-relais-A.log"
```
```powershell
java -jar cotisation-service/target/cotisation-service-0.1.0-SNAPSHOT.jar --server.port=8091 --mutuelle.cotisation.outbox.taille-lot=5 --mutuelle.cotisation.outbox.delai-relais-ms=2000 "--logging.file.name=$env:TEMP\demo-relais-B.log"
```
Pourquoi `taille-lot=5` : avec 20 clés et le lot par défaut de 50, la première instance verrouillerait toutes les têtes et la seconde ne publierait rien (section 14.2).

Vérifications. Base, avec la version d'Oracle (attendu : `10000 | 10000 | 0`) :
```powershell
@("SET LINESIZE 200", 'SELECT banner_full FROM v$version;', "SELECT COUNT(*) AS total, COUNT(envoye_le) AS envoyees, COUNT(*) - COUNT(envoye_le) AS non_envoyees FROM outbox WHERE topic = 'cotisations.demo-relais';") | sqlplus -s $c
```
Offsets de fin du topic (attendu : somme égale à 10 000) :
```powershell
docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic cotisations.demo-relais | ForEach-Object { [int]($_ -split ':')[2] } | Measure-Object -Sum
```
Comptes par instance (attendu : la somme des deux colonnes `evenements` vaut 10 000) et erreurs (attendu : aucune sortie) :
```powershell
foreach ($i in 'A','B') { $l = @(Select-String -Path "$env:TEMP\demo-relais-$i.log" -Encoding utf8 -Pattern 'outbox : (\d+) \S+\(s\)'); $m = $l | ForEach-Object { [int]$_.Matches[0].Groups[1].Value } | Measure-Object -Sum; "$i lignes=$($m.Count) evenements=$($m.Sum) premiere=$($l[0].Line.Substring(0,29)) derniere=$($l[-1].Line.Substring(0,29))" }
```
```powershell
Select-String -Path "$env:TEMP\demo-relais-A.log","$env:TEMP\demo-relais-B.log" -Encoding utf8 -Pattern 'ORA-| ERROR | WARN '
```
Ordre par clé. Lecture du topic, avec la clé en début de ligne (ne pas ajouter `print.partition` ni `print.offset` : ils décaleraient le début de ligne et casseraient le motif ci-dessous) :
```powershell
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic cotisations.demo-relais --from-beginning --timeout-ms 30000 --consumer-property enable.auto.commit=false --property print.key=true --property key.separator=";" | Out-File -Encoding utf8 $env:TEMP\demo-relais-topic.txt
```
Regroupement par clé (l'identifiant de l'événement est une chaîne, lisible dans les octets de la valeur Avro). Attendu : 20 lignes, `Messages` = 500, `Croissant` = `True` ; pour `DEMO-k`, `Premier` = k et `Dernier` = 9 980 + k :
```powershell
Get-Content $env:TEMP\demo-relais-topic.txt | Where-Object { $_ -match '^(DEMO-\d\d);.*dddddddd-0000-0000-0000-(\d{12})' } | ForEach-Object { [pscustomobject]@{ Cle = $Matches[1]; N = [int]$Matches[2] } } | Group-Object Cle | ForEach-Object { $ns = @($_.Group.N); [pscustomobject]@{ Cle = $_.Name; Messages = $ns.Count; Croissant = (($ns -join ',') -eq (($ns | Sort-Object) -join ',')); Premier = $ns[0]; Dernier = $ns[-1] } }
```
Une première tentative avec `kafka-avro-console-consumer` a échoué (`Unknown magic byte!`, cause probable non vérifiée : la clé chaîne décodée comme de l'Avro).

Nettoyage (constaté le 26/11/2026). Suppression des lignes de démonstration (attendu : `10000 rows deleted.`) :
```powershell
@("DELETE FROM outbox WHERE topic = 'cotisations.demo-relais';", "COMMIT;", "SELECT COUNT(*) AS lignes, COUNT(*) - COUNT(envoye_le) AS non_envoyees FROM outbox;") | sqlplus -s $c
```
Suppression du topic, puis contrôle (le topic a disparu de la liste) :
```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic cotisations.demo-relais
```
```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```
Suppression du sujet du Schema Registry, d'abord normale puis définitive (les deux appels ont répondu `[1]`) :
```powershell
curl.exe -s -X DELETE http://localhost:8082/subjects/cotisations.demo-relais-value
```
```powershell
curl.exe -s -X DELETE "http://localhost:8082/subjects/cotisations.demo-relais-value?permanent=true"
```
Après le nettoyage, la table `outbox` est revenue à 1 ligne, dont 0 non envoyée.

### 14.9 Limites
- **Un événement en tête qui ne se convertit pas bloque le relais** (constaté par lecture : la boucle de `RelayerOutbox.relayer()`, sans `try/catch` ; l'`IllegalArgumentException` de `ConvertisseurAvro` est levée hors du `try` de `PublicationCotisationKafka`). Il est réessayé à chaque passage et empêche les suivants de son lot ; aucune quarantaine n'existe. **Déduit, non exercé** : avec plusieurs instances, le blocage n'est pas strictement total. Une instance qui saute la ligne verrouillée par une autre peut publier des événements plus récents pendant ce court passage ; chaque instance qui réserve l'événement empoisonné échoue.
- **L'ordre repose sur l'horloge de l'application** (`cree_le` = `emiseLe`, sections 8 et 9) : deux transactions concurrentes d'un même adhérent peuvent être validées dans l'ordre inverse (déduit, non exercé). Plusieurs instances du service ne changent pas cela.
- **Parallélisme borné** par le nombre de têtes de clé et la taille du lot (section 14.2) ; **`FACTEUR_CANDIDATS = 3`** : au-delà de trois instances, une instance peut ne rien réserver (déduit).
- **Course entre les deux temps** (déduit, non testé) : un candidat lu au temps 1 peut avoir été marqué envoyé par une autre instance avant le temps 2. La condition `envoye_le IS NULL` du temps 2 l'écarte : il est alors simplement non obtenu.
- **Verrous tenus pendant la publication, pas de timeout de transaction, fenêtre de doublons élargie** : section 14.3.
- **Un passage coûte environ un aller-retour par ligne obtenue ou sautée** (constaté, section 14.6).

### 14.10 Ce qui reste non vérifié
- Arrêt brutal d'une instance : délai de libération des verrous, doublons produits.
- Lot par défaut de 50 avec deux instances.
- Plusieurs brokers.
- Chemin d'écriture de l'outbox avec plusieurs instances.
- Plan d'exécution de la forme retenue sur la vraie table `outbox`, et contention mesurée avec la requête analytique.
- `ROWNUM` et les verrous, d'après une source externe non relue.
- Relecture des valeurs Avro du topic de démonstration par un consommateur (`kafka-avro-console-consumer` a échoué, cause non vérifiée).
