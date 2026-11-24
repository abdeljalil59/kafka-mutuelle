# Phase 3 : outbox transactionnel et relais (`cotisation-service`)

> **Mise à jour du 24/11/2026.** L'outbox de `cotisation-service` est passé de H2 à Oracle Free 23ai (`schema.sql`, variables `COTISATION_DB_URL`, `COTISATION_DB_USER`, `COTISATION_DB_PASSWORD`). **L'outbox est le seul état du service** : il n'a pas de table `cotisation`, la transaction ne porte qu'un `INSERT`. Ce journal décrit l'état du 18/11/2026 (H2) ; les sections 3, 4, 7, 8 et 9 sont mises à jour, la section 10 reste le relevé daté du 18/11 sur H2, la section 11 consigne la migration vers Oracle, la section 12 l'index de lecture du relais et la section 13 la démonstration sur l'Oracle local (24/11/2026).

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
- **Relais** : `@Scheduled(fixedDelayString = "${mutuelle.cotisation.outbox.delai-relais-ms}")`, 10 000 ms, lot de 50. Source : Javadoc de `@Scheduled` (« a fixed period between the end of the last invocation and the start of the next »), page lue en **7.1.0** alors que le projet résout la 7.0.10.
- **Lecture** : `WHERE envoye_le IS NULL ORDER BY cree_le, id_evenement FETCH FIRST ? ROWS ONLY`. Pas de `SKIP LOCKED`, pas de purge (décisions de l'étude) ; index de lecture : section 12.
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

## 7. Tests (`mvn -B test -pl cotisation-service -am`, 18/11 : `BUILD SUCCESS`, 39 tests, 11 pour `events-contract` ; 24/11/2026 : `BUILD SUCCESS`, 56 tests pour `cotisation-service`, 21 pour `events-contract`)
| Classe | Tests | Constate |
|---|---|---|
| `OutboxJdbcTest` | 8 (lancement 7) | Oracle Free dans un conteneur (`OracleIntegrationTest`), `schema.sql` de production : colonnes écrites ; format du payload (`montant` 50.00, `emiseLe` ISO) ; tri par `cree_le` puis `id_evenement` ; limite `FETCH FIRST ? ROWS ONLY` ; marquage ; `DuplicateKeyException` sur la clé primaire ; structure de l'index (colonnes et positions 1 à 3 via `USER_IND_COLUMNS`). Au 18/11 : 7 tests sur H2 mémoire |
| `RelayerOutboxTest` | 3 | Mockito sur les deux ports : publication puis marquage dans l'ordre ; arrêt du lot au premier échec ; 0 sans rien à relayer |
| `OutboxIntegrationTest` | 2 (lancement 7) | Relais **planifié** réel (200 ms), `@EmbeddedKafka`, sans espion : cas d'usage, base, relais, topic ; `idEvenement` de la ligne ; format du `JsonMapper` du contexte ; export Prometheus (`/actuator/prometheus`). Sur Oracle, un `@BeforeEach` vide `outbox` (déduit, non testé sans lui : sans lui, le vrai relais publierait les lignes laissées par une autre classe sur le même conteneur) |
| `OutboxTransactionIntegrationTest` | 2 | Sur Oracle. Relais coupé, `relayer()` appelé par le test : transaction active `[true, false]` ; doublon par échec de marquage |
| `EmettreCotisationTest`, `PublicationCotisationKafka*Test` | 2, 10 | Adaptés au commit 2 (payload tel quel, sans en-tête de type) |
- **Choix (déduit, non expérimenté)** : deux classes d'intégration, car un stub posé sur l'espion `Outbox` pendant que le relais planifié l'appelle serait une course.
- **Correction du chiffre du 18/11** : « 1 » pour `OutboxIntegrationTest` était périmé depuis le 19/11/2026, date du commit qui a ajouté le test d'export Prometheus (`d672866`, constaté par `git log -S`) ; ce n'est pas un effet de la migration.
- **Les trois `@SpringBootTest`** héritent d'`OracleIntegrationTest` : un conteneur Oracle par JVM de test, `@DynamicPropertySource` pour l'URL, l'utilisateur et le mot de passe ; un contexte Spring par classe (`@DirtiesContext`).
- **Détail au 24/11/2026 (lancement 7)** : `CotisationTest` 18, `OutboxJdbcTest` 8, `CotisationControllerTest` 4, `ConvertisseurAvroTest` 6, `PublicationCotisationKafkaTest` 6, `PublicationCotisationKafkaIntegrationTest` 3, `PublicationCotisationKafkaSansBrokerTest` 2, `RelayerOutboxTest` 3, `EmettreCotisationTest` 2, `OutboxIntegrationTest` 2, `OutboxTransactionIntegrationTest` 2, soit 56. Les autres chiffres du tableau sont ceux du 18/11.

## 8. Ce qui est constaté, déduit, non vérifié
| | Constaté | Déduit | Non vérifié |
|---|---|---|---|
| SQL / JDBC | `FETCH FIRST ? ROWS ONLY` avec paramètre (limites 1, 2, 10) ; tri ; `Instant` aller-retour exact (H2, 18/11) ; mêmes requêtes sous Oracle Free 23.26.4 avec `OffsetDateTime` UTC (`OutboxJdbcTest`, 24/11/2026) | | |
| Clé primaire | `DuplicateKeyException`, ligne d'origine intacte (H2 le 18/11, Oracle le 24/11/2026) | | |
| Format JSON | 4 propriétés ; `montant` à 50.00 ; `emiseLe` en chaîne ISO-8601 ; identique avec un `JsonMapper` nu et avec celui de Spring | | Autres réglages des deux mappers ; lecture par `prelevement-service` dans un test automatisé (relevée en démonstration 3, section 10) |
| Transaction | Active via le cas d'usage, inactive en appel direct ; 2 lignes validées | | Rollback conjoint avec une table métier (inexistante) |
| Relais | Le relais planifié publie puis marque ; arrêt du lot au premier échec | Latence max = `delai-relais-ms` + temps du lot | Relais sous charge ; `catch` de `RelaiOutbox` |
| Au moins une fois | Doublon par échec de marquage simulé (mêmes `idEvenement` et payload) ; relevé en démonstration 3 : republication provoquée par un `UPDATE`, doublon ignoré par `prelevement-service` (relevé du 18/11 sur le code d'alors : le journal « doublon ignoré » n'existe plus, section 13) ; rejoué sur Oracle le 24/11/2026 : une seule ligne en base, un second résultat publié, compteur à 1 (section 13) | Un vrai plantage donne le même résultat | Vrai plantage de processus |
| Multi-instance | | Deux instances publieraient les mêmes lignes | `FOR UPDATE SKIP LOCKED` |
| Purge | | La table grossit | Purge des lignes envoyées |
| Broker | Preuves automatisées sur le broker embarqué 4.2.2 ; publication vers le broker 4.4.0 du compose relevée en démonstrations 2 et 3 (section 10, 18/11) ; broker réellement arrêté puis rétabli (`docker stop kafka`, `docker start kafka`), relais rattrapant (section 13, 24/11/2026) | | |
| Oracle | Conteneur 23.26.4 : `OutboxJdbcTest`, flux complet, transaction, doublon par échec de marquage (lancement 7, 56 tests verts) ; Oracle local 23.4.0.24.05 : démonstration en direct, 3 scénarios, types des colonnes et colonnes de l'index relevés (24/11/2026, section 13) | | Oracle local : aucun test automatisé ; `ORA-02014` non vérifié sur 23.26.4 (section 11) |

## 9. Notes et limites
- **Latence** : un événement est publié au passage suivant, soit jusqu'à 10 s plus le temps de publier le lot. **Une seule instance**, pas de purge.
- **Base Oracle** (`spring.datasource.*` de `application.yml`, variables `COTISATION_DB_*`) : la table survit au redémarrage. Au 18/11/2026, la base était H2 en mémoire par défaut (aucune propriété `datasource` dans `application.yml`, constaté alors) et les lignes non envoyées disparaissaient au redémarrage.
- **`cle` limitée à 100 caractères** : un `idAdherent` plus long fait échouer l'`INSERT` ; **déduit** : 500, `GestionErreursRest` ne traite pas les erreurs de base (non testé ici).
- **Gestionnaire 503** : plus atteint depuis le cas d'usage (il ne publie plus) ; conservé et testé avec un mock.
- **Ordre par clé** : celui de `cree_le` (horloge du service). Deux transactions concurrentes du même adhérent pourraient être validées dans l'ordre inverse (déduit, non exercé).
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

### `FOR UPDATE SKIP LOCKED` et `ORA-02014` (relevé de l'utilisateur ; non vérifié sur 23.26.4)
- **Relevé par l'utilisateur sur Oracle local 23.4.0.24.05** : `… FETCH FIRST 2 ROWS ONLY FOR UPDATE SKIP LOCKED` donne `ORA-02014`. **Non refait** sur le conteneur de test 23.26.4 : **non vérifié** sur cette version.
- Repli accepté sur 23.4 (même relevé) : `WHERE id_evenement IN (SELECT id_evenement FROM (SELECT … ORDER BY cree_le, id_evenement) WHERE ROWNUM <= 2) FOR UPDATE SKIP LOCKED`.
- **Déduit, non testé à deux sessions** : la sous-requête ne verrouille rien, donc une instance peut recevoir moins de lignes que demandé.
- **Hors périmètre** : `SKIP LOCKED` fera l'objet d'une tâche séparée, avec le test qui documente la perte d'ordre par clé. La lecture actuelle (mono-instance) conserve l'ordre par clé ; tout reste « au moins une fois ».

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
- **(f) Un doublon produit un second résultat** : `TraiterDemandePrelevement.java:35-36` appelle `enregistrer` puis `publier` sans condition (commentaires `:27-30`). Le résultat republié a le même `idEvenement` (`PublicationResultats.java:10` : republier la même décision « donne le même `idEvenement`, que le consommateur aval utilise pour dédoublonner »). Les deux messages de `prelevements.resultats` le confirment. **Constaté (code lu)** : `stats-service` ne compte un `idEvenement` qu'une fois ([kafka-streams.md](kafka-streams.md), lignes 36 et 73) ; `impaye-service` ignore un `idEvenement` déjà enregistré (`PersistanceImpayeJdbc.java:32-49` : `INSERT` et capture de `DuplicateKeyException`, compteur `impaye.doublons.ignores` ; contrainte `uq_impaye_id_evenement`, `schema.sql:15`). **Non exercé en direct** : ces deux services n'ont pas tourné pendant cette démonstration ; `impaye-service` n'enregistre que les rejets (README, tableau des modules) et la décision de la démonstration était ACCEPTE.

### Ce que cette démonstration ne prouve pas
- **Un vrai plantage** entre l'accusé du broker et le marquage : le replay est provoqué par un `UPDATE` manuel.
- **Le plan d'exécution** de la requête du relais sur la vraie table `outbox` : non relevé (section 12).
- **Plusieurs instances du relais** et `SKIP LOCKED` (section 11).
