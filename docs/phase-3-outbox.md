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
- **Lien avec le consommateur** : `prelevement-service` déduplique sur `idEvenement` (clé primaire). Il lit un JSON sans en-tête de type (`spring.json.use.type.headers: false`, type par défaut `CotisationEmise`, **constaté** dans son `application.yml`). La lecture du payload de l'outbox par ce consommateur n'est **pas testée** de bout en bout.

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
| Format JSON | 4 propriétés ; `montant` à 50.00 ; `emiseLe` en chaîne ISO-8601 ; identique avec un `JsonMapper` nu et avec celui de Spring | | Autres réglages des deux mappers ; lecture par `prelevement-service` |
| Transaction | Active via le cas d'usage, inactive en appel direct ; 2 lignes validées | | Rollback conjoint avec une table métier (inexistante) |
| Relais | Le relais planifié publie puis marque ; arrêt du lot au premier échec | Latence max = `delai-relais-ms` + temps du lot | Relais sous charge ; `catch` de `RelaiOutbox` |
| Au moins une fois | Doublon par échec de marquage simulé (mêmes `idEvenement` et payload) | Un vrai plantage donne le même résultat | Vrai plantage de processus |
| Multi-instance | | Deux instances publieraient les mêmes lignes | `FOR UPDATE SKIP LOCKED` |
| Purge | | La table grossit | Purge des lignes envoyées |
| Broker | Preuves sur le broker embarqué 4.2.2 | | Broker 4.4.0 du compose ; broker arrêté en réel |
| Oracle | | | Tout comportement Oracle |

## 9. Notes et limites
- **Latence** : un événement est publié au passage suivant, soit jusqu'à 10 s plus le temps de publier le lot. **Une seule instance**, pas de purge.
- **Base en mémoire par défaut** (aucune propriété `datasource` dans `application.yml`, **constaté**) : les lignes non envoyées disparaissent au redémarrage. L'outbox n'est durable qu'avec une base fichier ou Oracle (phase 6).
- **`cle` limitée à 100 caractères** : un `idAdherent` plus long fait échouer l'`INSERT` ; **déduit** : 500, `GestionErreursRest` ne traite pas les erreurs de base (non testé ici).
- **Gestionnaire 503** : plus atteint depuis le cas d'usage (il ne publie plus) ; conservé et testé avec un mock.
- **Ordre par clé** : celui de `cree_le` (horloge du service). Deux transactions concurrentes du même adhérent pourraient être validées dans l'ordre inverse (déduit, non exercé).
- **Démonstrations prévues, non exécutées** : (1) `--mutuelle.cotisation.outbox.relais-actif=false` : 201 et ligne non envoyée ; (2) broker injoignable (`--spring.kafka.bootstrap-servers=localhost:9999`) : 201, WARN du relais, puis reprise ; (3) doublon : `UPDATE outbox SET envoye_le = NULL` lu avec `org.h2.tools.Shell` sur une base fichier (`jdbc:h2:file:./cotisation-service/target/demo-outbox;AUTO_SERVER=TRUE`, `--spring.sql.init.mode=always`, `--spring.datasource.username=sa`).
