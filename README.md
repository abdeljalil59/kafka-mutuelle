# kafka-mutuelle

Projet d'apprentissage de **Kafka avec Java / Spring**, appliqué à un domaine de mutuelle
(cotisations, prélèvements, impayés). Objectif : comprendre et **observer en live** les
messages, partitions, offsets, le lag et le rebalancing, avec une preuve (test ou capture)
pour chaque expérience.

## Flux

```
cotisation-service ──► cotisations.emises ──► prelevement-service ──► prelevements.resultats ──► impaye-service ──► impayes.relances
                                                                                        ├──► stats-service (agrégation par adhérent, API REST)
                                                                                        ├──► Kafka Connect ──► PostgreSQL (JDBC Sink)
                                                                                        └──► kafka-lab (dashboard live, pas encore réalisé)
```

Réalisé : les quatre services (`cotisation-service`, `prelevement-service`, `impaye-service`, `stats-service`) et le connecteur Kafka Connect. Pas encore réalisé : `kafka-lab` (dashboard live).

Les modules, leurs ports, leurs topics Kafka et leurs bases sont décrits dans « Architecture / Modules » ci-dessous.

| Topic | Clé | Partitions | Valeur |
|---|---|---|---|
| `cotisations.emises` | `idAdherent` | 3 | **Avro** (sujet `cotisations.emises-value` du Schema Registry) |
| `prelevements.resultats` | `idAdherent` | 3 | **Avro** `ResultatPrelevement` : un seul topic, champ `type` ACCEPTE / REJETE. Créé et publié par `prelevement-service`, dans une transaction |
| `impayes.relances` | `idAdherent` | 3 | **JSON** en chaîne (`idAdherent`, `nombreRejets`, `dernierMontant`, `relanceLe`), pas de schéma Avro. Créé et publié par `impaye-service` |
| `cotisations.emises-dlt` (messages de `cotisations.emises` rejetés : illisibles, ou en échec dans le listener) | `idAdherent` (clé d'origine conservée) | 3 | octets d'origine (illisible) ou Avro (échec du listener) ; rétention 30 jours à la création du topic |
| `cotisations.retry-demo`, `…-retry`, `…-dlt` (démo `@RetryableTopic`, inactive par défaut) | libre | 3 | JSON brut |

## Architecture / Modules

| Module | Port | Rôle | Kafka | Base |
|---|---|---|---|---|
| `events-contract` | — | schémas Avro partagés (`CotisationEmise`, `ResultatPrelevement`) : contrat d'interface entre les services, sans dépendance Spring | — | — |
| `cotisation-service` | 8081 | reçoit les cotisations par REST et les publie par un relais d'outbox, qui supporte plusieurs instances (démontré à deux, avec arrêt brutal d'une instance) | producteur de `cotisations.emises` (Avro) | Oracle Free 23ai (table `outbox`) |
| `prelevement-service` | 8083 | décide ACCEPTE ou REJETE (plafond), enregistre la décision, publie le résultat | consomme `cotisations.emises`, publie `prelevements.resultats` (Avro) **dans une transaction Kafka**, DLT (Dead Letter Topic : topic où atterrissent les messages en échec) sur `cotisations.emises-dlt` | Oracle Free 23ai |
| `impaye-service` | 8084 | enregistre les prélèvements rejetés, relance un adhérent dès que ses rejets atteignent le seuil ; pas de DLT | consomme `prelevements.resultats` en `read_committed` (**sans transaction Kafka**), publie `impayes.relances` (JSON) | H2 en mémoire |
| `stats-service` | 8086 | calcule en temps réel, par adhérent, le nombre de prélèvements acceptés et rejetés et le montant total (Kafka Streams) ; expose le résultat par une API REST | consomme `prelevements.resultats` (Avro) ; ne publie aucun topic métier (seulement les topics internes de Kafka Streams) | état local (RocksDB, une base clé-valeur embarquée), sauvegardé dans un topic Kafka |

Infra Docker (`infra/docker-compose.yml`) : Kafka KRaft (9092), Schema Registry (8082), Kafka UI (8080), PostgreSQL (5433), Kafka Connect (8085), Prometheus (9090), Grafana (3000). Les services Java tournent sur l'hôte.

## Prérequis

- Docker Desktop (démarré avant toute commande `docker`), y compris pour `mvn verify` complet : les tests de `prelevement-service` et de `cotisation-service` démarrent Oracle dans un conteneur (premier téléchargement : environ 1,3 Go, 5,1 Go sur disque ; démarrage de 4 à plus de 10 minutes selon la mémoire de la machine, au-delà de laquelle le build échoue)
- Java 17
- Maven
- **Oracle Free 23ai** installé en local, pour **exécuter** `prelevement-service` et `cotisation-service` (par défaut `localhost:1522/freepdb1`, utilisateur `kafka_mutuelle`). URL, utilisateur et mot de passe se surchargent par les variables d'environnement `PRELEVEMENT_DB_URL`, `PRELEVEMENT_DB_USER` et `PRELEVEMENT_DB_PASSWORD` pour le premier, `COTISATION_DB_URL`, `COTISATION_DB_USER` et `COTISATION_DB_PASSWORD` pour le second (mêmes valeurs par défaut). Ces services ne démarrent pas sans base ; `impaye-service` reste autonome (H2). `cotisation-service` crée sa table `outbox` au démarrage si elle n'existe pas ; une table existante n'est pas modifiée.

## Démarrage rapide

Toutes les commandes sont à lancer depuis la racine du dépôt, dans PowerShell.

**1. Infrastructure.**

```powershell
docker compose -f infra/docker-compose.yml up -d
```

Au premier démarrage, Kafka Connect télécharge son plugin JDBC : accès Internet requis. **Ne jamais utiliser `down -v`** : il supprime le volume `kafka-mutuelle-data` et toutes les données.

**2. Module partagé.** `events-contract` doit être installé localement avant de lancer un service :

```powershell
mvn -B -pl events-contract install -DskipTests
```

**3. Services.** Un terminal par service : ces commandes ne se terminent pas.

```powershell
mvn -pl cotisation-service spring-boot:run
```

```powershell
mvn -pl prelevement-service spring-boot:run
```

```powershell
mvn -pl impaye-service spring-boot:run
```

```powershell
mvn -pl stats-service spring-boot:run
```

`prelevement-service` et `cotisation-service` ne démarrent pas sans Oracle (voir « Prérequis »).

**4. Envoyer une cotisation.** Sous Windows PowerShell, les guillemets du JSON passés directement à `curl` sont altérés : le corps est donc écrit d'abord dans un fichier (constaté). Le fichier `cotisation-demo.json` est créé par le lecteur ; il n'est pas versionné.

```powershell
Set-Content -Path cotisation-demo.json -Value '{"idAdherent":"ADH-DEMO-01","montant":42.50}'
curl.exe -X POST http://localhost:8081/cotisations -H "Content-Type: application/json" -d "@cotisation-demo.json"
```

La réponse est un `201`. Le montant 42.50 est inférieur au plafond de 100.00 : le prélèvement est accepté. Sous Linux ou macOS :

```bash
curl -X POST http://localhost:8081/cotisations -H "Content-Type: application/json" -d '{"idAdherent":"ADH-DEMO-01","montant":42.50}'
```

**5. Lire les statistiques** (quelques secondes plus tard) :

```powershell
curl.exe http://localhost:8086/stats
curl.exe http://localhost:8086/stats/ADH-DEMO-01
```

(`curl.exe` et non `curl` : dans Windows PowerShell, `curl` est un alias de `Invoke-WebRequest`.)

## Stack

- Java 17, Maven multi-modules : `events-contract` (records et schémas Avro `CotisationEmise` et `ResultatPrelevement`), `cotisation-service`, `prelevement-service`, `impaye-service`, `stats-service`
- Spring Boot 4.1.2, Spring for Apache Kafka 4.1.2, kafka-clients 4.2.2
- Kafka 4.4.0 (`apache/kafka:4.4.0`, KRaft, un nœud) et Kafka UI v1.5.0, par Docker Compose (`infra/docker-compose.yml`)
- **Avro 1.11.5 et Confluent Schema Registry 7.9.11** (`io.confluent:kafka-avro-serializer` 7.9.11) : contrats typés de `cotisations.emises` et `prelevements.resultats`
- **Kafka Streams** pour `stats-service` (version de `kafka-streams` gérée par Spring Boot, `io.confluent:kafka-streams-avro-serde` 7.9.11) ; **Kafka Connect** (`cp-kafka-connect:7.9.11`, plugin JDBC Sink 10.8.8) et **PostgreSQL 17** pour copier `prelevements.resultats` dans une table SQL, sans code Java
- **Oracle Free 23ai** pour `prelevement-service` (table des décisions, idempotence) et `cotisation-service` (table `outbox`, index de lecture du relais, réservation des événements par `FOR UPDATE SKIP LOCKED`) ; `ojdbc17` 23.26.3.0.0 ; tests sur Oracle dans un conteneur (Testcontainers 2.0.5, image `gvenzl/oracle-free:23.26.4-slim-faststart`)
- **H2** pour `impaye-service` (table `impaye`, en mémoire : les impayés sont perdus à l'arrêt)
- **Actuator + Micrometer, Prometheus v3.16.0 et Grafana 13.3.1** : métriques des trois services (`/actuator/prometheus`), dashboard « Kafka Mutuelle » provisionné (5 lignes, 14 panneaux ; fichiers dans `prometheus/` et `grafana/`). Aucun panneau n'est encore dédié à `impaye-service` (sa cible Prometheus existe) ; deux panneaux sans filtre par service, la latence du listener et les connexions HikariCP, en afficheront les séries dès que la cible sera scrapée

| Composant | Port de l'hôte |
|---|---|
| Kafka (broker) | 9092 |
| Kafka UI | 8080 |
| **Schema Registry** | **8082** (8081 dans le conteneur, car 8081 est pris par `cotisation-service`) |
| `cotisation-service` (API REST et Actuator) | 8081 |
| `prelevement-service` (Actuator, `/actuator/prometheus`) | 8083 (surchargeable par `PRELEVEMENT_SERVER_PORT`) |
| **`impaye-service`** (Actuator, `/actuator/prometheus`) | **8084** (surchargeable par `IMPAYE_SERVER_PORT`) |
| **Prometheus** | **9090** |
| **Grafana** | **3000** (dashboard « Kafka Mutuelle » ; `admin`/`admin`, lecture anonyme : usage local uniquement) |
| Oracle Free 23ai local (utilisé par `prelevement-service` et `cotisation-service`) | 1522 (service `freepdb1`, utilisateur `kafka_mutuelle`) |
| **`stats-service`** (REST `/stats`, Kafka Streams) | **8086** (surchargeable par `STATS_SERVER_PORT`) |
| **PostgreSQL 17** (cible du JDBC Sink, base `kafka_connect_sink`) | **5433** (5432 dans le conteneur) |
| **Kafka Connect** (API REST) | **8085** (8083 dans le conteneur, car 8083 est pris par `prelevement-service`) |

L'infrastructure se démarre comme indiqué dans « Démarrage rapide » : la commande démarre aussi Prometheus, Grafana, PostgreSQL et Kafka Connect. Les quatre services Java tournent sur l'hôte (`java -jar` ou IDE), pas dans le compose, et il n'existe aucun Dockerfile. Les cibles Prometheus des trois premiers services sont `down` tant qu'ils sont arrêtés (`stats-service` n'expose pas de métriques). Prometheus ne relit pas `prometheus/prometheus.yml` tout seul : après l'ajout d'une cible, redémarrer son conteneur.

## `cotisation-service` : relais d'outbox à plusieurs instances

- **Principe** : chaque cotisation est écrite dans la table `outbox` ; un relais planifié la publie sur `cotisations.emises`, puis la marque envoyée. La livraison est « au moins une fois » : un doublon porte le même `idEvenement` et doit être absorbé par un consommateur idempotent.
- **Plusieurs instances** : le relais réserve ses événements en deux temps (les têtes de clé lues sans verrou, puis `SELECT … FOR UPDATE SKIP LOCKED` par ligne), dans une transaction par passage. Il réserve au plus un événement par clé et par passage : une autre instance ne peut pas publier un événement plus récent de la même clé tant que celui-ci n'est pas marqué envoyé. Un déclenchement enchaîne des passages tant que le précédent a réservé des événements, au plus `mutuelle.cotisation.outbox.passages-max` (100 par défaut).
- **Constaté** : une démonstration à deux instances (ports 8081 et 8091, même base Oracle Free 23.4.0.24.05) a publié 10 000 événements sur 20 clés : aucun doublon (somme des offsets de fin du topic égale à 10 000), ordre conservé pour chaque clé, aucune erreur Oracle. Un arrêt brutal de l'une des deux instances (processus tué en plein passage, une fois) a laissé 5 doublons, soit la taille du lot : l'autre instance avait réservé ses clés au plus 69 ms après l'arrêt et les a republiés en 111 ms au plus ; l'ordre par clé est resté croissant. Limites de ces essais : taille de lot de 5 (et non 50), backlog inséré en SQL (le chemin d'écriture de l'outbox n'est pas exercé), processus tué sur la machine de la base (meilleur cas), un seul essai concluant. Détail, mesures et commandes : [phase-3-outbox.md](docs/phase-3-outbox.md), section 14.
- **Limites** : *héritées* (déjà présentes avec une seule instance) : un événement en tête qui ne se convertit pas en Avro bloque le relais (pas de quarantaine), et l'ordre par clé repose sur l'horloge de l'application ; *conséquences de la conception à plusieurs instances* : les verrous sont tenus pendant la publication, il n'y a pas de délai maximal de transaction, et un plantage avant la validation du passage republie tout le passage (constaté une fois). Démarrer une instance pendant qu'une autre relaie peut échouer au démarrage (`ORA-00054`, constaté une fois, cause déduite).
- **Non vérifié** : le lot par défaut (50) avec deux instances ; la libération des verrous quand la connexion reste ouverte (panne réseau) et les arrêts à d'autres moments de la passe ; l'absorption des doublons par des consommateurs idempotents après un arrêt brutal (aucun consommateur n'était configuré sur le topic de démonstration) ; la fréquence de l'échec au démarrage ; plusieurs brokers.

## `prelevement-service` : consommation, transactions et DLT

- **Listener en mode record** : `@KafkaListener` reçoit un `ConsumerRecord` à la fois. Le mode batch (`List<ConsumerRecord>`, `BatchListenerFailedException`, repli un par un) a été abandonné : sous transactions, un listener batch avec `DefaultAfterRollbackProcessor` redélivre le lot entier à l'infini sans jamais appeler le recoverer (relevé lors des essais), alors qu'un listener record isole le message fautif. Voir [transactions-exactement-une-fois.md](docs/transactions-exactement-une-fois.md), §8.
- **Écriture idempotente par `MERGE` Oracle** (`WHEN NOT MATCHED THEN INSERT` seul) : un doublon est ignoré, sans exception. `JdbcTemplate.batchUpdate` est conservé (`JdbcClient` n'a pas de batch), avec une décision par appel.
- **Résultat publié** : après le `MERGE`, la décision (ACCEPTE ou REJETE) est publiée sur `prelevements.resultats` avec `idEvenement` pour identifiant. Un message rejoué republie la même décision.
- **Transactions Kafka** : `spring.kafka.producer.transaction-id-prefix` (`tx-prelevement-` par défaut, **unique par instance**, variable `KAFKA_TRANSACTION_ID_PREFIX`) et consommateur en `read_committed`. Lecture de `cotisations.emises`, résultat publié et offset sont validés ou annulés ensemble. **Oracle ne participe pas à la transaction Kafka** : c'est l'idempotence du `MERGE` qui absorbe un rejeu.
- **Erreurs** : `DefaultAfterRollbackProcessor` à la place du `DefaultErrorHandler` (qui ne gère pas un container transactionnel) : 5 livraisons (1 + 4 réessais à 1, 2, 4 puis 8 s), puis `cotisations.emises-dlt` sur la même partition, avec validation de l'offset (`commitRecovered`). Une `PrelevementInvalideException` et un message illisible ne sont pas réessayés (1 livraison).
- **Preuves exactly-once** (`ExactlyOnceIntegrationTest`, 5 tests sur le vrai code, broker embarqué) : commit (résultat et offset visibles ensemble), abort après envoi (résultat invisible en `read_committed`, offset non validé, ligne Oracle conservée), isolation (`read_uncommitted` voit le résultat annulé, `read_committed` non), redélivrance (un seul résultat visible, une seule ligne), DLT après 5 livraisons sans résultat visible. L'absence se prouve par le LSO (*last stable offset* : position jusqu'à laquelle les messages transactionnels sont décidés, ce qui veut dire que la transaction est terminée), pas par un délai. Non prouvé : les transactions contre le broker réel 4.4.0 (les tests utilisent le broker embarqué 4.2.2), un crash en plein traitement, et l'atomicité entre l'envoi au DLT et la validation de l'offset.
- **Métriques** : `/actuator/prometheus` (port 8083) : séries clientes Kafka (lag par partition, débit), timer du listener, pool HikariCP, timers du `KafkaTemplate` (`kafkaTemplate` pour le DLT, `resultatsKafkaTemplate` pour les résultats), deux compteurs custom `prelevement_decisions_total{decision}` et `prelevement_doublons_ignores_total`, et `kafka_producer_txn_*_time_ns_total`, qui sont des **cumuls de temps** et non des compteurs de transactions. Le lag vient du client Kafka : il disparaît quand le consommateur est arrêté (pas d'exporter côté broker).

Détails et mesures : [transactions-exactement-une-fois.md](docs/transactions-exactement-une-fois.md), [amelioration-dlt.md](docs/amelioration-dlt.md), [phase-6-metriques.md](docs/phase-6-metriques.md).

## `impaye-service`

- **Consommation** : `prelevements.resultats` en `read_committed` (le producteur amont est transactionnel : un résultat d'une transaction annulée n'est jamais livré), groupe `impaye-service`, mode record. Les résultats ACCEPTE sont ignorés ; un REJETE devient un impayé. **Pas de transaction Kafka** côté service.
- **Persistance** : H2 en mémoire, table `impaye` (clé d'idempotence : `id_evenement`, contrainte d'unicité). Un résultat rejoué est ignoré sans exception et gonfle le compteur `impaye_doublons_ignores_total`.
- **Relance** : quand le nombre d'impayés d'un adhérent atteint `app.relance.seuil` (3 par défaut) et qu'aucune relance n'a encore été émise pour lui, une relance JSON est publiée sur `impayes.relances` (clé `idAdherent`), **une seule par adhérent**. Le nombre de rejets est lu en base (`COUNT` par adhérent), pas tenu dans un compteur. Un drapeau `relance_emise` en base dit si la relance est partie : le cas d'usage **publie d'abord, marque ensuite**. La décision se prend à chaque message, nouveau ou rejoué : un doublon n'ajoute rien (pas de ligne, pas de décompte) et ne relance pas si elle est déjà partie.
- **Erreurs** : 3 réessais espacés de 1 s, puis le message est journalisé en ERROR et abandonné : **pas de DLT** dans cette première version.
- **Reprise d'une publication ratée** : si la publication échoue, rien n'est marqué ; l'exception remonte, le message est réessayé et, s'il est abandonné, le rejet suivant de l'adhérent émet la relance (décompte et montant de ce rejet-là). Une relance n'est donc **pas perdue**. **Limite assumée** : si le marquage échoue après une publication réussie, le réessai republie : une relance **en double** est possible, donc le consommateur de `impayes.relances` doit être idempotent (sur `idAdherent`). Le service est mono-instance : sa base H2 est propre au processus.
- **Topic d'entrée non créé par ce service** (`allow.auto.create.topics=false`) : il appartient à `prelevement-service`. Démarré avant lui, `impaye-service` attend le topic au lieu de le faire créer à une seule partition.
- **Tests** : sans Oracle ni Docker (broker embarqué, registre factice `mock://`, H2). Le `read_committed` est prouvé avec un producteur transactionnel de test, et un témoin négatif (consommateur forcé en `read_uncommitted`) fait échouer le test d'abort.

## Kafka Connect : JDBC Sink sur `prelevements.resultats`

Un connecteur **JDBC Sink** (`connect/jdbc-sink-resultats.json`) copie `prelevements.resultats` (Avro) dans la table PostgreSQL `resultats_prelevements`, sans code Java : `upsert` sur la clé primaire `idEvenement` (idempotence attendue, rejeu non encore prouvé). La table est créée **à la main** par `connect/init-postgres.sql` (`auto.create=false`, voir les pièges ci-dessous), et `decideLe` y est un `BIGINT` (microsecondes). Services ajoutés à `infra/docker-compose.yml` : `postgres` (port 5433) et `kafka-connect` (`cp-kafka-connect:7.9.11`, API REST sur 8085). Cette étape n'ajoute aucun code Java, donc aucun test. Constaté à la main : le connecteur tourne et 4 lignes ont été écrites. Pièges rencontrés (point dans le nom du topic, `timestamp-micros` en bigint, erreurs SQL masquées), commandes et limites : [kafka-connect.md](docs/kafka-connect.md).

## `stats-service` : Kafka Streams

- **Topology** : `prelevements.resultats` lu en KStream (Avro), re-keyé par `idAdherent` (`selectKey`, `repartition`), dédupliqué par `idEvenement` (`WindowStore`, rétention 7 jours configurable par `stats.deduplication.retention`), puis agrégé (`groupByKey` puis `aggregate`) dans une KTable matérialisée par le State Store `stats-store` (RocksDB, sauvegardé dans un topic de changelog).
- **Agrégat `StatsAdherent`** (record immuable, JSON par un Serde Jackson écrit à la main) : `nombreAcceptes`, `nombreRejetes`, `montantTotal` (double, tous résultats), `dernierEvenement` (millisecondes).
- **Interactive Queries** : `GET /stats/{idAdherent}` (200 ou 404) et `GET /stats` (tous), 503 tant que le store n'est pas prêt. **Mono-instance** : avec plusieurs instances, chacune ne verrait que ses partitions.
- **Consommation** en `read_committed` (producteur amont transactionnel). Garantie **au moins une fois** (`at_least_once`, constaté au démarrage). Un résultat rejoué n'est compté qu'une fois **dans la limite de la rétention de déduplication** (7 jours par défaut, comme la rétention du topic).
- **Prouvé par des tests** : 30 tests sans broker (`TopologyTestDriver`, MockMvc), dont la déduplication (mêmes résultats rejoués, contenu identique, expiration après la rétention).
- **Perte du dossier d'état local** (constaté le 24/11/2026) : dossier d'état supprimé, topics de changelog conservés ; au redémarrage, `stats-store` et le store de déduplication sont restaurés depuis leurs changelogs (3,0 s), `GET /stats` renvoie les mêmes comptes et montants. Un redémarrage rapproché d'un arrêt sans départ du groupe peut attendre jusqu'à environ 45 s (27,5 s constatés). Détail : [kafka-streams.md](docs/kafka-streams.md), §8 ter.
- **Vérifié à la main** (service démarré contre le broker 4.4.0 et le registre, relevés manuels, sans test automatisé) : démarrage réel, agrégation des messages déjà présents sur le topic, mise à jour en temps réel après une nouvelle cotisation, restauration du state store après redémarrage (chiffres identiques avant et après). Détail : [kafka-streams.md](docs/kafka-streams.md), §8 bis.
- **Constaté au rejeu** : avec la déduplication, remettre les offsets du groupe à zéro puis redémarrer laisse les chiffres inchangés (3 acceptés/1 rejeté/335.0 et 0/1/200.0). Avant la déduplication, le même geste donnait 6/2/670.0.
- **Non vérifié** : le cas avec dossier d'état local conservé (réutilisation des checkpoints), le cas hors rétention contre le broker réel (prouvé seulement par un test), le rôle exact du cache, et si `exactly_once_v2` changerait le résultat.

## Tests

**264 tests, somme dérivée** : `events-contract` 21, `cotisation-service` 71, `prelevement-service` 109, `impaye-service` 33, `stats-service` 30. Chaque nombre est mesuré à sa date, pas d'un bloc : `cotisation-service` 71 au `mvn -B -pl cotisation-service test` du 26/11/2026 (avec conteneur Oracle) ; `events-contract` 21, `impaye-service` 33 et `stats-service` 30 au `mvn -B verify -pl events-contract,impaye-service,stats-service` du 24/11/2026 (aucun conteneur démarré, 1 min 13 s) ; les 109 de `prelevement-service` viennent du dernier build complet, dont la date n'est pas consignée. **Constaté** par l'historique git : aucune modification de `prelevement-service` ni de `events-contract` après le 20/11/2026 (dernier commit sur `prelevement-service` : 20/11/2026 à 16:23). **Déduit** : le nombre 109 reste valable si ce build est postérieur à ce commit. Le total n'a pas été mesuré en un seul build.

- Build complet : `mvn -B verify` (Docker et mémoire suffisante pour Oracle : `prelevement-service` et `cotisation-service` démarrent Oracle dans un conteneur).
- Sans Oracle : `mvn -B verify -pl events-contract,impaye-service,stats-service` (84 tests, relevé le 24/11/2026).

## Avancement

| Phase | Contenu | Statut |
|---|---|---|
| 0 | Kafka en CLI (Docker) + Kafka UI | Terminée ([phase-0-cli.md](docs/phase-0-cli.md)) |
| 1 | Producer et consumer Spring, tests unitaires | Réalisée : [producteur](docs/phase-1-producteur.md) (tests unitaires, `@EmbeddedKafka`, requêtes et panne du broker constatées sur le broker 4.4.0) et [consommateur](docs/phase-1-consommateur.md) (règle testée, offsets commités, reprise et départ propre constatés). Non exercés : délai dépassé après envoi et futur en échec (producteur), `CooperativeStickyAssignor`, `group.protocol=consumer` ; les erreurs du listener relèvent de la phase 3 |
| 2 | Clés, rebalancing, plusieurs instances | Réalisée pour **deux instances** ([phase-2-rebalancing.md](docs/phase-2-rebalancing.md)) : routage par clé, ordre dans une partition, arrivée d'un second membre, départ propre (environ 1,5 s) et arrêt brutal (34 à 41 s) constatés. Non exercés : plus de deux membres ou plus de membres que de partitions, `CooperativeStickyAssignor`, `group.protocol=consumer`, `session.timeout.ms` modifié |
| 3 | Idempotence, retries, DLT | Réalisée (idempotence, `@RetryableTopic`, DLT avec BackOff exponentiel : journaux de la phase 3 et [amelioration-dlt.md](docs/amelioration-dlt.md)) |
| 4 | Prometheus puis Grafana | Réalisée sans exporter de lag broker (Actuator, Micrometer, Prometheus et Grafana ; lag par les métriques clientes Kafka) : [phase-6-metriques.md](docs/phase-6-metriques.md) (le « 6 » du nom de fichier est une étiquette de travail, voir « Études réalisées ») |
| 5 | Dashboard live `kafka-lab` + scénarios | À faire |
| 6 | Outbox Oracle, Avro / Schema Registry, exactly-once, Kafka Streams | En cours : Avro et Schema Registry, Oracle (`MERGE`), exactly-once (transactions et preuves sur `prelevement-service`), `impaye-service` (rejets, relance) et Kafka Streams (`stats-service`, [kafka-streams.md](docs/kafka-streams.md)) réalisés ; outbox de `cotisation-service` migré sur Oracle, relais démontré à deux instances ([phase-3-outbox.md](docs/phase-3-outbox.md), sections 11, 12 et 14) |
| 7 | Tests d'intégration complets, éventuellement ELK | En cours (`@EmbeddedKafka`, registre factice et Oracle Testcontainers sur les trois premiers services ; `stats-service` : `TopologyTestDriver` seulement, pas de test avec broker) ; ELK à faire |
| Hors plan initial | Kafka Connect : JDBC Sink de `prelevements.resultats` vers PostgreSQL, par configuration seule | Réalisée, vérifiée à la main (connecteur actif, 4 lignes écrites) ; rejeu (idempotence) non prouvé : [kafka-connect.md](docs/kafka-connect.md) |

## Études réalisées

Onze études, chacune avec son journal ou son guide (constaté, déduit, non vérifié) et, selon l'étude, ses tests. Les numéros de « phase » dans les noms de fichiers (phase 3, phase 5, phase 6…) sont des étiquettes de travail, indépendantes du tableau d'avancement ci-dessus. Les journaux sont des relevés **datés** : un encadré « Mise à jour » en tête signale ce qui a changé depuis.

| Étude | Contenu | Journal |
|---|---|---|
| Consommateur idempotent | Décision de prélèvement écrite une seule fois par `idEvenement` (clé primaire, doublon absorbé) | [phase-3-idempotence.md](docs/phase-3-idempotence.md) |
| Outbox | Cotisation enregistrée en base (H2 le 18/11, Oracle depuis le 24/11/2026), publiée par un relais planifié, au moins une fois ; relais à plusieurs instances (`FOR UPDATE SKIP LOCKED`), mesures et démonstration à deux instances (26/11/2026) | [phase-3-outbox.md](docs/phase-3-outbox.md) |
| Retry topic | Retries non bloquants avec `@RetryableTopic`, cohabitation avec le DLT, perte de l'ordre par clé | [phase-3-retry-topic.md](docs/phase-3-retry-topic.md) |
| Avro et Schema Registry | Schéma `CotisationEmise` enregistré dans le registre (port 8082), sérialisation Avro de bout en bout | [phase-4-avro-schema-registry.md](docs/phase-4-avro-schema-registry.md) |
| Oracle (`prelevement-service`) | H2 remplacé par Oracle Free 23ai ; tests sur Oracle dans un conteneur (Testcontainers) ; découvertes sur `Instant` et ORA-12899 | [phase-5-etape-1-oracle.md](docs/phase-5-etape-1-oracle.md) |
| `MERGE` et écriture par lot (`prelevement-service`) | Idempotence par `MERGE … WHEN NOT MATCHED` sans exception ; listener batch avec `BatchListenerFailedException`, **remplacé depuis par un listener record** | [phase-5-etape-2-merge-batch.md](docs/phase-5-etape-2-merge-batch.md) |
| Métriques (Actuator, Micrometer, Prometheus, Grafana) | Séries clientes Kafka, timer du listener et pool HikariCP ; deux compteurs custom (décisions, doublons) ; Prometheus et dashboard Grafana provisionnés ; relevé du smoke test | [phase-6-metriques.md](docs/phase-6-metriques.md) |
| Amélioration du DLT | BackOff exponentiel (5 livraisons, 15 s), rétention 30 jours, tests de livraisons, panneaux Grafana du DLT | [amelioration-dlt.md](docs/amelioration-dlt.md) |
| Transactions et exactly-once | Guide pédagogique et application au projet : transactions sur `prelevement-service`, `read_committed`, preuves, limites ; `impaye-service` | [transactions-exactement-une-fois.md](docs/transactions-exactement-une-fois.md) |
| Kafka Connect (JDBC Sink) | Copie de `prelevements.resultats` vers PostgreSQL par configuration seule ; pièges : point dans le nom du topic, `timestamp-micros`, erreurs SQL masquées | [kafka-connect.md](docs/kafka-connect.md) |
| Kafka Streams (`stats-service`) | Agrégation par adhérent (KTable, State Store), consultation REST du store, relevés en direct | [kafka-streams.md](docs/kafka-streams.md) |

Journal d'apprentissage : [`docs/`](docs/) : [phase 0, Kafka en CLI](docs/phase-0-cli.md), [phase 1, producteur](docs/phase-1-producteur.md), [phase 1, consommateur](docs/phase-1-consommateur.md), [phase 2, rebalancing](docs/phase-2-rebalancing.md), [phase 3, message invalide](docs/phase-3-message-invalide.md), [phase 3, idempotence](docs/phase-3-idempotence.md), [phase 3, outbox](docs/phase-3-outbox.md), [phase 3, retry topic](docs/phase-3-retry-topic.md), [phase 4, Avro et Schema Registry](docs/phase-4-avro-schema-registry.md), [phase 5 étape 1, Oracle](docs/phase-5-etape-1-oracle.md), [phase 5 étape 2, MERGE et batch](docs/phase-5-etape-2-merge-batch.md), [phase 6, métriques](docs/phase-6-metriques.md), [amélioration du DLT](docs/amelioration-dlt.md), [transactions et exactly-once](docs/transactions-exactement-une-fois.md), [Kafka Connect](docs/kafka-connect.md), [Kafka Streams](docs/kafka-streams.md). Versions retenues : [`docs/versions.md`](docs/versions.md) (Oracle, ojdbc17, Testcontainers, Avro 1.11.5, Confluent 7.9.11, Prometheus et Grafana relevés).
