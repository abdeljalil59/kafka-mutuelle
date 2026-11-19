# kafka-mutuelle

Projet d'apprentissage de **Kafka avec Java / Spring**, appliqué à un domaine de mutuelle
(cotisations, prélèvements, impayés). Objectif : comprendre et **observer en live** les
messages, partitions, offsets, le lag et le rebalancing, avec une preuve (test ou capture)
pour chaque expérience.

## Flux

```
cotisation-service ──► cotisations.emises ──► prelevement-service ──► prelevements.resultats ──► impaye-service
                                                                                        └──► kafka-lab (dashboard live)
```

Réalisé : `cotisation-service` ──► `cotisations.emises` ──► `prelevement-service` (décisions enregistrées dans Oracle, de façon idempotente et par lot).
Pas encore réalisé : `prelevements.resultats`, `impaye-service`, `kafka-lab`.

| Topic | Clé | Partitions | Valeur |
|---|---|---|---|
| `cotisations.emises` | `idAdherent` | 3 | **Avro** (sujet `cotisations.emises-value` du Schema Registry) |
| `prelevements.resultats` (champ `type` : ACCEPTE / REJETE) | `idAdherent` | 3 | à définir (non réalisé) |
| `impayes.relances` | `idAdherent` | 3 | à définir (non réalisé) |
| `cotisations.emises-dlt` (messages de `cotisations.emises` rejetés : illisibles, ou en échec dans le listener) | `idAdherent` (clé d'origine conservée) | 3 | octets d'origine (illisible) ou Avro (échec du listener) |
| `cotisations.retry-demo`, `…-retry`, `…-dlt` (démo `@RetryableTopic`, inactive par défaut) | libre | 3 | JSON brut |

## Stack

- Java 17, Maven multi-modules : `events-contract` (records et schéma Avro), `cotisation-service`, `prelevement-service`
- Spring Boot 4.1.2, Spring for Apache Kafka 4.1.2, kafka-clients 4.2.2
- Kafka 4.4.0 (`apache/kafka:4.4.0`, KRaft, un nœud) et Kafka UI v1.5.0, par Docker Compose (`infra/docker-compose.yml`)
- **Avro 1.11.5 et Confluent Schema Registry 7.9.11** (`io.confluent:kafka-avro-serializer` 7.9.11) : contrat typé de `cotisations.emises`
- **Oracle Free 23ai** pour `prelevement-service` (table des décisions, idempotence) : H2 y est retiré ; `ojdbc17` 23.26.3.0.0 ; tests sur Oracle dans un conteneur (Testcontainers 2.0.5, image `gvenzl/oracle-free:23.26.4-slim-faststart`)
- **H2 reste** pour `cotisation-service` (table `outbox`)
- **Actuator + Micrometer, Prometheus v3.16.0 et Grafana 13.3.1** : métriques des deux services (`/actuator/prometheus`), dashboard « Kafka Mutuelle » provisionné (fichiers dans `prometheus/` et `grafana/`)

| Composant | Port de l'hôte |
|---|---|
| Kafka (broker) | 9092 |
| Kafka UI | 8080 |
| **Schema Registry** | **8082** (8081 dans le conteneur, car 8081 est pris par `cotisation-service`) |
| `cotisation-service` (API REST et Actuator) | 8081 |
| `prelevement-service` (Actuator, `/actuator/prometheus`) | 8083 (surchargeable par `PRELEVEMENT_SERVER_PORT`) |
| **Prometheus** | **9090** |
| **Grafana** | **3000** (dashboard « Kafka Mutuelle » ; `admin`/`admin`, lecture anonyme : usage local uniquement) |
| Oracle Free 23ai local (utilisé par `prelevement-service`) | 1522 (service `freepdb1`, utilisateur `kafka_mutuelle`) |

Démarrer l'infrastructure : `docker compose -f infra/docker-compose.yml up -d` (démarre aussi Prometheus et Grafana). Les deux services scrapés tournent sur l'hôte, pas dans le compose : leurs cibles Prometheus sont `down` tant qu'ils sont arrêtés. Ne jamais utiliser `down -v` : il supprime le volume `kafka-mutuelle-data` et toutes les données.

## Consommation dans `prelevement-service`

- **Consumer Kafka en mode batch** : `@KafkaListener(batch = "true")` reçoit une `List<ConsumerRecord>`, tous les messages d'un poll.
- **Écriture idempotente par `MERGE` Oracle** (`WHEN NOT MATCHED THEN INSERT` seul) : un doublon est ignoré, sans exception ni log (plus de capture de `DuplicateKeyException`). Toutes les décisions du lot sont écrites par un seul `JdbcTemplate.batchUpdate` (`JdbcClient` n'a pas de batch dans `spring-jdbc` 7.0.10).
- **Repli un par un sur échec du lot** : le cas d'usage rejoue les décisions une à une pour trouver la première en échec, puis le listener lève `BatchListenerFailedException(index)`. Les offsets avant l'index sont validés ; seul le message fautif est réessayé puis envoyé au DLT.
- **Messages illisibles** (`value() == null`) détectés dans le listener, en mode batch rien ne les route automatiquement.
- **Métriques** : Actuator + Micrometer exposent `/actuator/prometheus` (port 8083) : séries clientes Kafka (lag par partition, débit), timer du listener, pool HikariCP, et deux compteurs custom, `prelevement_decisions_total{decision}` (décisions réellement insérées) et `prelevement_doublons_ignores_total`. Prometheus (9090) les scrape, Grafana (3000) affiche le dashboard « Kafka Mutuelle ». Le lag vient du client Kafka : il disparaît quand le consommateur est arrêté (pas d'exporter côté broker).

Détails et mesures : [phase-5-etape-2-merge-batch.md](docs/phase-5-etape-2-merge-batch.md) (batch et `MERGE`), [phase-6-metriques.md](docs/phase-6-metriques.md) (métriques).

## Prérequis

- Docker Desktop (démarré avant toute commande `docker`), y compris pour `mvn verify` : les tests de `prelevement-service` démarrent Oracle dans un conteneur (premier téléchargement : environ 1,3 Go, 5,1 Go sur disque ; démarrage de plusieurs minutes sur une machine chargée)
- Java 17
- Maven
- **Oracle Free 23ai** installé en local, pour **exécuter** `prelevement-service` (par défaut `localhost:1522/freepdb1`, utilisateur `kafka_mutuelle`). URL, utilisateur et mot de passe se surchargent par les variables d'environnement `PRELEVEMENT_DB_URL`, `PRELEVEMENT_DB_USER` et `PRELEVEMENT_DB_PASSWORD`. Le service ne démarre pas sans base ; `cotisation-service`, lui, reste autonome (H2).

## Avancement

| Phase | Contenu | Statut |
|---|---|---|
| 0 | Kafka en CLI (Docker) + Kafka UI | Terminée |
| 1 | Producer et consumer Spring, tests unitaires | En cours |
| 2 | Clés, rebalancing, plusieurs instances | En cours |
| 3 | Idempotence, retries, DLT | En cours |
| 4 | Prometheus puis Grafana | Réalisée sans exporter de lag broker (Actuator, Micrometer, Prometheus et Grafana ; lag par les métriques clientes Kafka) : [phase-6-metriques.md](docs/phase-6-metriques.md), « phase 6 » de travail |
| 5 | Dashboard live `kafka-lab` + scénarios | À faire |
| 6 | Outbox Oracle, Avro / Schema Registry, exactly-once, Kafka Streams | En cours (Avro et Schema Registry réalisés ; `prelevement-service` migré sur Oracle, écriture idempotente par `MERGE` et par lot ; outbox vu sur H2 en phase 3, pas encore sur Oracle ; exactly-once et Kafka Streams à faire) |
| 7 | Tests d'intégration complets, éventuellement ELK | À faire |

## Études réalisées

Sept études, chacune avec son journal (constaté, déduit, non vérifié) et ses tests. Leur numéro de « phase » est un nom de travail : au tableau ci-dessus, les trois premières relèvent de la phase 3 (l'outbox y est vu sur H2, avant l'outbox Oracle de la phase 6), les trois suivantes de la phase 6 (la « phase 5 » de travail n'est pas la phase 5 du tableau, `kafka-lab`), et la dernière, la « phase 6 » de travail (métriques), est la phase 4 du tableau (Prometheus et Grafana).

| Étude | Contenu | Journal |
|---|---|---|
| Consommateur idempotent | Décision de prélèvement écrite une seule fois par `idEvenement` (clé primaire, doublon absorbé) | [phase-3-idempotence.md](docs/phase-3-idempotence.md) |
| Outbox | Cotisation enregistrée en base, publiée par un relais planifié, au moins une fois | [phase-3-outbox.md](docs/phase-3-outbox.md) |
| Retry topic | Retries non bloquants avec `@RetryableTopic`, cohabitation avec le DLT, perte de l'ordre par clé | [phase-3-retry-topic.md](docs/phase-3-retry-topic.md) |
| Avro et Schema Registry | Schéma `CotisationEmise` enregistré dans le registre (port 8082), sérialisation Avro de bout en bout | [phase-4-avro-schema-registry.md](docs/phase-4-avro-schema-registry.md) |
| Oracle (`prelevement-service`) | H2 remplacé par Oracle Free 23ai ; tests sur Oracle dans un conteneur (Testcontainers) ; découvertes sur `Instant` et ORA-12899 | [phase-5-etape-1-oracle.md](docs/phase-5-etape-1-oracle.md) |
| `MERGE` et écriture par lot (`prelevement-service`) | Idempotence par `MERGE … WHEN NOT MATCHED` sans exception ; listener Kafka en mode batch, `JdbcTemplate.batchUpdate`, repli un par un et `BatchListenerFailedException(index)` | [phase-5-etape-2-merge-batch.md](docs/phase-5-etape-2-merge-batch.md) |
| Métriques (Actuator, Micrometer, Prometheus, Grafana) | Séries clientes Kafka, timer du listener et pool HikariCP ; deux compteurs custom (décisions, doublons) ; Prometheus et dashboard Grafana provisionnés ; relevé du smoke test | [phase-6-metriques.md](docs/phase-6-metriques.md) |

Journal d'apprentissage : [`docs/`](docs/) : [phase 0, Kafka en CLI](docs/phase-0-cli.md), [phase 1, producteur](docs/phase-1-producteur.md), [phase 1, consommateur](docs/phase-1-consommateur.md), [phase 2, rebalancing](docs/phase-2-rebalancing.md), [phase 3, message invalide](docs/phase-3-message-invalide.md), [phase 3, idempotence](docs/phase-3-idempotence.md), [phase 3, outbox](docs/phase-3-outbox.md), [phase 3, retry topic](docs/phase-3-retry-topic.md), [phase 4, Avro et Schema Registry](docs/phase-4-avro-schema-registry.md), [phase 5 étape 1, Oracle](docs/phase-5-etape-1-oracle.md), [phase 5 étape 2, MERGE et batch](docs/phase-5-etape-2-merge-batch.md), [phase 6, métriques](docs/phase-6-metriques.md). Versions retenues : [`docs/versions.md`](docs/versions.md) (Oracle, ojdbc17, Testcontainers, Avro 1.11.5, Confluent 7.9.11, Prometheus et Grafana relevés).
