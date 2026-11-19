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

Réalisé : `cotisation-service` ──► `cotisations.emises` ──► `prelevement-service` (décision enregistrée en base).
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
- H2 : table des décisions (idempotence) côté `prelevement-service`, table `outbox` côté `cotisation-service`

| Composant | Port de l'hôte |
|---|---|
| Kafka (broker) | 9092 |
| Kafka UI | 8080 |
| **Schema Registry** | **8082** (8081 dans le conteneur, car 8081 est pris par `cotisation-service`) |
| `cotisation-service` (API REST) | 8081 |

Démarrer l'infrastructure : `docker compose -f infra/docker-compose.yml up -d`. Ne jamais utiliser `down -v` : il supprime le volume `kafka-mutuelle-data` et toutes les données.

## Prérequis

- Docker Desktop (démarré avant toute commande `docker`)
- Java 17
- Maven

## Avancement

| Phase | Contenu | Statut |
|---|---|---|
| 0 | Kafka en CLI (Docker) + Kafka UI | Terminée |
| 1 | Producer et consumer Spring, tests unitaires | En cours |
| 2 | Clés, rebalancing, plusieurs instances | En cours |
| 3 | Idempotence, retries, DLT | En cours |
| 4 | Prometheus puis Grafana | À faire |
| 5 | Dashboard live `kafka-lab` + scénarios | À faire |
| 6 | Outbox Oracle, Avro / Schema Registry, exactly-once, Kafka Streams | En cours (Avro et Schema Registry réalisés ; outbox vu sur H2 en phase 3 ; Oracle, exactly-once et Kafka Streams à faire) |
| 7 | Tests d'intégration complets, éventuellement ELK | À faire |

## Études réalisées

Quatre études, chacune avec son journal (constaté, déduit, non vérifié) et ses tests. Leur numéro de « phase » est un nom de travail : au tableau ci-dessus, les trois premières relèvent de la phase 3 (l'outbox y est vu sur H2, avant l'outbox Oracle de la phase 6) et la quatrième de la phase 6.

| Étude | Contenu | Journal |
|---|---|---|
| Consommateur idempotent | Décision de prélèvement écrite une seule fois par `idEvenement` (clé primaire, doublon absorbé) | [phase-3-idempotence.md](docs/phase-3-idempotence.md) |
| Outbox | Cotisation enregistrée en base, publiée par un relais planifié, au moins une fois | [phase-3-outbox.md](docs/phase-3-outbox.md) |
| Retry topic | Retries non bloquants avec `@RetryableTopic`, cohabitation avec le DLT, perte de l'ordre par clé | [phase-3-retry-topic.md](docs/phase-3-retry-topic.md) |
| Avro et Schema Registry | Schéma `CotisationEmise` enregistré dans le registre (port 8082), sérialisation Avro de bout en bout | [phase-4-avro-schema-registry.md](docs/phase-4-avro-schema-registry.md) |

Journal d'apprentissage : [`docs/`](docs/) : [phase 0, Kafka en CLI](docs/phase-0-cli.md), [phase 1, producteur](docs/phase-1-producteur.md), [phase 1, consommateur](docs/phase-1-consommateur.md), [phase 2, rebalancing](docs/phase-2-rebalancing.md), [phase 3, message invalide](docs/phase-3-message-invalide.md), [phase 3, idempotence](docs/phase-3-idempotence.md), [phase 3, outbox](docs/phase-3-outbox.md), [phase 3, retry topic](docs/phase-3-retry-topic.md), [phase 4, Avro et Schema Registry](docs/phase-4-avro-schema-registry.md). Versions retenues : [`docs/versions.md`](docs/versions.md) (pas encore à jour pour Avro 1.11.5 et Confluent 7.9.11).
