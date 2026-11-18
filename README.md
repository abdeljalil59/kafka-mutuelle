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

| Topic | Clé | Partitions |
|---|---|---|
| `cotisations.emises` | `idAdherent` | 3 |
| `prelevements.resultats` (champ `type` : ACCEPTE / REJETE) | `idAdherent` | 3 |
| `impayes.relances` | `idAdherent` | 3 |
| `cotisations.emises-dlt` (messages de `cotisations.emises` rejetés à la désérialisation) | `idAdherent` (clé d'origine conservée) | 3 |

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
| 6 | Outbox Oracle, Avro / Schema Registry, exactly-once, Kafka Streams | À faire |
| 7 | Tests d'intégration complets, éventuellement ELK | À faire |

Journal d'apprentissage : [`docs/`](docs/) : [phase 0, Kafka en CLI](docs/phase-0-cli.md), [phase 1, producteur](docs/phase-1-producteur.md), [phase 1, consommateur](docs/phase-1-consommateur.md), [phase 2, rebalancing](docs/phase-2-rebalancing.md), [phase 3, message invalide](docs/phase-3-message-invalide.md), [phase 3, idempotence](docs/phase-3-idempotence.md). Versions retenues : [`docs/versions.md`](docs/versions.md).
