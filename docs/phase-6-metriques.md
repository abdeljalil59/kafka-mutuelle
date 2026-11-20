# Phase 6 : métriques avec Actuator, Micrometer, Prometheus et Grafana

> **Mise à jour (20/11/2026)** : ce journal décrit l'état du 19/11/2026 (listener batch, deux services, dashboard de 6 panneaux). Depuis :
> - Le listener est **en mode record** (commit `8010336`) : le panneau « Latence du listener (par lot) » s'appelle « par message » (commit `15d89f3`), et la limite « `doublons.ignores` surestimé après un lot en échec » (§9) ne se présente plus avec le listener actuel.
> - Il y a **trois** services scrapés : `impaye-service` (port 8084, job `impaye-service`) s'ajoute.
> - Le dashboard compte **5 lignes et 14 panneaux** : la ligne « Dead Letter Topic » (`0042f62`, filtrée sur `name="kafkaTemplate"` par `d751f53`), puis la ligne « Transactions » (`97e45cd`).
> - Des séries `kafka_producer_*` existent maintenant (producteur transactionnel) : `kafka_producer_txn_*_time_ns_total`, des **cumuls de temps**, pas des compteurs de transactions (voir [transactions-exactement-une-fois.md](transactions-exactement-une-fois.md), §8).
> - Les 148 tests (§4) sont devenus 212 ([README](../README.md)).

Environnement (19/11/2026, heure locale +01:00) : Spring Boot 4.1.2, Micrometer 1.17.2, client Prometheus 1.7.0, spring-kafka 4.1.2, kafka-clients 4.2.2, Prometheus v3.16.0, Grafana 13.3.1. Versions : voir [versions.md](versions.md). Étape précédente : [phase-5-etape-2-merge-batch.md](phase-5-etape-2-merge-batch.md).

Pour chaque point : ce qui est **constaté** (test, sortie réelle, source ou doc lue), **déduit** et **non vérifié**. Les résultats du smoke test (§8) sont des **relevés de l'utilisateur** : je ne l'ai pas rejoué.

## 1. Objectif et périmètre
- Observer en live `cotisation-service` et `prelevement-service` : lag, débit, latence du listener, connexions du pool, décisions métier. Troisième niveau de « live » du brief (tendances), après Kafka UI et le futur dashboard maison.
- **Numérotation** : « phase 6 » est le nom de travail de cette étude. Au brief, Prometheus et Grafana sont la **phase 4** ; la phase 6 du brief (outbox Oracle, Avro, Streams) est autre chose.
- **Décision** : métriques clientes Kafka seulement, **pas d'exporter de lag côté broker** (le point ouvert du brief, §10, est tranché ainsi pour l'instant).
- Inchangés : le domaine et le cas d'usage, le code Avro, les configurations Kafka, `ErreursKafkaConfig`.

## 2. Étapes et commits
| Étape | Commit | Contenu | Tests |
|---|---|---|---|
| 1 | `d672866` | Actuator, Micrometer, registre Prometheus, `/actuator/prometheus` sur les deux services | 139 → 141 |
| 2 | `efb7c2a` | Compteurs custom dans `JournalDecisionsJdbc` | 141 → 148 |
| 3 | `d378909` | Prometheus dans le compose, `prometheus/prometheus.yml` | aucun test Maven |
| 4 | `d75afcb` | Grafana dans le compose, datasource et dashboard provisionnés | aucun test Maven |

Décisions validées par l'utilisateur : `spring-boot-starter-webmvc` et port 8083 pour `prelevement-service` ; pas de jauge d'outbox pour l'instant ; métriques clientes d'abord ; tags d'images vérifiés après `docker pull` avant d'être épinglés.

## 3. Étape 1 : Actuator, Micrometer et exposition
- **Dépendances** : `spring-boot-starter-actuator` (qui apporte `spring-boot-starter-micrometer-metrics`, **constaté** dans son pom 4.1.2 sur Maven Central) et `micrometer-registry-prometheus` (scope runtime, versions du BOM Boot : Micrometer 1.17.2 et client Prometheus 1.7.0, **constaté**). En plus pour `prelevement-service` : `spring-boot-starter-webmvc`, car ce service n'avait aucun serveur web et la doc dit que les endpoints HTTP d'Actuator concernent les applications web.
- **Exposition** : `management.endpoints.web.exposure.include: health,metrics,prometheus` (doc : seul `health` l'est par défaut). Port `${PRELEVEMENT_SERVER_PORT:8083}` : 8080 est pris par Kafka UI, 8081 par `cotisation-service`, 8082 par le registre.
- **Métriques Kafka sans code** (**constaté**) : `KafkaMetricsAutoConfiguration` (spring-boot-kafka 4.1.2) personnalise les fabriques de consommateurs et de producteurs autoconfigurées ; la doc Boot le dit (« Auto-configuration registers a MicrometerConsumerListener and MicrometerProducerListener… »). Les timers `spring.kafka.listener` et `spring.kafka.template` viennent de spring-kafka (doc 4.1.2).
- **Tests** : un test d'exposition par service, ajouté à une classe d'intégration existante (`CotisationEmiseListenerIntegrationTest`, `OutboxIntegrationTest`) : serveur web sur port aléatoire, `@AutoConfigureMetrics`, `GET /actuator/prometheus` après un vrai message, assertion de noms de séries. Sans `@AutoConfigureMetrics`, un `@SpringBootTest` n'exporte rien (doc testing 4.1.2 : « meter registries, except the in-memory backed, are not auto-configured »). L'annotation vient de `spring-boot-starter-actuator-test` (ajout hors de la liste initiale) ; les starters de test Kafka et webmvc ne l'apportent pas (**constaté**).
- **Capture réelle** (démarrage de `prelevement-service`, 45 s) : HTTP 200, `text/plain;version=0.0.4;charset=utf-8`, 209 séries, 3 endpoints exposés, Tomcat sur 8083, aucun WARN ni ERROR.

### Séries constatées (capture réelle)
| Besoin | Série | Étiquettes |
|---|---|---|
| Lag par partition | `kafka_consumer_fetch_manager_records_lag` | `client_id`, `partition`, `topic` |
| Lag maximum du client | `kafka_consumer_fetch_manager_records_lag_max` | `client_id`, `spring_id` |
| Débit | `kafka_consumer_fetch_manager_records_consumed_total` et `_rate` | `client_id`, `topic` (et version globale sans `topic`) |
| Latence du listener | `spring_kafka_listener_seconds_{count,sum,max}` | `name`, `result`, `exception` |
| Envois | `spring_kafka_template_seconds_count` | `name="kafkaTemplate"`, `result`, `exception` |
| Pool | `hikaricp_connections_{active,idle,pending,max,min}` | `pool="HikariPool-1"` |

## 4. Étape 2 : compteurs custom
- **Où** : dans l'adaptateur `JournalDecisionsJdbc` (Micrometer reste hors du domaine et de `application`). Deux compteurs : `prelevement.decisions` (étiquette `decision`, ACCEPTE ou REJETE) et `prelevement.doublons.ignores`.
- **Comment** : une boucle sur le tableau renvoyé par `batchUpdate` : résultat 1 → décision insérée, 0 → doublon ignoré, autre valeur (par exemple -2, `SUCCESS_NO_INFO`) → rien. **Constaté** sur Oracle à travers Spring (`JournalDecisionsJdbcTest`) : 1 pour une insertion, 0 pour un doublon, y compris un doublon à l'intérieur du lot. « Décisions » compte donc les lignes **réellement insérées**, pas les messages reçus.
- **Séries créées à 0 dès le démarrage** : sinon le passage de « série absente » à 1 échappe à `rate()` et `increase()`. Du coup, la seule présence de la série ne prouve rien : le test d'intégration vérifie sa **valeur** (≥ 1 après un message de 50,00).
- **Noms Prometheus constatés** avec le vrai registre du projet (expérience hors dépôt, classpath d'exécution de `prelevement-service`) : `prelevement_decisions_total{decision="ACCEPTE"|"REJETE"}` et `prelevement_doublons_ignores_total` (`# HELP` vide : pas de description).
- **Tests** : 6 tests Mockito (`MeterRegistry`, `Counter` et `JdbcTemplate` simulés, sans base), un test sur Oracle avec un vrai registre en mémoire, un test d'exposition. Mockito sur des classes qui ne sont pas des ports : demandé explicitement ; `registry.counter(nom, étiquettes…)` est utilisé car `Counter.builder(…).register(registre)` passe par une méthode package-private de `MeterRegistry` (**constaté** dans le jar), que Mockito ne peut pas simuler.
- **Build** : `BUILD SUCCESS`, 148 tests (16 + 55 + 77), 9 min 17 s.

## 5. Étape 3 : Prometheus
- `prom/prometheus:v3.16.0`, port 9090, `prometheus/prometheus.yml` à la racine (`scrape_interval: 15s`, deux jobs `/actuator/prometheus` sur `host.docker.internal:8081` et `:8083`). Le compose est dans `infra/` : montage `../prometheus/prometheus.yml:…:ro`.
- **Tag vérifié par `docker pull`** : `v3.16.0` et `latest` ont le même condensé (`sha256:e04987641b57…`), `promtool --version` annonce 3.16.0.
- **Validation** : `docker compose config --quiet` (code 0), `promtool check config` (code 0 sur le fichier, 1 sur un fichier invalide volontaire).
- `depends_on: kafka` conservé à la demande, alors que Prometheus ne scrape pas le broker. Pas de volume : l'historique des métriques est perdu si le conteneur est recréé.

## 6. Étape 4 : Grafana
- `grafana/grafana:13.3.1`, port 3000, fichiers dans `grafana/` (racine) montés en lecture seule : `provisioning/datasources/prometheus.yml` (`uid: prometheus`, `http://prometheus:9090`, réseau Docker interne) et `provisioning/dashboards/` (`dashboards.yml` et `kafka-mutuelle.json`). `GF_SECURITY_ADMIN_PASSWORD=admin`, accès anonyme en lecture : démonstration locale uniquement (le port est publié sur toutes les interfaces de la machine).
- **Tag vérifié** : `13.3.1` et `latest` ont le même condensé (`sha256:0615ec4f…`), `grafana --version` annonce 13.3.1.
- **Dashboard « Kafka Mutuelle »** : 3 lignes, 6 panneaux de type `timeseries` (le type `graph` demandé est l'ancien format), sans variable de template :

| Ligne | Panneau | Requête |
|---|---|---|
| Vue d'ensemble | Lag par partition | `kafka_consumer_fetch_manager_records_lag{topic="cotisations.emises"}` |
| Vue d'ensemble | Débit consommé | `rate(kafka_consumer_fetch_manager_records_consumed_total{topic="cotisations.emises"}[1m])` |
| Décisions métier | Décisions par minute (empilées) | `sum by (decision) (increase(prelevement_decisions_total[1m]))` |
| Décisions métier | Doublons ignorés par minute | `increase(prelevement_doublons_ignores_total[1m])` |
| Latence et pool | Latence du listener (par lot) | moyenne `rate(sum)/rate(count)` et `spring_kafka_listener_seconds_max`, `result="success"` |
| Latence et pool | Connexions HikariCP | `hikaricp_connections_active` et `_idle`, légende par `job` |

- **Écarts à la demande** : filtre `topic="cotisations.emises"` ajouté au débit (sans lui, trois séries : la globale et les deux copies par topic) ; `sum by (decision)` pour empiler.
- **Validations avant tout démarrage** : JSON valide, identifiants uniques, `uid` de datasource cohérent avec le fichier de provisioning ; chaque nom de série et chaque étiquette filtrée existent dans les captures réelles ; les 8 requêtes passent `promtool check rules` (code 0, et code 1 sur une requête invalide volontaire) ; YAML analysés par SnakeYAML ; `docker compose config --quiet` (code 0).

## 7. Découvertes et incidents
- **Doublon de topic (constaté)** : le client Kafka émet chaque série par topic en double, `topic="cotisations.emises"` et `topic="cotisations_emises"` (la doc Kafka 4.4 dit que la copie avec des soulignés est dépréciée). 6 séries de lag pour 3 partitions. Toute requête filtre sur le nom avec le point.
- **Lag par partition absent au démarrage (constaté)** : absent 17 s après l'assignation, présent à 75 s. **Hypothèse non vérifiée** : actualisation périodique de Micrometer. Le test n'asserte donc que `records_lag_max`, présent dès la création du consommateur.
- **Séries absentes au démarrage (constaté)** : aucune `kafka_producer_*` tant que le producteur du DLT n'a rien envoyé ; les timers listener et template existent à 0 avec `result="success"` seulement. Les séries `result="failure"` n'ont pas été observées.
- **Délai de démarrage d'Oracle en test (constaté)** : deux builds complets ont échoué parce que le conteneur dépassait 5 minutes (16 erreurs, toutes au démarrage d'`OracleIntegrationTest`). L'hypothèse « échec ponctuel » (relance sans modification) est **fausse**. Mesures : 8 min 41 s puis 5 min 50 s. Délai porté à 10 minutes. La machine était plus chargée (875 Mo de RAM libre) ; **cause non établie**. Un build de `prelevement-service` dure alors jusqu'à 11 minutes.
- **Jar remplacé sous un processus en cours** : pendant la documentation de la phase 5, un build a échoué au `repackage` à cause d'un `java -jar` de l'utilisateur ; le build avait déjà remplacé ce jar. À garder en tête avant `mvn verify` pendant un smoke test.
- **Page Kafka 4.4 « Monitoring » (constaté)** : les métriques broker listées pour le lag ne concernent que la réplication (`ReplicaFetcherManager`, `FetcherLagMetrics`), aucune le lag d'un groupe de consommateurs. Un JMX exporter sur le broker ne répond donc pas à ce besoin.

## 8. Smoke test de l'utilisateur (relevés de l'utilisateur)
- Prometheus scrape les deux services via `host.docker.internal` sous Windows : **constaté** (les métriques apparaissent dans Grafana).
- Dashboard « Kafka Mutuelle » chargé par provisioning : **constaté**, 6 panneaux en 3 lignes.
- **Lag par partition** : 3 partitions visibles, lag 0 (consommateur à jour).
- **Débit consommé** : pointe visible à l'envoi d'un message.
- **Latence du listener** : pointe visible.
- **Connexions HikariCP** : 4 séries (actives et inactives pour les deux services).
- **Décisions et doublons** : panneaux visibles avec les légendes ACCEPTE et REJETE et « doublons ignorés ». `increase(…[1m])` a besoin de **deux scrapes** pour afficher un point.
- **Message traité** : `offset=4`, `ADH-SMOKE-GRAFANA`, montant 75.00, décision ACCEPTE.

## 9. Limites connues
- **Lag absent quand le consommateur est arrêté** (**déduit** : les séries viennent du client Kafka du processus, qui n'est plus scrapé ; non testé en conditions réelles). C'est le moment où le lag intéresse le plus. Seul un exporter côté broker le couvrirait : `kafka-exporter` annonce « 0.10.1.0 (and later) » et KMinion « v0.11+ », **aucun ne parle de Kafka 4.x** (README lus) : compatibilité avec le broker 4.4 **non vérifiée**.
- **`doublons.ignores` surestimé après un lot en échec** : un lot qui échoue ne renvoie aucun tableau de résultats ; ses lignes déjà écrites ne sont pas comptées comme décisions, et leur rejeu une par une les compte comme doublons ignorés. Commentaire dans le code et description du panneau.
- **Latence « par lot »** : le timer `spring.kafka.listener` est enregistré une fois par appel du listener ; la doc ne précise pas, pour un listener batch, s'il compte par lot ou par message (**déduit** : un enregistrement par lot ; non vérifié avec un lot de plusieurs messages).
- **Aucun volume** pour Prometheus et Grafana : rien n'est conservé à la recréation, sauf les fichiers provisionnés.
- **Sécurité locale** : Grafana publié sur toutes les interfaces avec `admin`/`admin` et lecture anonyme.

## 10. Ce qui est constaté, déduit, non vérifié
| | Constaté | Déduit | Non vérifié |
|---|---|---|---|
| Exposition | `/actuator/prometheus` 200, 209 séries, noms et étiquettes (capture) ; tests d'exposition verts | | Séries `result="failure"` |
| Lag | 3 partitions, lag 0 (smoke) ; doublon de topic ; présent entre 17 s et 75 s après l'assignation | Actualisation périodique de Micrometer | Lag non nul ; comportement à l'arrêt du consommateur |
| Compteurs | Noms Prometheus ; 1 et 0 de Oracle à travers Spring ; valeur ≥ 1 après un message | Comptage faussé après un lot en échec rejoué | Compteur de doublons en conditions réelles ; décision REJETE en direct |
| Prometheus | Tag et condensé ; `promtool` ; cibles joignables sous Windows (smoke) | | Données conservées après recréation (aucun volume) |
| Grafana | Tag et condensé ; dashboard chargé, 6 panneaux en 3 lignes (smoke) ; requêtes valides syntaxiquement | `schemaVersion` 39 migré par Grafana 13 (le chargement a réussi) | Exactitude des valeurs affichées au-delà du smoke |
| Latence | Pointe visible (smoke) | Un enregistrement par lot | Lot de plusieurs messages |
| Exporter broker | Page Kafka 4.4 : aucun lag de groupe côté broker | | `kafka-exporter` et KMinion avec Kafka 4.4 |
| Build | 148 tests verts, 9 min 17 s | | Durée sur une autre machine (Oracle jusqu'à 8 min 41 s) |

## 11. Journaux antérieurs devenus partiellement obsolètes
- [phase-5-etape-1-oracle.md](phase-5-etape-1-oracle.md) : « Délai de démarrage de 5 minutes » (§2) est périmé, il est de 10 minutes depuis `d672866` ; le compte de 53 tests aussi.
- [phase-5-etape-2-merge-batch.md](phase-5-etape-2-merge-batch.md) : 139 tests au total est périmé (148 ici) ; « Rien de prévu dans le brief » (§10) est périmé pour l'observabilité. Non modifiés.

## 12. Suite
Pas d'exporter broker pour l'instant (décision). Pistes : un essai jetable de `kafka-exporter` ou de KMinion contre le broker 4.4 si le lag à l'arrêt devient nécessaire ; la jauge d'outbox (écartée pour l'instant) ; la suite du brief (dashboard live `kafka-lab`).
