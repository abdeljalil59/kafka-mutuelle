# Kafka Streams : `stats-service`

Objectif : agréger en temps réel les résultats de prélèvement **par adhérent** (nombre d'acceptés, de rejetés, montant total, dernier événement) et exposer le résultat par une API REST, sans base de données : l'état vit dans un **State Store** local, sauvegardé dans Kafka.

> **Statut.** La logique d'agrégation et la déduplication par `idEvenement` sont prouvées par 30 tests sans broker (`TopologyTestDriver`, registre Avro factice `mock://`). Le service a été **vérifié à la main** contre le broker 4.4.0 et le registre (topics internes créés, `GET /stats` sur un store alimenté, restauration après redémarrage) et la **déduplication a été validée en direct** : un rejeu des offsets laisse les chiffres inchangés (§8 bis). **Non vérifié** : l'origine de la restauration (disque local ou changelog), le comportement exact des requêtes pendant le démarrage, le rôle du cache, le cas hors rétention contre le broker réel (prouvé seulement par un test). Voir §9.

## 1. KStream et KTable

| | KStream | KTable |
|---|---|---|
| Sens | un **flux d'événements** : chaque message est un fait indépendant | un **état** : la dernière valeur connue par clé |
| Deux messages de même clé | deux événements | le second **remplace** le premier |
| Ici | `prelevements.resultats` : chaque décision de prélèvement compte | `stats-store` : les stats courantes de chaque adhérent |

Une agrégation transforme un KStream en KTable : chaque événement met à jour la ligne de son adhérent. La KTable est la « table » dont le KStream est le journal des modifications (dualité flux/table).

## 2. La topology

Code : `adapter/in/kafka/StatsTopology.java`.

```java
builder.stream(topic, Consumed.with(Serdes.String(), resultatSerde))          // KStream<String, ResultatPrelevement>
       .selectKey((cle, resultat) -> resultat.getIdAdherent())                 // re-keying
       .repartition(Repartitioned.<String, ResultatPrelevement>as("stats-store")...)   // repartitionnement explicite
       .processValues(new DeduplicationResultats(retention), Named.as("deduplication-resultats"))   // doublons écartés
       .groupByKey(Grouped.with(Serdes.String(), resultatSerde))
       .aggregate(StatsAdherent::vide,                                         // valeur initiale
                  (idAdherent, resultat, stats) -> ajouter(stats, resultat),   // agrégateur
                  Named.as("stats-par-adherent"),
                  Materialized.<String, StatsAdherent, KeyValueStore<Bytes, byte[]>>as("stats-store")
                          .withKeySerde(Serdes.String())
                          .withValueSerde(statsSerde));                        // KTable matérialisée
```

- **`selectKey` et repartitionnement.** La clé du topic est déjà `idAdherent` (`PublicationResultatsKafka.java:39` dans `prelevement-service`), mais Kafka Streams ne peut pas le deviner : `selectKey` marque la clé comme modifiée, et le repartitionnement est donc nécessaire. Il est déclaré explicitement par `repartition()`, avec le nom du store, pour que la déduplication et l'agrégation soient dans la même tâche. C'est un coût réel (un topic et un aller-retour de plus), ici accepté pour la démonstration. Prouvé par `leChangementDeClePasseParUnTopicDeRepartitionnement` ; le nom du topic (`stats-service-stats-store-repartition`) est confirmé en direct (§8 bis).
- **Déduplication par `idEvenement`** (`DeduplicationResultats`) : un `FixedKeyProcessor` avec un `WindowStore` (`stats-dedup-store`) ne transmet que la première occurrence d'un `idEvenement`. La rétention se règle par `stats.deduplication.retention` (défaut `7d`, voir §7). La fenêtre est de 1 ms : une seule entrée par événement, la contrainte constatée étant « rétention >= fenêtre ». **Constaté** : avec le `WindowStore`, un `fetch` ne retrouve plus une entrée plus ancienne que la rétention (sonde P2). **Déduit, non mesuré** : le choix du `WindowStore` plutôt que d'un `KeyValueStore` avec punctuator, parce que l'expiration se fait par rétention sans parcourir tout le store. Aucune mesure de coût n'a été faite.
- **L'agrégateur est du domaine pur** : `StatsAdherent` est un `record` immuable (`avecAccepte`, `avecRejete`), sans import Kafka ni Spring. Chaque message produit un nouvel objet.
- **`montantTotal`** est un `double` (consigne : simplifier la sérialisation JSON). Il cumule **tous** les résultats, acceptés et rejetés. Un `double` accumule des erreurs d'arrondi sur de gros volumes ; pour de l'argent réel, il faudrait un `BigDecimal` ou des centimes entiers.
- **`dernierEvenement`** : le plus grand `decideLe` vu, en millisecondes (les microsecondes d'Avro sont tronquées). Un message arrivé en retard ne le fait pas reculer.

## 3. State Store

- `Materialized.as("stats-store")` nomme le store : c'est ce nom que les Interactive Queries utilisent.
- Le store est **local** (RocksDB, dans `spring.kafka.streams.state-dir`, `./target/state-stores` par défaut) et rapide.
- Il est **sauvegardé dans un topic de changelog** interne (`stats-service-stats-store-changelog`). Si le répertoire local est perdu, Streams reconstruit le store en relisant le changelog.
- Sérialisation : `StatsAdherentSerde`, un Serde JSON écrit à la main avec Jackson (choix de simplicité et de testabilité, plutôt que `JsonSerde` de spring-kafka qui demande une configuration de types de confiance).

## 4. Interactive Queries

Code : `adapter/out/streams/LectureStatsStateStore.java`, derrière le port `LectureStats`.

```java
ReadOnlyKeyValueStore<String, StatsAdherent> store =
        streams.store(StoreQueryParameters.fromNameAndType("stats-store", QueryableStoreTypes.keyValueStore()));
store.get("ADH-CONNECT-01");   // une clé
store.all();                   // toutes les clés (itérateur à fermer)
```

| Endpoint | Réponse |
|---|---|
| `GET /stats/{idAdherent}` | `200` avec le JSON, `404` si l'adhérent est inconnu |
| `GET /stats` | `200` : objet JSON `{ "ADH-1": {...}, ... }`, trié par adhérent |
| (les deux) | `503` si Kafka Streams n'est pas encore `RUNNING` ou se rééquilibre (`InvalidStateStoreException`) |

Exemple de corps : `{"nombreAcceptes":2,"nombreRejetes":1,"montantTotal":235.0,"dernierEvenement":1763640000000}`.

**Limite assumée : mono-instance.** Chaque instance ne détient que le store des partitions qui lui sont attribuées. Avec plusieurs instances, un `GET /stats/{id}` tomberait sur une instance qui n'a pas cet adhérent, et `GET /stats` ne listerait qu'une partie. Il faudrait alors `application.server`, `queryMetadataForKey` et un appel vers l'instance qui détient la clé. Non fait : une seule instance suffit à la démonstration.

## 5. Garanties et limites de l'agrégation

- **Au moins une fois** (`processing.guarantee = at_least_once`, constaté dans le journal de démarrage du service, bloc `StreamsConfig values` ; valeur par défaut de Streams 4.2.2 relevée aussi dans la bibliothèque) : après un crash, un message peut être retraité. La déduplication écarte ce retraitement tant que le store de déduplication a gardé l'entrée. Les deux stores (déduplication et agrégat) sont écrits séparément : en at-least-once, un crash entre les deux pourrait laisser l'un en avance sur l'autre (déduit, non prouvé). `exactly_once_v2` n'a pas été essayé ici.
- **`read_committed`** (`consumer.isolation.level` dans `application.yml`) : `prelevement-service` publie dans une transaction Kafka ; sans ce réglage, un résultat d'une transaction annulée serait compté. Prouvé : `StreamsConfigTest` montre que Streams transmet le réglage au consommateur ; l'effet contre le vrai broker n'est pas constaté.
- **Déduplication par `idEvenement`** : un résultat rejoué (remise à zéro des offsets, ou `prelevement-service` qui republie la même décision avec le même `idEvenement`) n'est compté qu'une fois, **dans la limite de la rétention** (`stats.deduplication.retention`, 7 jours par défaut ; `prelevements.resultats` a `retention.ms=604800000`, constaté). Un événement rejoué après cette durée est recompté (test `unIdEvenementRejoueApresLaRetentionEstRecompte`). Le temps utilisé est celui du record Kafka (`message.timestamp.type=CreateTime`). À comparer avec `impaye-service`, qui dédoublonne en base. **Prouvée par des tests et validée en direct** pour un rejeu des offsets (§8 bis) ; le cas hors rétention n'est prouvé que par le test.
- **Messages antérieurs au démarrage** (`auto.offset.reset`) : **constaté : les messages antérieurs ont bien été agrégés** (les 4 messages déjà présents sur `prelevements.resultats` au premier démarrage, voir §8 bis). Le réglage n'est pas posé dans `application.yml` et la valeur par défaut de Streams n'a pas été lue dans la documentation : seul le comportement est constaté. Changer l'`application-id` repart d'un état vide.

## 6. Architecture du module

```
domain/                   StatsAdherent (record immuable, agrégation pure)
application/              ConsulterStats (cas d'usage), port out LectureStats, StatsIndisponibleException
adapter/in/kafka/         StatsTopology, DeduplicationResultats, StatsAdherentSerde
adapter/in/rest/          StatsController, StatsReponse (DTO distinct du domaine)
adapter/out/streams/      LectureStatsStateStore (Interactive Queries)
config/                   StatsConfig (câblage Spring, Serde Avro)
```

La topology est classée en `adapter/in` : elle consomme un topic, comme un listener Kafka. Elle n'est pas dans `domain` car elle importe Kafka Streams.

## 7. Configuration

`stats-service/src/main/resources/application.yml` : port **8086** (`STATS_SERVER_PORT`), `application-id: stats-service`, brokers `localhost:9092`, registre `http://localhost:8082`, répertoire des stores `STATS_STATE_DIR`, rétention de la déduplication `stats.deduplication.retention` (`7d` par défaut). L'`application-id` sert aussi de groupe de consommation et de préfixe des topics internes.

## 8. Tests (30)

| Fichier | Tests | Ce qui est prouvé |
|---|---|---|
| `StatsTopologyTest` | 13 | comptage acceptés/rejetés, montant, indépendance des adhérents, re-keying, dernier événement, repartitionnement, store nommé ; déduplication : même `idEvenement` envoyé deux fois, deux `idEvenement` distincts au contenu identique, rejeu d'un lot de 3 résultats, rappel après la rétention, absence de ligne au changelog pour un doublon (`TopologyTestDriver`, sans broker) |
| `StatsAdherentTest` | 5 | agrégat pur, immuabilité, `dernierEvenement` non régressif |
| `StatsControllerTest` | 5 | 200, 404, liste, liste vide, 503 (MockMvc autonome, cas d'usage simulé) |
| `StatsAdherentSerdeTest` | 3 | aller-retour JSON, champs du JSON, `null` |
| `ConsulterStatsTest` | 3 | délégation au port (Mockito sur le port seulement) |
| `StreamsConfigTest` | 1 | `consumer.isolation.level` bien transmis au consommateur |

Sous Windows, `TopologyTestDriver` journalise `Failed to change permissions for the directory ...\kafka-streams` : sans effet sur les résultats (changement de permissions POSIX impossible sur NTFS).

## 8 bis. Constaté en live

Observations faites à la main sur `stats-service` démarré sur l'hôte contre le compose (broker 4.4.0, registre sur 8082). Aucun journal de commandes n'a été conservé : ce sont des relevés, pas des preuves rejouables.

### Constaté

- **Premier démarrage** : `GET /stats` renvoie `{"ADH-CONNECT-01":{"nombreAcceptes":2,"nombreRejetes":1,"montantTotal":235.0,...},"ADH-CONNECT-02":{"nombreAcceptes":0,"nombreRejetes":1,"montantTotal":200.0,...}}`, soit les 4 messages déjà présents sur `prelevements.resultats`, lus depuis le début (earliest).
- **Mise à jour en direct** : après un `POST /cotisations` (ADH-CONNECT-01, 100.00), `GET /stats/ADH-CONNECT-01` passe à `nombreAcceptes` 3 et `montantTotal` 335.0 en quelques secondes.
- **Topics internes** : `kafka-topics --list` montre deux topics créés par Kafka Streams : `stats-service-stats-store-changelog` et `stats-service-stats-store-repartition`.
- **Changelog** : 3 partitions, réplication 1, `cleanup.policy=compact`. Une ligne par mise à jour, avec l'agrégat complet : ADH-CONNECT-02 une fois, ADH-CONNECT-01 deux fois (2/1/235.0 puis 3/1/335.0), l'ancienne valeur n'étant pas encore compactée.
- **Redémarrage** : après arrêt et relance, les logs montrent `Restored and ready to run` pour les tâches 1_0 et 1_2, puis `REBALANCING -> RUNNING`, et `GET /stats` renvoie des chiffres identiques à ceux d'avant l'arrêt.
- **Pendant le démarrage** : une requête faite avant `RUNNING` n'a rien renvoyé ; une requête faite après `RUNNING` a renvoyé HTTP 200.
- **Rejeu par remise à zéro des offsets** : service arrêté, puis `kafka-consumer-groups.sh --reset-offsets --to-earliest --execute` (groupe `stats-service`, topic `prelevements.resultats`), puis redémarrage. `GET /stats` renvoie ADH-CONNECT-01 = 6 acceptés, 2 rejetés, 670.0 et ADH-CONNECT-02 = 0 accepté, 2 rejetés, 400.0 (avant : 3/1/335.0 et 0/1/200.0). Les compteurs et les montants sont **doublés** ; `dernierEvenement` est inchangé (1795429828754 et 1795426655710).
- **Outil `kafka-streams-application-reset.sh`** (`--bootstrap-server localhost:19092 --application-id stats-service --input-topics prelevements.resultats`) : il refuse de s'exécuter tant que le service tourne (`IllegalStateException` : le groupe `stats-service` est toujours actif). Service arrêté, le dry-run annonce la remise à 0 des offsets de `prelevements.resultats` sur les 3 partitions et la suppression des topics internes `stats-service-stats-store-repartition` et `stats-service-stats-store-changelog`. Après exécution, la liste des topics ne contient plus aucun topic « stats ».
- **Dossier d'état local non supprimé par l'outil** : il est configuré dans `application.yml` (`state-dir: ${STATS_STATE_DIR:./target/state-stores}`), soit `stats-service/target/state-stores/stats-service`. Contenu observé : `0_0` à `0_2` vides ; `1_0` à `1_2` avec un dossier `rocksdb` et un `.checkpoint` ; un fichier `kafka-streams-process-metadata` daté du premier démarrage.
- **Après suppression manuelle de ce dossier et redémarrage**, `GET /stats` renvoie 3/1/335.0 et 0/1/200.0.
- **`processing.guarantee`** : le journal de démarrage du service (bloc `StreamsConfig values`) affiche `at_least_once`. C'est la valeur effective.
- **Rétention du topic source** : `retention.ms=604800000` (7 jours), `retention.bytes=-1`, `message.timestamp.type=CreateTime` (`kafka-configs.sh --describe`).
- **Sonde jetable (supprimée), bibliothèque `kafka-streams` 4.2.2 et `TopologyTestDriver`** :
  - P1 : `persistentWindowStore` refuse une rétention inférieure à la fenêtre (`The retention period of the window store p1 must be no smaller than its window size`) et accepte rétention = fenêtre, ou 7 j avec une fenêtre de 1 ms.
  - P2 : avec une rétention de 1 h et un temps du flux à 2 h, un `fetch` ne retrouve plus l'entrée posée à t=0 ; un `put` hors rétention ne lève pas d'exception ; un doublon à l'instant courant est retrouvé.
  - P3a : le changelog du `WindowStore` est lisible dans `TopologyTestDriver` (5 enregistrements pour 5 `put`, y compris celui hors rétention).
  - P3b : dans `TopologyTestDriver`, le changelog d'un agrégat `count` a une ligne par mise à jour (1, 2, 3).
- **Contrôle par mutation** (tests) : sans `processValues`, 4 tests passent au rouge (même `idEvenement` deux fois, rejeu d'un lot, rappel après la rétention, changelog) ; avec une déduplication par adhérent + type + montant, 3 tests passent au rouge (contenu identique, et deux tests existants).
- **Validation en direct de la déduplication** (broker 4.4.0) : après `kafka-streams-application-reset` et suppression de `stats-service/target/state-stores`, la nouvelle topologie démarre avec `stats.deduplication.retention = 7d` **sans erreur de conversion**, et `GET /stats` renvoie 3/1/335.0 (ADH-CONNECT-01) et 0/1/200.0 (ADH-CONNECT-02).
- **Topics internes constatés** : `stats-service-stats-store-repartition` (le nom prévu est confirmé), `stats-service-stats-store-changelog`, et `stats-service-stats-dedup-store-changelog` (nouveau).
- **Rejeu avec déduplication** : service arrêté, `kafka-consumer-groups.sh --reset-offsets --to-earliest --execute` sur `prelevements.resultats` (3 partitions à l'offset 0), redémarrage. Les chiffres restent 3/1/335.0 et 0/1/200.0 (avant la déduplication, le même geste donnait 6/2/670.0).
- **Offsets de fin après le rejeu** (`kafka-get-offsets.sh`, partitions p0/p1/p2) : `repartition` 2/0/8 (total 10 = 5 messages lus deux fois) ; `dedup-store-changelog` 1/0/4 (total 5, aucune écriture pour les doublons) ; `stats-store-changelog` 1/0/1 (total 2, une ligne par adhérent, aucune réécriture au rejeu). Les 4 messages d'ADH-CONNECT-01 sont dans la partition 2, le message d'ADH-CONNECT-02 dans la partition 0.
- **Piège** : `kafka-consumer-groups.sh --reset-offsets` échoue (« Assignments can only be reset if the group 'stats-service' is inactive, but the current state is Stable ») tant que le groupe a un membre actif ; ce membre avait 6 partitions (3 du topic source, 3 du repartitionnement). Il a fallu arrêter complètement le service. Après arrêt, aucun processus Java de `stats-service` ne restait (vérifié avec `Get-CimInstance Win32_Process`) et la remise à zéro a fonctionné.
- **Le store de déduplication survit à un redémarrage** : après l'arrêt et le redémarrage du rejeu, les doublons ont été écartés.

### Déduit

- Le topic `repartition` existe à cause du `selectKey` qui précède l'agrégation.
- Les 4 messages initiaux n'ont produit que 2 lignes de changelog, probablement à cause du cache de Kafka Streams, qui fusionne les mises à jour d'une même clé avant le commit.
- Une agrégation compteur/somme n'est pas idempotente : un rejeu additionne à nouveau. `dernierEvenement` est stable, ce qui suggère un maximum.
- La sous-topologie 0 est sans état ; la sous-topologie 1 porte le state store (d'après les dossiers `0_x` vides et `1_x` avec `rocksdb`).
- Les dossiers `rocksdb` datent du premier démarrage : la restauration constatée au redémarrage vient **probablement du disque local** et non du changelog.
- L'identifiant du client Streams (même valeur dans les logs avant et après redémarrage) est probablement conservé dans `kafka-streams-process-metadata`.
- La suppression manuelle du dossier d'état (dernière puce « Constaté ») a eu lieu **après** que l'outil de réinitialisation a supprimé le changelog et remis les offsets à 0 : l'état a donc été recalculé depuis le topic source, et non restauré depuis le changelog. Elle ne tranche pas l'origine de la restauration.
- Un `put` hors rétention est écrit dans le changelog du `WindowStore` (P3a) ; on ne sait pas s'il est conservé dans le store.
- `exactly_once_v2` ne remplacerait pas la déduplication : relire des messages (remise à zéro des offsets, republication) est une nouvelle lecture.
- L'identifiant du client Streams a changé (`8e4472a7-…` puis `e2682e39-…`) après la suppression du dossier d'état : c'est cohérent avec sa conservation dans `kafka-streams-process-metadata` (fichier non lu).
- Un membre resté dans le groupe un court moment après l'arrêt du service est probablement un membre qui n'a pas quitté proprement le groupe (non prouvé).

### Non vérifié

- Si la restauration vient du dossier d'état local ou du changelog.
- La cause exacte de l'absence de réponse pendant le démarrage (code HTTP non observé ; le contrôleur est censé répondre 503, non constaté).
- Le rôle exact du cache (`commit.interval.ms`, taille du cache) dans la fusion des écritures.
- Que le double comptage au rejeu se produirait aussi avec `exactly_once_v2`.
- Si le store de déduplication est restauré depuis le disque local ou depuis son changelog (`stats-service-stats-dedup-store-changelog`).
- Le cas hors rétention contre le broker réel (prouvé seulement par le test `unIdEvenementRejoueApresLaRetentionEstRecompte`).

## 9. Non vérifié, à faire

1. Origine de la restauration : **partiellement prouvé**. L'état est restauré, probablement depuis le disque local (déduit, §8 bis). Pour trancher : arrêter le service, supprimer `stats-service/target/state-stores/stats-service` **en gardant le changelog**, redémarrer et comparer `GET /stats`.
2. Validation en direct de la déduplication : **constatée** (§8 bis : un rejeu des offsets laisse 3/1/335.0 et 0/1/200.0, les topics internes et le nom du topic de repartitionnement sont constatés, la conversion de `7d` n'a pas posé de problème). Reste à vérifier : l'origine de la restauration du store de déduplication, et le cas hors rétention contre le broker réel.
3. `exactly_once_v2` : pour l'activer, `spring.kafka.streams.properties.processing.guarantee: exactly_once_v2` (broker 2.5 ou plus, doc Kafka 4.4 ; `commit.interval.ms` passerait à 100 par défaut). Question ouverte : les deux stores deviendraient atomiques (déduit). Cela ne remplace pas la déduplication.
