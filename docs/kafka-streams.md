# Kafka Streams : `stats-service`

Objectif : agréger en temps réel les résultats de prélèvement **par adhérent** (nombre d'acceptés, de rejetés, montant total, dernier événement) et exposer le résultat par une API REST, sans base de données : l'état vit dans un **State Store** local, sauvegardé dans Kafka.

> **Statut.** La logique d'agrégation est prouvée par 25 tests sans broker (`TopologyTestDriver`, registre Avro factice `mock://`). Le démarrage réel contre le broker 4.4.0 et le registre a été **vérifié à la main** (topics internes créés, `GET /stats` sur un store alimenté, restauration après redémarrage) : voir §8 bis. **Non vérifié** : l'origine de la restauration (disque local ou changelog), le comportement exact des requêtes pendant le démarrage, le rôle du cache. Voir §9.

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
       .groupByKey(Grouped.with(Serdes.String(), resultatSerde))
       .aggregate(StatsAdherent::vide,                                         // valeur initiale
                  (idAdherent, resultat, stats) -> ajouter(stats, resultat),   // agrégateur
                  Named.as("stats-par-adherent"),
                  Materialized.<String, StatsAdherent, KeyValueStore<Bytes, byte[]>>as("stats-store")
                          .withKeySerde(Serdes.String())
                          .withValueSerde(statsSerde));                        // KTable matérialisée
```

- **`selectKey` et repartitionnement.** La clé du topic est déjà `idAdherent`, mais Kafka Streams ne peut pas le deviner : `selectKey` marque la clé comme modifiée, et le `groupByKey` suivant fait passer les messages par un **topic de repartitionnement** interne, pour garantir que tous les messages d'un adhérent arrivent à la même tâche. C'est un coût réel (un topic et un aller-retour de plus), ici accepté pour la démonstration. Sans `selectKey`, `groupByKey` n'en crée pas. Prouvé par `leChangementDeClePasseParUnTopicDeRepartitionnement`.
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

- **Au moins une fois** (garantie par défaut de Streams, `processing.guarantee` non modifié) : après un crash, un message peut être retraité et **compté deux fois**. L'exactly-once (`exactly_once_v2`) corrigerait cela mais n'a pas été essayé ici.
- **`read_committed`** (`consumer.isolation.level` dans `application.yml`) : `prelevement-service` publie dans une transaction Kafka ; sans ce réglage, un résultat d'une transaction annulée serait compté. Prouvé : `StreamsConfigTest` montre que Streams transmet le réglage au consommateur ; l'effet contre le vrai broker n'est pas constaté.
- **Pas de dédoublonnage** par `idEvenement` : un résultat rejoué (par exemple `prelevement-service` qui republie après rejeu) est compté à nouveau. À comparer avec `impaye-service`, qui dédoublonne en base.
- **Messages antérieurs au démarrage** (`auto.offset.reset`) : **constaté : les messages antérieurs ont bien été agrégés** (les 4 messages déjà présents sur `prelevements.resultats` au premier démarrage, voir §8 bis). Le réglage n'est pas posé dans `application.yml` et la valeur par défaut de Streams n'a pas été lue dans la documentation : seul le comportement est constaté. Changer l'`application-id` repart d'un état vide.

## 6. Architecture du module

```
domain/                   StatsAdherent (record immuable, agrégation pure)
application/              ConsulterStats (cas d'usage), port out LectureStats, StatsIndisponibleException
adapter/in/kafka/         StatsTopology, StatsAdherentSerde
adapter/in/rest/          StatsController, StatsReponse (DTO distinct du domaine)
adapter/out/streams/      LectureStatsStateStore (Interactive Queries)
config/                   StatsConfig (câblage Spring, Serde Avro)
```

La topology est classée en `adapter/in` : elle consomme un topic, comme un listener Kafka. Elle n'est pas dans `domain` car elle importe Kafka Streams.

## 7. Configuration

`stats-service/src/main/resources/application.yml` : port **8086** (`STATS_SERVER_PORT`), `application-id: stats-service`, brokers `localhost:9092`, registre `http://localhost:8082`, répertoire des stores `STATS_STATE_DIR`. L'`application-id` sert aussi de groupe de consommation et de préfixe des topics internes.

## 8. Tests (25)

| Fichier | Tests | Ce qui est prouvé |
|---|---|---|
| `StatsTopologyTest` | 8 | comptage acceptés/rejetés, montant, indépendance des adhérents, re-keying, dernier événement, repartitionnement, store nommé (`TopologyTestDriver`, sans broker) |
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

### Déduit

- Le topic `repartition` existe à cause du `selectKey` qui précède l'agrégation.
- Les 4 messages initiaux n'ont produit que 2 lignes de changelog, probablement à cause du cache de Kafka Streams, qui fusionne les mises à jour d'une même clé avant le commit.

### Non vérifié

- Si la restauration vient du dossier d'état local ou du changelog.
- La cause exacte de l'absence de réponse pendant le démarrage (code HTTP non observé ; le contrôleur est censé répondre 503, non constaté).
- Le rôle exact du cache (`commit.interval.ms`, taille du cache) dans la fusion des écritures.

## 9. Non vérifié, à faire

1. Restauration après redémarrage : **partiellement prouvé** : l'état est restauré, mais l'origine (disque local ou changelog) reste à vérifier.
2. Prouver le double comptage au rejeu, puis tester `exactly_once_v2`.
