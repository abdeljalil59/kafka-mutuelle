# Amélioration DLT (Dead Letter Topic)

> **Mise à jour (20/11/2026)** : ce journal décrit l'état du commit `0042f62` et n'a pas été réécrit. Depuis :
> - **Le listener n'est plus en mode batch** : `prelevement-service` traite un message par appel (commit `8010336`). `BatchListenerFailedException`, le repli un par un et les notifications « par record en mode batch » (§ Tests d'intégration, tableau final) ne valent plus. Le biais du compteur de doublons après un lot en échec (§ Limites connues) ne se présente plus avec le listener actuel, qui n'écrit qu'une décision par appel ; **déduit** : une redélivrance après un échec survenu après le `MERGE` (l'envoi du résultat, par exemple) compte, elle, comme un doublon, puisque la ligne existe déjà.
> - **Le `DefaultErrorHandler` est remplacé par un `DefaultAfterRollbackProcessor`** (commit `3f78c42`, transactions Kafka) : mêmes paramètres `kafka.error-handler.*` (BackOff exponentiel, 5 livraisons), même `DeadLetterPublishingRecoverer`, même topic de rejets, avec `commitRecovered`. `CompteurDeLivraisons` se pose sur ce processeur. Les 5 livraisons puis le DLT sont aussi prouvés **sous transaction** (`ExactlyOnceIntegrationTest`, voir [transactions-exactement-une-fois.md](transactions-exactement-une-fois.md), §8).
> - **Les panneaux du DLT filtrent sur `name="kafkaTemplate"`** (commit `d751f53`) : le template des résultats (`resultatsKafkaTemplate`) a ses propres séries. Le dashboard compte désormais 5 lignes et 14 panneaux (ligne « Transactions »).
> - `ErreursKafkaConfigTest` compte 17 tests (14 ici) ; le dépôt en compte 212 au total ([README](../README.md)).

Environnement (19 et 20/11/2026, heure locale +01:00) : Spring Boot 4.1.2, spring-kafka 4.1.2, kafka-clients 4.2.2, broker `apache/kafka:4.4.0`, Prometheus v3.16.0, Grafana 13.3.1. **Aucune dépendance ni version n'a changé** (le commit ne touche aucun `pom.xml`) : [versions.md](versions.md) n'est donc pas modifié. Commit : `0042f62 feat: BackOff exponentiel et panneaux DLT` (7 fichiers, +426 −20).

Pour chaque point : ce qui est **constaté** (test, sortie réelle, source ou doc lue), **déduit** et **non vérifié**. Le build complet final (§7) est un **relevé de l'utilisateur**, lancé dans son terminal ; j'ai recoupé ses chiffres avec les rapports Surefire du dossier.

## Contexte
Le DLT existait depuis la phase 3 (commits `e3d40cc` et `6d0c28e`, journal [phase-3-message-invalide.md](phase-3-message-invalide.md)) : `DefaultErrorHandler` avec `DeadLetterPublishingRecoverer` vers `<topic>-dlt`, même partition, `PrelevementInvalideException` non réessayée. Son `BackOff` était celui par défaut, `FixedBackOff(0, 9)` : 0 ms de pause, 9 réessais, soit 10 livraisons sans pause (**constaté** : 10 appels du port, test `dix_livraisons_avant_dlt`). Cette amélioration remplace ce comportement. Le code le signalait déjà : « à revoir quand le traitement écrira en base ».

Problème du défaut (**déduit**) : 10 réessais sans pause s'épuisent en millisecondes, donc une coupure passagère d'Oracle enverrait au DLT tous les messages en vol ; et pour une erreur déterministe (ORA-12899, même ligne, même échec), ils ne servent à rien.

## Changements

### BackOff exponentiel
- `ExponentialBackOffWithMaxRetries` (classe de spring-kafka 4.1.2, citée dans la doc) : 4 réessais, pauses de 1 s, 2 s, 4 s puis 8 s, soit **5 livraisons et 15 s au plus** avant le DLT.
- Paramètres externalisés dans `application.yml`, lus par `@Value` sans valeur par défaut dans le code (convention du projet : une propriété absente fait échouer le démarrage) :
  - `kafka.error-handler.initial-interval: 1000` (millisecondes)
  - `kafka.error-handler.multiplier: 2.0`
  - `kafka.error-handler.max-retries: 4`
- Le préfixe `kafka.error-handler` est celui demandé ; les autres propriétés du service sont sous `prelevement.kafka.*`.
- La construction est isolée dans `ErreursKafkaConfig.backOffDeConsommation(...)` (pas un bean) pour que les tests lisent la suite des pauses sans contexte Spring.
- **Constaté (doc 4.1.2)** : le gestionnaire de pause par défaut « simply suspends the thread » (le thread du consommateur est bloqué) ; la pause réelle est plafonnée par ce qui reste de `max.poll.interval.ms`. `ExponentialBackOff` a un intervalle maximal par défaut de 30 s (**constaté** dans le jar) : il ne joue pas avec 1 s × 2 sur 4 réessais.

### Sérialisation du DLT (inchangée, rappel)
- `DelegatingByTypeSerializer` : `byte[]` → `ByteArraySerializer`, `CotisationEmise` → `KafkaAvroSerializer`, un seul `KafkaTemplate`. Elle date des phases 3 et 4 : **elle n'a pas été modifiée ici**.
- Elle permet de publier au DLT aussi bien les messages illisibles (octets d'origine, restaurés par le recoverer, doc) que les messages désérialisés mais en échec dans le listener. `KafkaTemplate<byte[], byte[]>` ou `<String, GenericRecord>` ne conviendraient pas : la valeur d'un échec du listener est la classe générée `CotisationEmise` (`specific.avro.reader: true`).

### Rétention du DLT
- `retention.ms=2592000000` (30 jours) posé sur le bean `NewTopic` du DLT. Le broker du compose garde 7 jours par défaut (`log.retention.hours=168`, **constaté**).
- **La rétention n'est posée qu'à la création du topic** : `spring.kafka.admin.modify-topic-configs` vaut `false` par défaut (**constaté** dans les métadonnées de Spring Boot). Le topic `cotisations.emises-dlt` déjà créé sur le broker garde donc 7 jours. Pour l'appliquer au topic existant (PowerShell ou cmd ; Git Bash réécrit le chemin `/opt/…`) :
  ```
  docker exec kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter --topic cotisations.emises-dlt --add-config retention.ms=2592000000
  ```
  Relecture : mêmes options avec `--describe` à la place de `--alter --add-config …`.
- **Constaté** : l'option `--topic` figure dans l'aide de `kafka-configs.sh` 4.4 et `--describe --topic` fonctionne sur ce broker. La commande `--alter` n'a **pas** été exécutée par moi : l'avoir appliquée au topic existant est **non vérifié** (à relire avec `--describe`).

### Panneaux Grafana
- Ligne 4 du dashboard, « Dead Letter Topic » (le dashboard compte désormais 4 lignes et 8 panneaux, `uid` et datasource inchangés) :
  - « Messages envoyés au DLT » : `increase(spring_kafka_template_seconds_count{job="prelevement-service",result="success"}[5m])`
  - « Échecs d'envoi au DLT » : même requête avec `result="failure"`, en rouge.
- **Les séries se distinguent par l'étiquette `result`** (`success` ou `failure`), pas par `exception` : l'étiquette `exception` existe aussi (`none` ou le nom de la classe), mais les requêtes ne l'utilisent pas.
- Pas de compteur custom : dans le code principal, seul le recoverer utilise ce `KafkaTemplate` (**constaté** par recherche dans le code). Réserve : `RetryDemoListener` (désactivé par défaut) référence le même bean `kafkaTemplate` ; activé, ses envois s'ajoutent à la série.
- **Constaté** : les 10 requêtes du dashboard passent `promtool check rules` (code 0) ; `spring_kafka_template_seconds_count{result="success"}` existe dans les captures réelles. **Non constaté** : la série `result="failure"`, qui n'existe qu'après un premier échec d'envoi (la doc spring-kafka décrit deux timers, succès et échec). Le panneau affichera « No data » tant qu'aucun envoi n'a échoué.

## Tests
### `ErreursKafkaConfigTest` : 14 tests, sans broker (**constaté**, verts)
Les 6 d'origine (octets intacts, sérialisation Avro, type non prévu, topic de rejets à 3 partitions, `PrelevementInvalideException` non réessayée, autre exception réessayée) et 8 ajoutés :
- les pauses par défaut valent 1000, 2000, 4000, 8000 ms puis le `BackOff` s'arrête ;
- les pauses suivent l'intervalle, le multiplicateur et le nombre de réessais configurés (10, 30) ;
- `application.yml` configure des pauses de 1, 2, 4 puis 8 s (le fichier livré, lu par `YamlPropertiesFactoryBean` : seul test qui lit les valeurs de production) ;
- le topic de rejets est déclaré avec `retention.ms=2592000000` ;
- 5 livraisons pour une erreur réessayable, puis un seul envoi au DLT ;
- 1 livraison pour une `PrelevementInvalideException` ;
- 1 livraison pour un message illisible ;
- notifications du `RetryListener` : `failedDelivery` 1 à 5 puis `recovered`.

### Tests d'intégration modifiés (`CompteurDeLivraisons`, **constaté**, verts)
`CompteurDeLivraisons` est un `RetryListener` de test (`ConcurrentHashMap`), posé sur le `DefaultErrorHandler` du contexte par `setRetryListeners` : aucun changement du code de production. Il compte par clé de message.
- `MessageInvalideIntegrationTest` (3 tests) : message illisible → 1 livraison puis 1 envoi au DLT ; montant négatif → 1 livraison puis 1 envoi ; **nouveau** : `retention.ms=2592000000` sur le topic créé par le service (broker embarqué).
- `PersistanceIntegrationTest` : `dix_livraisons_avant_dlt` devient `cinq_livraisons_avant_dlt` (échec Oracle ORA-12899, 65 caractères) : **5 appels du port et 5 `failedDelivery`**, un envoi au DLT. Les pauses y sont ramenées à 10 ms (`kafka.error-handler.initial-interval=10`) pour ne pas attendre 15 s par échec ; les vraies pauses sont vérifiées sur le `BackOff` (ci-dessus).
- **Constaté** : `failedDelivery` est notifié à **chaque** livraison échouée, y compris la dernière (la 5e), avant `recovered` : le nombre de livraisons est le nombre de `failedDelivery`, pas ce nombre plus un.
- **Constaté** : en mode batch, ce sont les notifications **par record** qui servent (celles de `FailedRecordTracker`). La variante par lot (`ConsumerRecords`) vient d'un autre chemin de spring-kafka (bytecode) ; le compteur l'enregistre sans l'asserter, sa valeur n'a pas été relevée.
- **Point fermé** : « une `PrelevementInvalideException` n'est pas réessayée en mode batch » était **non vérifié** ([phase-5-etape-2-merge-batch.md](phase-5-etape-2-merge-batch.md)). Il est maintenant constaté : 1 livraison, puis le DLT.

## Décisions prises
- **D1** : `ExponentialBackOff` plutôt que `FixedBackOff` : résilience aux pannes passagères (réseau, base), 15 s au plus avant le DLT.
- **D2** : pas de classification des erreurs SQL. Raison de l'analyse : la traduction par Spring des erreurs Oracle (coupure de connexion, etc.) n'est pas mesurée (**non vérifié**) ; couplage à Oracle en plus (argument de l'utilisateur). **Conséquence constatée** : une erreur déterministe comme ORA-12899 est maintenant réessayée 4 fois avec pauses, soit 15 s avant le DLT, au lieu de quelques millisecondes.
- **D3** : rétention 30 jours (temps d'analyse et de retraitement), à la création du topic **et** par `kafka-configs --alter` pour le topic existant. Le DLT garde des payloads avec des `idAdherent` plus longtemps : à garder en tête côté données personnelles.
- **D4** : deux panneaux Grafana, sur la série existante, sans compteur custom.
- **D5** : tests de livraisons avec `RetryListener` ; pas de test « DLT injoignable ».
- **D6** : niveau de log inchangé. Les 3 logs ERROR « Seek to current after exception » vus en phase 6 ([phase-6-metriques.md](phase-6-metriques.md), §7) subsistent ; `setLogLevel` existe (doc, défaut ERROR) si l'on veut les baisser.
- **D7** : `resetStateOnRecoveryFailure` laissé à son défaut. Doc : si la récupération échoue, « the BackOff will be reset by default and redeliveries will again go through the back offs before recovery is attempted again ».

## Découvertes et incidents
- **Partition inexistante au DLT (constaté)** : `DeadLetterPublishingRecoverer` vérifie la partition de destination. Avec un `KafkaTemplate` simulé sans partitions, il journalise `Destination resolver returned non-existent partition cotisations.emises-dlt-0, KafkaProducer will determine partition to use for this topic`. **Déduit** : avec un DLT auto-créé à 1 partition (`num.partitions=1`, `auto.create.topics.enable=true` sur le broker, constatés), le message ne serait pas perdu mais ne garderait pas la partition d'origine. Cela corrige mon analyse précédente, qui laissait entendre un échec d'envoi ; le bean `NewTopic` reste justifié (3 partitions, rétention).
- **Démarrage d'Oracle en test (constaté)** : lors de cette amélioration, trois builds complets lancés par moi ont échoué au démarrage du conteneur (17 erreurs, toutes `Timed out waiting for 'DATABASE IS READY TO USE!'` après les 10 minutes autorisées) ; le dernier a duré 2 h 52 min, le conteneur ayant démarré à 19:38 et l'échec étant consigné à 22:26 (**déduit** : machine figée ou en veille). Claude Code a ensuite arrêté la commande pour mémoire critique. Mesures prises juste après le premier échec : 512 Mo de RAM libre sur l'hôte ; ensuite, 7 Go de fichier d'échange en usage et 1,4 Go consommés par les conteneurs Docker sur 7,6 Go. **Cause du ralentissement non établie.** L'utilisateur a lancé le build dans son terminal : `BUILD SUCCESS` (§7).
- **Relevés préalables à l'implémentation (constaté sur le broker du compose)** : DLT à 3 partitions, RF 1, vide ; `log.retention.hours=168`, `log.cleanup.policy=delete`, aucune config dynamique sur le DLT.

## Limites connues
- **Pauses bloquantes** : chaque message en échec bloque le thread du consommateur jusqu'à 15 s. **Déduit** : plusieurs messages en échec dans un même lot s'enchaînent, N échecs coûtent environ N × 15 s ; les pauses sont plafonnées par `max.poll.interval.ms` (doc).
- **DLT injoignable : non testé** (D5). **Déduit** de la doc : le message n'est pas perdu, mais le cycle de réessais recommence et la partition reste bloquée tant que le DLT ne répond pas.
- **Compteur de doublons** : le biais documenté en [phase-6-metriques.md](phase-6-metriques.md), §9 (lot en échec rejoué ligne par ligne) est inchangé.
- **Pas de ventilation par cause** des messages envoyés au DLT : la série ne distingue pas illisible, invalide et échec de base.
- **Panneau d'échecs vide** tant qu'aucun envoi n'a échoué (voir plus haut).

## Ce qui est constaté, déduit, non vérifié
| | Constaté | Déduit | Non vérifié |
|---|---|---|---|
| BackOff | Suite 1000, 2000, 4000, 8000 ms puis arrêt ; valeurs de `application.yml` ; 5 livraisons (unitaire, et intégration par 5 appels du port + 5 `failedDelivery`) | 15 s bloquantes au plus par message | Pauses réelles de 1 à 8 s sur un vrai broker (les intégrations les réduisent à 10 ms) |
| Erreurs non réessayées | 1 livraison pour `PrelevementInvalideException` et pour un message illisible, y compris en mode batch | | Autres exceptions fatales |
| Rétention | `retention.ms` posé sur `NewTopic` et sur le topic créé par le service (broker embarqué) | | Application sur le topic existant du compose (commande non exécutée par moi) |
| Dashboard | Requêtes valides pour `promtool` ; série `result="success"` existe | | Affichage réel de la ligne 4 ; série `result="failure"` |
| `RetryListener` | `failedDelivery` à chaque livraison (y compris la dernière) ; notifications par record en mode batch | | Valeur des notifications par lot |
| Build | 157 tests verts : 16 + 55 + 86 (rapports Surefire), 7 min 55 s (relevé de l'utilisateur) | | Durée sur une autre machine (démarrage d'Oracle de 3 min 53 s à plus de 10 min) |
| DLT injoignable | | Partition bloquée, aucune perte | Tout le comportement |

## Journaux antérieurs devenus partiellement obsolètes
Non modifiés. Ils décrivent le `BackOff` par défaut (10 livraisons sans pause) ou des chiffres d'avant cette amélioration :
- [phase-3-message-invalide.md](phase-3-message-invalide.md) : §3 (« BackOff laissé au défaut » et tableau des sources, `FixedBackOff(0L, 9)`) et §7 (notes et limites : 10 livraisons).
- [phase-3-idempotence.md](phase-3-idempotence.md) : §5 (scénario 2), §6 (`dix_livraisons_avant_dlt`, `times(10)`), §7 (tableau) et §8 (note sur le `BackOff` par défaut).
- [phase-3-retry-topic.md](phase-3-retry-topic.md) : le `DefaultErrorHandler` comparé à `@RetryableTopic` y fait 10 livraisons sans pause.
- [phase-5-etape-2-merge-batch.md](phase-5-etape-2-merge-batch.md) : « 10 livraisons » dans la table des tests ; le point « non vérifié » sur les réessais en mode batch est désormais fermé.
- [phase-6-metriques.md](phase-6-metriques.md) : « 148 tests » (157 depuis cette amélioration) et « 6 panneaux en 3 lignes » (8 panneaux en 4 lignes ; le relevé du smoke test reste vrai pour le dashboard d'alors).
