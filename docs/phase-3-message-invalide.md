# Phase 3 : message illisible, topic de rejets (dead-letter)

Environnement (17/11/2026, heure locale +01:00) : Spring Boot 4.1.2, spring-kafka 4.1.2, kafka-clients 4.2.2 (versions : voir [versions.md](versions.md)), broker `apache/kafka:4.4.0` du compose. Consommateur : [phase-1-consommateur.md](phase-1-consommateur.md). Clés et rebalancing : [phase-2-rebalancing.md](phase-2-rebalancing.md).

Pour chaque point : ce qui est **constaté** (sortie réelle ou source lue), **déduit** et **non vérifié**. Les heures des logs Spring sont en heure de Paris (+01:00) ; celles de la console Kafka dans le conteneur sont en UTC.

## 1. Objectif de la phase
- Montrer qu'un message illisible bloque une partition quand le consommateur n'est pas protégé, puis le sortir de la route sans bloquer les messages suivants.
- Conserver le message illisible tel quel (octets d'origine) dans un topic de rejets, avec le contexte de l'échec (topic, groupe, exception).
- Prouver chaque point par un test d'intégration et une démonstration contre le broker du compose.
- **Hors périmètre de cette étape** : les réessais d'une exception levée par le listener (voir section 7), l'idempotence, le `@RetryableTopic`.

## 2. Le problème : un message illisible sans protection (poison pill)
Avant le topic de rejets, le consommateur utilisait `JacksonJsonDeserializer` directement en valeur et le `DefaultErrorHandler` par défaut. Test sur un topic jetable `cotisations.invalides` (1 partition) : offset 0 valide, offset 1 non JSON, offset 2 valide (relevé de la session du 17/11/2026 ; les logs bruts n'ont pas été conservés).
- **Constaté** : l'erreur de désérialisation est levée dans `poll()` (`RecordDeserializationException`), avant le listener. `DefaultErrorHandler.handleOtherException` la refuse : le message d'erreur recommande de configurer un `ErrorHandlingDeserializer` pour la valeur et/ou la clé.
- **Constaté** : le consommateur relit le même offset, l'erreur se répète (erreurs espacées d'environ 1 ms). Le message valide de l'offset 2 n'est jamais traité.
- **Constaté** : le groupe reste en apparence sain (le membre est listé, aucun rebalancing) ; seul le lag augmente.
- **Conséquence** : un seul message illisible bloque toute la partition derrière lui, sans plantage ni rebalancing.
- **Récupération manuelle (constatée)** : arrêt du consommateur, puis `kafka-consumer-groups … --reset-offsets --to-offset 2`, d'abord sans `--execute` (simulation) puis avec ; au redémarrage, le consommateur reprend à l'offset 2 et lit le message valide. L'outil affiche un avertissement : Kafka 5.0 exigera `--dry-run` ou `--execute` explicites. Le consommateur a été arrêté par `Ctrl+C` en copiant les erreurs.

## 3. La solution mise en place
Code relu tel qu'il est dans le dépôt (commit `e3d40cc`). `domain/` et `application/` ne sont pas touchés : tout est dans `config/` et `application.yml`.

### `application.yml`
```yaml
value-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
properties:
  spring.deserializer.value.delegate.class: org.springframework.kafka.support.serializer.JacksonJsonDeserializer
  spring.json.value.default.type: io.github.abdeljalil59.mutuelle.events.CotisationEmise
  spring.json.use.type.headers: false
```
La clé reste en `StringDeserializer` (non enveloppée). **Déduit** : un octet invalide dans une clé est remplacé, jamais refusé : `StringDeserializer.deserialize` fait `new String(bytes, charset)` (bytecode lu, aucun `throw` vu ; comportement JDK non relu).

### `config/ErreursKafkaConfig.java`
- **Topic de rejets** : `NewTopic` nommé `<topic source>-dlt` (même fonction `topicRejets` pour le topic et pour le routage), 3 partitions, 1 réplique. Le nombre de partitions est une constante : la doc spring-kafka exige au moins autant de partitions que le topic source.
- **Un seul `KafkaTemplate`** : un `DefaultKafkaProducerFactoryCustomizer` fixe le sérialiseur de valeur, un `DelegatingByTypeSerializer` : `byte[]` vers `ByteArraySerializer` (désérialisation ratée), `CotisationEmise` vers `JacksonJsonSerializer` (exception dans le listener). Un autre type provoque une `SerializationException` (testé).
- **Gestionnaire d'erreurs** : un `DefaultErrorHandler` avec un `DeadLetterPublishingRecoverer`, destination `new TopicPartition(<topic>-dlt, message.partition())` : même numéro de partition que l'original. BackOff laissé au défaut de `DefaultErrorHandler` (commentaire dans le code : à revoir quand le traitement écrira en base).
- **Raccordement** : Boot 4.1.2 récupère un unique bean `CommonErrorHandler` pour la fabrique de conteneurs (`ObjectProvider.getIfUnique`).

### Défauts vérifiés dans les jars
| Point | Source | Statut |
|---|---|---|
| Suffixe du topic de rejets | Bytecode de `DeadLetterPublishingRecoverer` (recette `\u0001-dlt`) ; doc `annotation-error-handling.html` (« `<originalTopic>-dlt` ») | **Constaté** : `-dlt` (et non `.DLT`) |
| Valeur publiée pour une `DeserializationException` | Bytecode : `DeserializationException.getData()` passé à `createProducerRecord` ; doc : « The original `byte[]` data … is restored as the record value » | **Constaté** : octets d'origine ; **vu par le test et la démonstration 1** |
| En-têtes ajoutés | `KafkaHeaders`, `DeadLetterRecordManager` (noms `kafka_dlt-*`) ; l'en-tête `springDeserializerExceptionValue` est retiré (`Headers.remove`) | **Constaté** (jar) ; en-têtes lus en console (démonstration 1) et testés (section 3, tests) |
| Classification par défaut | Bytecode : `ExceptionClassifier.defaultFatalExceptionsList` | **Constaté** : `DeserializationException`, `MessageConversionException`, `ConversionException`, `MethodArgumentResolutionException`, `NoSuchMethodException`, `ClassCastException` ne sont pas réessayées ; parcours des causes activé |
| BackOff par défaut | Bytecode et doc (`FixedBackOff(0L, 9)`) | **Constaté** (jar et doc) ; **exercé** : 10 livraisons confirmées par `times(10)` (voir [phase-3-idempotence.md](phase-3-idempotence.md), section 6) |
| Vérification de la partition de destination | Bytecode : `verifyPartition` vrai par défaut, délai 5 s ; partition absente : `warn` puis partition `-1` (Kafka choisit) | **Constaté** (jar) ; **non exercé** |
| Envoi du rejet | Bytecode : `waitForSendResultTimeout` 30 s, `failIfSendResultIsError` vrai | **Constaté** (jar) ; **déduit** : un DLT injoignable bloque de nouveau la partition |
| Offset du rejet commité | Doc : « The recovered record's offset is committed. » | Constaté en démonstration (section 4) |

### Tests (commit `c6cfe57`)
- **`ErreursKafkaConfigTest`** (4 tests, sans broker) : octets d'origine inchangés par le sérialiseur ; `CotisationEmise` sérialisé en JSON contenant l'`idEvenement` et `50.00` ; `SerializationException` pour un autre type ; topic `cotisations.emises-dlt` avec 3 partitions.
- **`MessageInvalideIntegrationTest`** (2 tests, `@EmbeddedKafka`, topic source seul déclaré, donc le DLT est créé par le service) :
  - **Garde-fou** : en `@BeforeEach`, vérifie que le contexte pointe vers le broker embarqué et non vers `localhost:9092`.
  - **Message illisible** : clé unique `adh-illisible` ; envoi de `ceci-n-est-pas-du-json` puis d'un JSON valide de même clé. Attente que la somme des offsets commités du groupe égale le nombre de messages envoyés, puis lecture complète du DLT. Constaté : exactement un rejet pour cette clé (le message valide n'y est pas), valeur égale aux octets d'origine, même partition que l'envoi, `kafka_dlt-original-topic` = `cotisations.emises`, `kafka_dlt-original-partition` lu en `int` et `kafka_dlt-original-offset` lu en `long`, égaux à la partition et à l'offset du message d'origine, `kafka_dlt-exception-fqcn` = `org.springframework.kafka.support.serializer.DeserializationException`, `springDeserializerExceptionValue` absent.
  - **Montant négatif** : clé `adh-negatif`, JSON bien formé avec `montant: -5` puis un valide : un rejet, octets d'origine, `kafka_dlt-exception-fqcn` = `DeserializationException`. Les en-têtes d'origine (partition, offset) ne sont pas assertés dans ce test.
  - **Lecture du DLT déterministe** : après l'attente des offsets du groupe, relève des offsets de fin du DLT puis lecture jusqu'à ces offsets. Le rejet étant publié avant le commit de son offset, les offsets de fin incluent tout rejet déjà publié : l'absence du message valide est donc un constat de lecture complète, pas d'une fenêtre de temps.
  - **Aucune lecture de logs** : ni dans le DLT, ni dans les offsets.
- **Dernier `verify`** (`-pl prelevement-service -am`) : `BUILD SUCCESS` ; `events-contract` 11 tests, `prelevement-service` 35 tests (29 d'avant plus 2 d'intégration et 4 unitaires), soit 46. `cotisation-service` (26 tests) n'était pas dans ce reactor et n'a pas été relancé.
- **Ordre aléatoire (phase `test`)** :

  | Graine | Contexte | Ordre réel (rapport Surefire) | Durée de la classe |
  |---|---|---|---|
  | (aucune) | `verify` | `illisible`, puis `negatif` | 8,587 s |
  | 946578346 | avant la lecture déterministe | `negatif`, puis `illisible` | 25,28 s |
  | 1800348260 | avant la lecture déterministe | `negatif`, puis `illisible` | 17,80 s |
  | (aucune) | après la lecture déterministe | `illisible`, puis `negatif` | 15,97 s |
  | 279441757 | après la lecture déterministe | `illisible`, puis `negatif` | 10,35 s |
  | 946578346 | après la lecture déterministe | `negatif`, puis `illisible` | 9,854 s |

  Les deux ordres ont été joués, verts, avant et après la modification. Les durées de la classe varient beaucoup (8,6 à 25,3 s) sans que la cause soit établie (voir section 7).

## 4. Démonstration 1 : un seul consommateur, topic `cotisations.demo`
Le topic n'a **pas** été créé avec 3 partitions comme prévu : il n'existait pas au démarrage du service, et il a été créé automatiquement.
- **Constaté** : `kafka-topics --describe --topic cotisations.demo` : `PartitionCount: 1, ReplicationFactor: 1`. Au démarrage (09:54:09.331, `WARN`) : `[Consumer clientId=consumer-demo-dlt-1, groupId=demo-dlt] The metadata response from the cluster reported a recoverable issue with correlation id 2 : {cotisations.demo=UNKNOWN_TOPIC_OR_PARTITION}`. À 09:54:09.480 : `Finished assignment for group at generation 1: {consumer-demo-dlt-1-19d65405-21f5-4133-a279-c9484ee7ff27=Assignment(partitions=[cotisations.demo-0])}`.
- **Constaté** : la `ConsumerConfig` loguée au démarrage (09:54:08.627) porte `allow.auto.create.topics = true`.
- **Déduit** : cette valeur est la cause de la création automatique du topic avec 1 partition (le lien de cause n'est pas observé). **Non vérifié** : `auto.create.topics.enable` côté broker.
- **Constaté** : le topic de rejets, créé par le service au démarrage : `kafka-topics --describe --topic cotisations.demo-dlt` : `PartitionCount: 3, ReplicationFactor: 1`, partitions 0, 1, 2, `Leader 1`, `Isr 1`. Le DLT (3 partitions) a donc plus de partitions que le topic source (1), ce que la règle (au moins autant) autorise.

Service lancé par `java -jar prelevement-service\target\prelevement-service-0.1.0-SNAPSHOT.jar '--prelevement.kafka.topic=cotisations.demo' '--spring.kafka.consumer.group-id=demo-dlt'`. Quatre messages envoyés avec la clé `adh3` par `kafka-console-producer` :

| Offset | Message |
|---|---|
| 0 | `{"idEvenement":"11111111-1111-1111-1111-111111111111","idAdherent":"adh3","montant":50.00,"emiseLe":"2026-11-17T08:00:00Z"}` (valide) |
| 1 | `ceci-n-est-pas-du-json` |
| 2 | `{"idEvenement":"22222222-2222-2222-2222-222222222222","idAdherent":"adh3","montant":-5,"emiseLe":"2026-11-17T08:00:01Z"}` |
| 3 | `{"idEvenement":"33333333-3333-3333-3333-333333333333","idAdherent":"adh3","montant":70.00,"emiseLe":"2026-11-17T08:00:02Z"}` (valide) |

Les offsets 0 et 3 sont constatés par les logs du listener ; les offsets 1 et 2 sont **déduits** de l'ordre d'envoi sur un topic neuf (instants d'envoi non relevés).

Logs de T2 (consommateur) :
```
09:54:48.047 CotisationEmiseListener : partition=0 offset=0 cle=adh3 idEvenement=11111111-1111-1111-1111-111111111111 montant=50.00 decision=ACCEPTE
09:54:55.322 (thread ntainer#0-0-C-1) ProducerConfig values : acks = -1, enable.idempotence = true, key.serializer = StringSerializer, value.serializer = org.springframework.kafka.support.serializer.DelegatingByTypeSerializer, client.id = producer-1
09:54:55.384 ProducerId set to 2008 with epoch 0
09:55:07.713 CotisationEmiseListener : partition=0 offset=3 cle=adh3 idEvenement=33333333-3333-3333-3333-333333333333 montant=70.00 decision=ACCEPTE
09:55:58.324 à 09:55:58.680 : arrêt propre par Ctrl+C (Revoke, LeaveGroup, « Consumer stopped »)
```
- **Constaté** : dans ce fichier de logs (09:54:09 à 09:55:58), aucune ligne `WARN` ni `ERROR` ; aucune ligne `INFO` qui mentionne le rejet lui-même ; la seule trace `INFO` est la création du producteur au premier rejet (09:54:55.322) ; les offsets 1 et 2 n'atteignent jamais le listener ; le message valide de l'offset 3 est traité après les rejets (pas de blocage).
- **Déduit (ordre des lignes)** : le producteur est créé au premier rejet, sur le thread du consommateur (`ntainer#0-0-C-1`) : le rejet est publié par le thread qui lit.
- **Constaté (après arrêt)** : `kafka-consumer-groups --describe --group demo-dlt` : `Consumer group 'demo-dlt' has no active members.` ; `cotisations.demo` partition 0 : `CURRENT-OFFSET 4`, `LOG-END-OFFSET 4`, `LAG 0`. Les offsets 1 et 2 ont été dépassés.

Contenu du DLT (`kafka-console-consumer`, `--from-beginning`, délai de 5 s, `print.key`, `print.partition`, `print.offset`) :
```
Partition:0  Offset:0  adh3  ceci-n-est-pas-du-json
Partition:0  Offset:1  adh3  {"idEvenement":"22222222-2222-2222-2222-222222222222","idAdherent":"adh3","montant":-5,"emiseLe":"2026-11-17T08:00:01Z"}
```
Le `TimeoutException` affiché par la console après les 2 messages est la fin normale du délai (`--timeout-ms 5000`), pas une erreur du service. **Constaté** : valeurs, clé et partition conservées ; les offsets du DLT (0 et 1) ne sont pas ceux d'origine (1 et 2).

En-têtes, lus avec `print.headers=true` :
- **Message de l'offset 0 du DLT (illisible)** : `kafka_dlt-exception-fqcn` et `kafka_dlt-exception-cause-fqcn` = `org.springframework.kafka.support.serializer.DeserializationException` ; `kafka_dlt-exception-message` = `failed to deserialize` ; `kafka_dlt-original-topic` = `cotisations.demo` ; `kafka_dlt-original-timestamp-type` = `CreateTime` ; `kafka_dlt-original-consumer-group` = `demo-dlt`. Trace (`kafka_dlt-exception-stacktrace`), premières lignes : `SerializationUtils.deserializationException(SerializationUtils.java:164)`, `ErrorHandlingDeserializer.deserialize(ErrorHandlingDeserializer.java:220)`, `Deserializer.deserialize(Deserializer.java:114)`, `CompletedFetch.parseRecord(CompletedFetch.java:324)`, …, `ClassicKafkaConsumer.poll(ClassicKafkaConsumer.java:645)` : la désérialisation a lieu pendant `poll()`, avant le listener. Causes : `SerializationException: Can't deserialize data  from topic [cotisations.demo]`, puis `tools.jackson.core.exc.StreamReadException: Unrecognized token 'ceci': was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false') at [Source: UNKNOWN; byte offset: #0]`.
- **Message de l'offset 1 du DLT (montant -5)** : mêmes `exception-fqcn`, `exception-cause-fqcn`, `exception-message`, `original-topic`, `original-timestamp-type` et `original-consumer-group`. Causes : `SerializationException: Can't deserialize data  from topic [cotisations.demo]`, puis `tools.jackson.databind.exc.ValueInstantiationException: Cannot construct instance of 'io.github.abdeljalil59.mutuelle.events.CotisationEmise', problem: montant doit être strictement positif at [Source: UNKNOWN; byte offset: #119]`, puis `java.lang.IllegalArgumentException: montant doit être strictement positif`. Le texte du message est celui de `CotisationEmise.java` ; la console PowerShell l'affiche avec `├¬` au lieu de `ê` (artefact d'affichage : **déduit**, UTF-8 lu avec une page de code OEM).
- **Constaté (les deux messages)** : `exception-fqcn` et `exception-cause-fqcn` sont identiques, donc inutilisables pour distinguer un JSON malformé d'une règle violée ; seules les lignes `Caused by` de la trace le permettent. Le rejet du montant négatif vient du constructeur de `CotisationEmise` (`IllegalArgumentException`), **pas** de `PrelevementInvalideException`.
- **Non lisibles en console** (valeurs binaires, **déduit**) : `kafka_dlt-original-partition`, `kafka_dlt-original-offset`, `kafka_dlt-original-timestamp` s'affichent vides ou en caractères illisibles. Les offsets d'origine ne sont donc **pas** constatés dans cette démonstration. Ce que le test d'intégration établit : sur le broker embarqué, `original-partition` (`int`) et `original-offset` (`long`) égalent la partition et l'offset renvoyés à l'envoi (section 3, tests).

## 5. Démonstration 2 : topic à 3 partitions, la partition d'origine est conservée (17/11/2026, 10:01)
Topic `cotisations.demo2` créé **avant** le service, avec `--partitions 3 --replication-factor 1` (`kafka-topics --describe` : `PartitionCount: 3`, partitions 0, 1, 2). Service lancé avec `'--prelevement.kafka.topic=cotisations.demo2' '--spring.kafka.consumer.group-id=demo-dlt2'`.
```
10:01:28.771 Finished assignment for group at generation 1: {consumer-demo-dlt2-1-e7046326-473b-44f6-a6be-dada8ac10872=Assignment(partitions=[cotisations.demo2-0, cotisations.demo2-1, cotisations.demo2-2])}
10:01:28.933 demo-dlt2: partitions assigned: [cotisations.demo2-0, cotisations.demo2-1, cotisations.demo2-2]
10:01:42.024 ProducerConfig values (producteur créé au premier rejet, value.serializer = DelegatingByTypeSerializer)
10:01:42.085 ProducerId set to 2012 with epoch 0
```
Le fichier de logs relevé s'arrête là ; la suite des logs du service n'a pas été relevée.

Trois messages illisibles (valeur non JSON) : clé `adh1` « illisible-pour-adh1 », clé `adh3` « illisible-pour-adh3 », clé `adh5` « illisible-pour-adh5 ». Contenu de `cotisations.demo2-dlt` (`--from-beginning --max-messages 3`, `print.key`, `print.partition`, `print.offset`) :
```
Partition:0  Offset:0  adh3  illisible-pour-adh3
Partition:1  Offset:0  adh5  illisible-pour-adh5
Partition:2  Offset:0  adh1  illisible-pour-adh1
```
Relecture du topic source (`kafka-console-consumer` sur `cotisations.demo2`, `--from-beginning --max-messages 3`, `print.key`, `print.partition`, `print.offset`) :
```
Partition:1  Offset:0  adh5  illisible-pour-adh5
Partition:2  Offset:0  adh1  illisible-pour-adh1
Partition:0  Offset:0  adh3  illisible-pour-adh3
```
(L'ordre d'affichage des partitions varie d'une lecture à l'autre, sans importance.)
- **Constaté** : sur le topic source, `adh5` est en partition 1, `adh1` en partition 2, `adh3` en partition 0 ; dans le DLT, les mêmes clés sont dans les mêmes partitions, avec un offset 0. Les trois rejets sont dans trois partitions différentes du DLT, la clé est conservée.
- **Recoupement** : sur un topic de 3 partitions, la phase 2 avait constaté `adh1` en partition 2, `adh3` en 0 et `adh5` en 1 ([phase-2-rebalancing.md](phase-2-rebalancing.md), section 3, C) ; un calcul Python indépendant du projet (murmur2, modulo 3) donne les mêmes partitions.
- **Limite (constaté sur le code, déduit sur l'équivalence)** : le DLT a aussi 3 partitions et la clé est conservée ; le partitionneur par défaut aurait donné le même résultat. Cette démonstration ne distingue pas l'affectation explicite de notre résolveur (`message.partition()`) d'un simple hachage de la clé. Le test d'intégration établit l'égalité des partitions sur un message, sans lever cette ambiguïté non plus. **Non exercé** : un message sans clé.

## 6. Ce qui est constaté, déduit, non vérifié
| | Constaté | Déduit | Non vérifié |
|---|---|---|---|
| Message illisible sans protection | Erreur levée dans `poll()` ; le même offset est relu ; le valide de l'offset 2 jamais traité ; membre listé, lag qui monte (section 2) | Le blocage vaut pour toute la partition derrière | Le nombre exact de répétitions (logs bruts non conservés) |
| Rejet avec protection | Un message illisible et un montant négatif vont au DLT avec leurs octets d'origine ; le message valide suivant est traité ; les offsets sont dépassés (test d'intégration, démonstration 1) | Offsets d'origine 1 et 2 en démonstration 1 (ordre d'envoi) | Les offsets d'origine en console (binaires) |
| En-têtes `kafka_dlt-*` | `exception-fqcn`, `cause-fqcn`, `message`, `original-topic`, `timestamp-type`, `consumer-group` lus en console ; `original-partition` (`int`) et `original-offset` (`long`) assertés par le test | Illisibilité des binaires en console | `original-timestamp` décodé |
| `PrelevementInvalideException` | Le montant négatif échoue dans le constructeur de `CotisationEmise` (`IllegalArgumentException`) et arrive au DLT en `DeserializationException` | `idAdherent` vide et champs null suivent le même chemin (mêmes règles) | Tout ce qui n'est pas le montant négatif |
| Partition d'origine conservée | Test d'intégration : même partition que l'envoi ; démonstration 2 : la partition d'origine des trois messages (relecture du topic source) est la même que celle du rejet dans le DLT, avec les trois partitions concernées | — | Distinction résolveur explicite / partitionneur par défaut ; message sans clé |
| Création du DLT | `cotisations.demo-dlt` créé par le service (3 partitions) | | Démarrage avec broker arrêté |
| Création du topic source | Topic créé avec 1 partition (démonstration 1) ; `allow.auto.create.topics = true` dans la `ConsumerConfig` loguée à 09:54:08.627 | Le lien de cause entre cette valeur et la création du topic | `auto.create.topics.enable` côté broker |

## 7. Notes et limites
- **BackOff** : `DefaultErrorHandler` garde son BackOff par défaut `FixedBackOff(0, 9)` : 10 livraisons, sans pause, puis le DLT, pour une exception levée par le listener. **À revoir quand une écriture en base existera.** Ce chemin (exception du listener, 10 livraisons, `CotisationEmise` publié au DLT en JSON) est maintenant **constaté** : `PersistanceIntegrationTest` (commit `eed89c6`, tests `dix_livraisons_avant_dlt` et `echec_base_puis_message_valide`), voir [phase-3-idempotence.md](phase-3-idempotence.md), section 6. Il n'était pas exercé dans les démonstrations de ce journal.
- **Non exercés** : démarrage avec le broker arrêté (comportement du `KafkaAdmin`), DLT avec moins de partitions que le topic source, DLT injoignable, format JSON d'`Instant` et de `BigDecimal` à la sérialisation, message sans clé.
- **Lenteur non expliquée** : `CotisationEmiseListenerIntegrationTest` a pris 26,01 s lors du dernier `mvn verify`, contre environ 7,9 s auparavant ; non investigué. La durée de `MessageInvalideIntegrationTest` varie aussi (8,6 à 25,3 s, section 3).
- **Avertissements de dépréciation des outils console** : `--property` est déprécié (utiliser `--reader-property` côté producteur et `--formatter-property` côté consommateur) ; `kafka-consumer-groups --reset-offsets` annonce que Kafka 5.0 exigera `--dry-run` ou `--execute` explicites.
- **Objets Kafka jetables créés** pour ces démonstrations (sur le broker du compose) : topics `cotisations.invalides`, `cotisations.demo`, `cotisations.demo-dlt`, `cotisations.demo2`, `cotisations.demo2-dlt` ; groupes `test-invalide`, `demo-dlt`, `demo-dlt2`. Destinés à être supprimés (hors de ce journal).
