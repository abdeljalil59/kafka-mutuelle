# Phase 1 : consommateur Kafka (`prelevement-service`)

Environnement (16/11/2026, heure locale +01:00) : Spring Boot 4.1.2, spring-kafka 4.1.2, kafka-clients 4.2.2 (versions : voir [versions.md](versions.md)), broker `apache/kafka:4.4.0` du compose. Producteur : [phase-1-producteur.md](phase-1-producteur.md).

Pour chaque point : ce qui est **constaté** (sortie réelle ou source citée), **déduit** et **non vérifié**.

## 1. Conception et décisions
- **Périmètre de cette étape** : le consommateur seul. Il lit `cotisations.emises`, applique une règle, journalise la décision. Il ne produit pas sur `prelevements.resultats` (le contrat `PrelevementResultat` viendra à l'étape suivante).
- **Règle déterministe** : prélèvement rejeté si `montant` est **strictement supérieur** au plafond ; plafond configurable (`prelevement.plafond`, 100.00 dans `application.yml`, pas de valeur par défaut dans le code). Limites testées : 100.00 accepté, 100.01 rejeté. La comparaison utilise `compareTo` : l'échelle du montant est ignorée (`100`, `100.0`, `100.00` sont égaux).
- **Architecture hexagonale légère** (même schéma que `cotisation-service`) : `domain/` (`DemandePrelevement`, `Decision`, `PlafondPrelevement`, `PrelevementInvalideException`), `application/` (`TraiterDemandePrelevement`), `adapter/in/kafka/` (`CotisationEmiseListener`), `config/` (`PrelevementConfig`). Le domaine n'importe jamais `events-contract` : l'adaptateur convertit `CotisationEmise` en `DemandePrelevement` (seuls `idAdherent` et `montant` passent). Pas de port de sortie à cette étape.
- **Deux types d'erreur** : `PrelevementInvalideException` pour une demande invalide (règle du domaine, comme `CotisationInvalideException`) ; `IllegalArgumentException` pour un plafond null ou non strictement positif (erreur de configuration : l'application échoue au démarrage).
- **Paramètres** (`application.yml`) : `group-id: prelevement-service`, `auto-offset-reset: earliest` (démonstration), `concurrency: 1`, clés en `StringDeserializer`, valeurs en `JacksonJsonDeserializer`, type par défaut `CotisationEmise` (`spring.json.value.default.type`), `spring.json.use.type.headers: false` explicite.
- **Pas de starter web** : le processus ne sert aucun port.
- **Journalisation** (adaptateur seulement, le domaine n'en sait rien) : `partition=… offset=… cle=… idEvenement=… montant=… decision=…`.
- **Dette connue** : tant qu'il n'y a pas de port de sortie, les tests d'intégration lisent la décision dans les logs ; ils s'appuieront sur le message produit quand `PrelevementResultat` existera.
- **Commits** : module et règle du plafond (`8293aca`), écouteur Kafka (`48d0ac0`), test d'intégration (`95092d6`), puis ce journal.

## 2. Défauts vérifiés
| Point | Source | Statut |
|---|---|---|
| `enable.auto.commit` | Documentation ([message-listener-container](https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/message-listener-container.html), verbatim : « the framework sets `enable.auto.commit` to `false` unless explicitly set ») ; bytecode `ListenerConsumer.determineAutoCommit` | **Constaté** : `enable.auto.commit = false` dans la liste `ConsumerConfig` de la démonstration |
| `AckMode` par défaut | Documentation (« The default `AckMode` is `BATCH` ») ; bytecode `ContainerProperties` (`AckMode.BATCH`) | Constaté dans le bytecode et la documentation ; **non journalisé à l'exécution** |
| `group.id` | `spring.kafka.consumer.group-id` | **Constaté** : `group.id = prelevement-service` dans `ConsumerConfig` et dans `kafka-consumer-groups --describe` |
| `@KafkaListener` activé par Boot | Jar `spring-boot-kafka` : `KafkaAnnotationDrivenConfiguration$EnableKafkaConfiguration` conditionnée par `EnableKafka` | **Constaté (effet)** : l'écouteur est attaché et traite les messages |
| Désérialiseur | Jar spring-kafka : constantes `spring.json.value.default.type`, `spring.json.use.type.headers`, `spring.json.trusted.packages` ; `JacksonMapperUtils.enhancedJsonMapper()` désactive `FAIL_ON_UNKNOWN_PROPERTIES` | **Constaté (effet)** : les messages sans en-tête sont lus en `CotisationEmise`, `montant` avec son échelle, `emiseLe` avec 9 et 6 chiffres de fraction |
| `spring.json.trusted.packages` | Bytecode : `isTrustedPackage` n'est appelé que quand l'identifiant de type vient d'un en-tête | **Constaté (effet)** : non configuré, la désérialisation réussit. Le mécanisme n'est pas lu en détail |
| Exception du listener | Documentation ([annotation-error-handling](https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html), verbatim : le conteneur commite les offsets en attente avant d'appeler le gestionnaire d'erreurs ; « after ten failures, the failed record is logged (at the `ERROR` level) ») ; bytecode : `new DefaultErrorHandler()` dans `ListenerConsumer`, `SeekUtils.DEFAULT_BACK_OFF` = `FixedBackOff(0L, 9L)`, `DEFAULT_MAX_FAILURES` = 10 | **Non exercé.** Déduit : 10 livraisons puis journalisation et saut. Offsets des records suivants après le saut : non vérifié |
| Configuration du client (liste `ConsumerConfig`, démonstration) | Log | **Constaté** : `auto.offset.reset = earliest`, `group.protocol = classic`, `partition.assignment.strategy = [RangeAssignor, CooperativeStickyAssignor]`, `session.timeout.ms = 45000`, `heartbeat.interval.ms = 3000`, `max.poll.interval.ms = 300000`, `max.poll.records = 500`, `isolation.level = read_uncommitted` |

## 3. Tests unitaires (sans Kafka)
- **Domaine** : `DemandePrelevementTest` (9), `PlafondPrelevementTest` (11), cas d'usage `TraiterDemandePrelevementTest` (3), soit 23 tests, sans Mockito (pas de port à simuler).
  - `PlafondPrelevementTest` : limite incluse (100.00 accepté), 100.01 rejeté, échelle ignorée (`100`, `100.0`, `100.000`), `100.001` rejeté, plafond null ou non strictement positif refusé.
- **Adaptateur** : `CotisationEmiseListenerTest` (3), avec un `ListAppender` de logback : format du journal pour une décision acceptée, pour une décision rejetée au-delà du plafond, et conservation de l'échelle du montant (`montant=50.00`).
- Décompte du module `prelevement-service` : **29** tests avec les 3 d'intégration (11 pour `events-contract`, 26 pour `cotisation-service`, soit 66 au total). `mvn -B verify` : `BUILD SUCCESS` ; le `repackage` de `prelevement-service` s'exécute avec `PrelevementServiceApplication` (« Replacing main artifact … with repackaged archive »).

## 4. Test d'intégration (`@EmbeddedKafka`)
- **Test** : `CotisationEmiseListenerIntegrationTest` (3 tests, `@SpringBootTest` + `@EmbeddedKafka(topics = "cotisations.emises", partitions = 3)`). Le contexte pointe vers le broker embarqué par `spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}` (nom constaté dans le jar : `EmbeddedKafkaBroker.SPRING_EMBEDDED_KAFKA_BROKERS`). Un garde-fou en `@BeforeEach` vérifie que le contexte n'utilise pas `localhost:9092`.
- **Constaté (verify normal)** : `Tests run: 3, Failures: 0` ; environ 7,9 s ; ordre d'exécution (rapport Surefire) : `commite_les_offsets_du_groupe`, `applique_la_regle_a_la_limite_du_plafond`, `consomme_le_json_reel_du_contrat`.
- **Ce qu'ils prouvent** : `consomme_le_json_reel_du_contrat` (JSON brut sans en-tête, `montant` 50.00, `emiseLe` à 9 chiffres de fraction, décision journalisée avec l'`idEvenement` envoyé) ; `applique_la_regle_a_la_limite_du_plafond` (100.00 donne `ACCEPTE`, 100.01 donne `REJETE`) ; `commite_les_offsets_du_groupe` (la somme des offsets commités du groupe est égale au nombre de messages envoyés par la classe, lue par `Admin.listConsumerGroupOffsets`).
- **Indépendance entre tests** : clés uniques, recherche des lignes de journal par `cle=<clé> `, liste de journal thread-safe (`CopyOnWriteArrayList`), compteur statique de messages envoyés pour le test de commit.
- **Ordre aléatoire (constaté)** : trois exécutions avec `-Djunit.jupiter.testmethod.order.default=org.junit.jupiter.api.MethodOrderer$Random` et une graine explicite (tirage au hasard de ma part : JUnit n'en journalise aucune dans la sortie Maven). Le rapport Surefire contient les deux propriétés dans la JVM de test. Résultats : 3 tests verts à chaque fois.

  | Graine | Ordre constaté (rapport Surefire) |
  |---|---|
  | 1061860978 | `applique…`, `commite…`, `consomme…` |
  | 988428312 | `consomme…`, `applique…`, `commite…` |
  | 703584960 | `consomme…`, `commite…`, `applique…` |

  Avec le défaut, quatre ordres sur six ont été joués ; le test de commit est passé en premier, au milieu et en dernier. **Déduit** : l'indépendance à l'ordre ; deux ordres ne sont pas joués. La commande demandée (`-pl prelevement-service` sans `-am`) échoue : `events-contract` n'est jamais installé dans `~/.m2` (`Could not find artifact …events-contract:jar:0.1.0-SNAPSHOT`) ; j'ai utilisé `-pl prelevement-service -am` avec `-Dsurefire.failIfNoSpecifiedTests=false`.
- **Ce que ces tests ne prouvent pas** : `auto.offset.reset = earliest` (voir la démonstration, section 5), le comportement face à un message illisible, la reprise après arrêt, le mode de commit lui-même, la compatibilité avec le broker 4.4.0 (le broker embarqué est en 4.2.2).

## 5. Démonstration contre le broker du compose
- **État de départ** (relevé de l'utilisateur) : `kafka` (`apache/kafka:4.4.0`) `Up 3 hours`, `kafka-ui` (`ghcr.io/kafbat/kafka-ui:v1.5.0`) `Up 2 days`, port 8081 libre. Topic `cotisations.emises` : 2 messages (partition 2, offsets 0 et 1, clé `adh1`, `montant` 50.00, champ `emiseLe`). `kafka-consumer-groups --list` : seulement `console-consumer-35135` (groupe inventé par l'outil CLI à chaque lancement) ; `prelevement-service` n'existe pas.
- **Premier démarrage de `prelevement-service`** (PID 35172, lignes brutes, 14:15) :
  ```
  Kafka version: 4.2.2
  14:15:44.330 ClassicKafkaConsumer : Subscribed to topic(s): cotisations.emises
  14:15:44.341 Started PrelevementServiceApplication in 1.925 seconds (process running for 2.472)
  14:15:44.684 Metadata : Cluster ID: 4L6g3nShT-eMCtK--X86sw
  14:15:44.688 ConsumerCoordinator : Discovered group coordinator localhost:9092 (id: 2147483646 rack: null isFenced: false)
  14:15:44.715 Successfully joined group with generation Generation{generationId=1, memberId='consumer-prelevement-service-1-3dbbadbc-…', protocol='range'}
  14:15:44.720 Finished assignment for group at generation 1: {consumer-prelevement-service-1-3dbbadbc-…=Assignment(partitions=[cotisations.emises-0, cotisations.emises-1, cotisations.emises-2])}
  14:15:44.731 Adding newly assigned partitions: [cotisations.emises-0, cotisations.emises-1, cotisations.emises-2]
  14:15:44.738/.739 Found no committed offset for partition cotisations.emises-2 / -1 / -0
  14:15:44.752/.753 Resetting offset for partition cotisations.emises-2 / -1 / -0 to position FetchPosition{offset=0, …}
  14:15:44.754 prelevement-service: partitions assigned: [cotisations.emises-0, cotisations.emises-1, cotisations.emises-2]
  14:15:44.810 CotisationEmiseListener : partition=2 offset=0 cle=adh1 idEvenement=d1c2d30a-fc4b-43c5-bf04-5a630fcb6f97 montant=50.00 decision=ACCEPTE
  14:15:44.811 CotisationEmiseListener : partition=2 offset=1 cle=adh1 idEvenement=20e78821-f1c4-473e-b1aa-36d13f8ef394 montant=50.00 decision=ACCEPTE
  ```
  - **Constaté** : le client 4.2.2 rejoint le groupe contre le broker 4.4.0 (protocole `classic`, stratégie `range`), reçoit les trois partitions, ne trouve aucun offset commité, repart de l'offset 0 (`earliest`) et lit les deux messages existants (mêmes `idEvenement` que ceux relus au broker à la section 8 de `phase-1-producteur.md`). Aucune ligne `Tomcat` : le processus est resté vivant (membre actif au `describe`, état `STABLE` dans Kafka UI).
  - **Déduit** : `id: 2147483646` du coordinateur correspond au nœud 1 (`Integer.MAX_VALUE - 1`).
- **Premier `describe`** (consommateur actif ; `CONSUMER-ID consumer-prelevement-service-1-3dbbadbc-…`, `HOST /172.20.0.1`, `CLIENT-ID consumer-prelevement-service-1`) :
  ```
  GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG
  prelevement-service cotisations.emises 0 - 0 -
  prelevement-service cotisations.emises 1 - 0 -
  prelevement-service cotisations.emises 2 2 2 0
  ```
  Les partitions 0 et 1, jamais traitées, n'ont **pas d'offset commité** (`-`). **Déduit** : `HOST /172.20.0.1` est l'adresse de la passerelle du réseau Docker.
- **Kafka UI v1.5.0** (relevé de l'utilisateur sur captures) : onglets du topic « Overview, Messages, Consumers, Settings, Statistics, ACLs » et un bouton « Produce Message » ; pas d'onglet producteurs. Overview : Partitions 3, Replication Factor 1, URP 0, In Sync Replicas « 3 of 3 », Type External, Segment Size 411 Bytes, Segment Count 3, Clean Up Policy DELETE, Message Count 2 ; partitions 0 et 1 vides, partition 2 « Next Offset » 2. Messages : deux messages en partition 2, offsets 1 et 0, horodatages `11:48:21.724` et `11:48:01.728` (heure locale), clé `adh1`. Onglet Consumers du topic : colonnes « Consumer Group ID, Active Consumers, Consumer Lag, Coordinator, State » ; `prelevement-service` : Active Consumers 1, Consumer Lag 0, Coordinator 1, State STABLE. Le menu de gauche contient aussi « Consumers ». L'écran de détail d'un groupe n'a pas été relevé.
  - **Déduit** : l'horodatage du record du premier message (`10:48:01.728Z`) est 222 ms après `emiseLe` (`10:48:01.506Z`) : il est posé à l'envoi, après la création du producteur.
- **Messages en direct** (`cotisation-service`, PID 27096) :
  - Démarrage : `Tomcat initialized with port 8081` ; un client d'administration (`adminclient-1`) est créé à 14:22:07.877 et fermé à 14:22:08.49, **avant** `Tomcat started on port 8081 (http)` (14:22:08.525) ; `Started CotisationServiceApplication in 3.14 seconds`. **Déduit** : c'est `KafkaAdmin` qui applique le bean `NewTopic` (le topic existait déjà). La création de topic reste non tracée dans les logs.
  - Le producteur n'est créé qu'à la première requête : `ProducerConfig values` à 14:22:24.730 (thread `nio-8081-exec-1`), `Instantiated an idempotent producer`, `Cluster ID` à .823, `ProducerId set to 2001 with epoch 0` à .827 (l'exécution du 16/11 à 10:53 avait obtenu l'identifiant 1000).
  - `POST adh1 100.00` : `201` en 0,407736 s ; `emiseLe` `2026-11-16T13:22:24.714998600Z` ; consommateur : `14:22:24.918 partition=2 offset=2 cle=adh1 idEvenement=fc846217-… montant=100.00 decision=ACCEPTE`.
  - `POST adh2 100.01` : `201` en 0,025054 s ; `emiseLe` `2026-11-16T13:23:11.989862800Z` ; consommateur : `14:23:12.010 partition=2 offset=3 cle=adh2 idEvenement=797f2113-… montant=100.01 decision=REJETE`.
  - `describe` : partition 2 : `4 4 0` ; partitions 0 et 1 : `- 0 -`.
  - **Déduit** : durée de bout en bout du second message environ 20 ms (`emiseLe` `13:23:11.9899Z`, décision `13:23:12.010Z`) ; la première requête paie la création du producteur (0,408 s contre 0,025 s).
- **Arrêt puis reprise, premier cycle** :
  - `Ctrl+C` sur le consommateur (les lignes de fermeture de ce processus, PID 35172, n'ont pas été relevées). `describe` : `Consumer group 'prelevement-service' has no active members.`, partition 2 : `4 4 0`, `CONSUMER-ID`, `HOST` et `CLIENT-ID` à `-`.
  - Pendant l'arrêt : `POST adh1 50.00` (`201`, 0,040689 s, `emiseLe` `13:28:26.522269600Z`) et `POST adh2 150.00` (`201`, 0,042201 s, `emiseLe` `13:28:32.410383800Z`). **Le lag de ce cycle n'a pas été capturé** (le consommateur était déjà relancé quand le `describe` a été tapé).
  - Reprise (PID 1692, lignes brutes) :
    ```
    14:28:46.442 Finished assignment for group at generation 3: {consumer-prelevement-service-1-11034754-…=Assignment(partitions=[cotisations.emises-0, cotisations.emises-1, cotisations.emises-2])}
    14:28:46.462 Found no committed offset for partition cotisations.emises-1 et -0
    14:28:46.463 ConsumerUtils : Setting offset for partition cotisations.emises-2 to the committed offset FetchPosition{offset=4, …}
    14:28:46.477/.478 Resetting offset for partition cotisations.emises-1 / -0 to position FetchPosition{offset=0, …}
    14:28:46.531 partition=2 offset=4 cle=adh1 idEvenement=06cb5f1b-… montant=50.00 decision=ACCEPTE
    14:28:46.533 partition=2 offset=5 cle=adh2 idEvenement=33abbc15-… montant=150.00 decision=REJETE
    ```
    **Constaté** : la partition 2 reprend à l'offset commité 4 ; aucune ligne `Resetting offset` pour elle ; les offsets 0 à 3 ne sont pas relus ; 150.00 est rejeté (supérieur au plafond de 100.00). **Déduit, non vérifié** : la génération 2 correspond au départ du premier membre (pas de log du broker) ; ce démarrage est en génération 3, le premier était en génération 1.
- **Départ propre** (PID 1692, `Ctrl+C`, lignes brutes) :
  ```
  14:29:59.371 Revoke previously assigned partitions [cotisations.emises-0, -1, -2]
  14:29:59.372 prelevement-service: partitions revoked: [...]
  14:29:59.373 Member consumer-prelevement-service-1-11034754-… sending LeaveGroup request to coordinator localhost:9092 (id: 2147483646 …) due to the consumer unsubscribed from all topics
  14:29:59.374 Resetting generation and member id due to: consumer pro-actively leaving the group
  14:29:59.375 Unsubscribed all topics or patterns and assigned partitions
  14:29:59.414 App info kafka.consumer … unregistered ; 14:29:59.415 prelevement-service: Consumer stopped
  ```
  `describe` : `has no active members`, partition 2 : `6 6 0`.
- **Second cycle, avec le lag capturé** : consommateur arrêté ; `POST adh1 70.00` (`201`, 0,062696 s, `emiseLe` `13:32:47.629726400Z`) et `POST adh2 120.00` (`201`, 0,013215 s, `emiseLe` `13:32:51.848787100Z`). `describe` : `has no active members`, partition 2 : `CURRENT-OFFSET 6`, `LOG-END-OFFSET 8`, **`LAG 2`**. Après relance : nouveau `CONSUMER-ID consumer-prelevement-service-1-6dc2624e-bc90-48a9-bc90-19f58dd6af47`, partition 2 : `8 8 0`, partitions 0 et 1 : `- 0 -`. Les lignes de décision de cette seconde reprise (offsets 6 et 7) **n'ont pas été relevées**.
- **Constaté, bilan** :
  - Le client 4.2.2 a rejoint un groupe (protocole `classic`, stratégie `range`), lu, décidé et commité des offsets contre le broker 4.4.0 ; reprise à l'offset commité ; départ propre avec `LeaveGroup`.
  - Un groupe sans membre conserve ses offsets, et chaque démarrage obtient un nouveau `CONSUMER-ID`.
  - Les partitions 0 et 1, jamais traitées, n'ont pas d'offset commité (`-` au `describe`, « Found no committed offset » à chaque démarrage, remise à 0 par `earliest`).
  - Le processus reste vivant sans serveur web : l'effet est constaté, le mécanisme (threads non-daemon du conteneur) n'a pas été lu.
- **Déduit** : avec un arrêt brutal (sans `LeaveGroup`), le broker n'identifierait le départ qu'après `session.timeout.ms` (45000 ms). Non exercé à cette étape ; constaté depuis : voir [phase-2-rebalancing.md](phase-2-rebalancing.md), scénario E.
- **Compatibilité client 4.2.2 et broker 4.4.0** : établie pour la production (section 6 de `phase-1-producteur.md`) et maintenant pour la consommation en groupe (rejoindre un groupe, lire, commiter, reprendre).

## 6. Non traité et non vérifié
- **Non traité (phase 3)** : les erreurs de désérialisation (message illisible) ; le comportement du conteneur n'est pas affirmé, il sera constaté en phase 3.
- **Non exercé** : les exceptions dans le listener (le `DefaultErrorHandler` par défaut est documenté mais jamais déclenché), le rebalancing à plusieurs membres (à deux membres : constaté, voir [phase-2-rebalancing.md](phase-2-rebalancing.md) ; au-delà : non exercé), `group.protocol=consumer` (KIP-848), les transactions, l'arrêt brutal (constaté depuis : voir [phase-2-rebalancing.md](phase-2-rebalancing.md), scénario E).
- **Non vérifié** :
  - Le mécanisme qui garde le processus vivant sans serveur web.
  - La création du topic par `KafkaAdmin` (non tracée).
  - Le premier `Ctrl+C` (PID 35172) : lignes de fermeture non relevées ; le lag de ce cycle n'a pas été capturé.
  - Les décisions des offsets 6 et 7 : non relevées.
  - L'écran de détail d'un groupe dans Kafka UI : non relevé.
  - **Le `repackage` de `cotisation-service` a réussi alors que son application tournait (port 8081 occupé, processus 34548 resté en écoute après un `Ctrl+C`), alors qu'il avait échoué dans les mêmes conditions à l'étape du renommage. Non expliqué.**
- **Suite prévue** : contrat `PrelevementResultat` dans `events-contract`, port de sortie et production sur `prelevements.resultats` ; les tests d'intégration liront alors le message produit plutôt que les logs.
