# Phase 1 : producteur Kafka (`cotisation-service`)

Environnement : Spring Boot 4.1.2, spring-kafka 4.1.2, kafka-clients 4.2.2 (versions : voir [versions.md](versions.md)), broker `apache/kafka:4.4.0` du compose.
Étape documentée : 3a (adaptateur Kafka de publication, tests unitaires). Les preuves sur un vrai broker viennent à l'étape 3b.

Pour chaque point : ce qui est **vérifié** (source citée) et ce qui est **déduit**.

## 1. Comportement de `send()` quand les métadonnées sont indisponibles
- **Vérifié (documentation)** : Javadoc de [`KafkaProducer` 4.2](https://kafka.apache.org/42/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html).
  - `send()` est asynchrone et rend la main tout de suite, mais « it will block for up to `max.block.ms` milliseconds while waiting for topic's metadata ».
  - Section Throws : `TimeoutException` « if the topic or the partition specified in the record cannot be found in metadata within `max.block.ms` », et `InterruptException`.
  - Le « failed future » de la Javadoc ne concerne que le buffer plein.
  - `max.block.ms` borne « the total time waiting for both metadata fetch and buffer allocation » ([producer-configs](https://kafka.apache.org/42/configuration/producer-configs/)).
- **Vérifié (bytecode, jars locaux, `javap -c`)** :
  - `KafkaProducer.doSend` (kafka-clients 4.2.2) attrape `ApiException` et crée un `KafkaProducer$FutureFailure`, c'est-à-dire un future en échec.
  - Il convertit `InterruptedException` en `InterruptException`, levée de façon synchrone. Les trois constructeurs de `InterruptException` appellent `Thread.currentThread().interrupt()` : le statut d'interruption est remis par Kafka.
  - `KafkaTemplate.observeSend` (spring-kafka 4.1.2) contient un handler pour `RuntimeException`.
- **Déduit, infirmé par le test 3c le 16/11/2026** :
  - `TimeoutException` est une `ApiException` (la Javadoc la dit réessayable). L'échec de métadonnées arriverait donc plutôt en future en échec, contrairement à ce que dit la section Throws de la Javadoc. Le bytecode et la Javadoc divergent ; je n'ai pas lu le code source.
  - **Infirmé** : le test sans broker (section 4) constate que `send()` lève une exception **synchrone** (`org.springframework.kafka.KafkaException` « Send failed », cause : `TimeoutException` de Kafka). Il n'y a pas de future en échec dans ce cas. La Javadoc avait raison. Hypothèse non vérifiée : le bloc d'attente des métadonnées n'a pas été lu en détail ; le handler `ApiException` du bytecode de `doSend` ne s'appliquerait pas à ce cas.
- **Non vérifié** :
  - Ce que fait le handler `RuntimeException` de `KafkaTemplate.observeSend` (relance synchrone ou non).
  - Le comportement avec un broker joignable mais sans le topic. Le cas d'un broker injoignable a été constaté à l'étape 3c (section 4).
- **Conséquence dans le code** : `PublicationCotisationKafka` protège les deux côtés. La branche `catch (RuntimeException)` est le chemin réellement emprunté quand les métadonnées sont indisponibles (constaté à l'étape 3c). Les échecs du future (`ExecutionException`), le délai dépassé (`TimeoutException`), l'interruption et les `RuntimeException` levées par `send()` sont tous enveloppés dans `PublicationImpossibleException`. Un cas nominal et quatre cas d'échec sont couverts par des tests unitaires (Mockito sur `KafkaTemplate`), qui prouvent la conversion d'exceptions mais rien du comportement réel de Kafka.

## 2. Délais du producteur
- **Vérifié (valeurs par défaut, Kafka 4.2)** : [producer-configs](https://kafka.apache.org/42/configuration/producer-configs/).

  | Propriété | Défaut | Rôle |
  |---|---|---|
  | `delivery.timeout.ms` | 120000 (2 min) | « An upper bound on the time to report success or failure after a call to `send()` returns » |
  | `request.timeout.ms` | 30000 (30 s) | Attente maximale de la réponse à une requête |
  | `linger.ms` | 5 | Attente pour regrouper des records dans un batch |
  | `max.block.ms` | 60000 (1 min) | Blocage dans `send()` (métadonnées, buffer) |
  | `retries` | 2147483647 | La doc recommande de piloter les retries par `delivery.timeout.ms` |
  | `acks` | `all` | |
  | `enable.idempotence` | `true` | |

  La page 4.4 n'a pas été lue.
- **Vérifié (contrainte)** : `delivery.timeout.ms` devrait être supérieur ou égal à `request.timeout.ms` + `linger.ms`. Aucune erreur de configuration explicite trouvée dans la page.
- **Valeurs choisies** (`application.yml`) : `request.timeout.ms` 5000, `delivery.timeout.ms` 9000, `max.block.ms` 5000, avec un délai applicatif de 10 s (`mutuelle.cotisation.kafka.delai-publication-ms`).
  - Contrainte respectée : 9000 ≥ 5000 + 5.
  - Raison : le délai de 10 s de l'appelant démarre au retour de `send()`. `request.timeout.ms` et `delivery.timeout.ms` sont sous ce délai, donc Kafka renonce avant nous. L'échec vient alors du producteur, pas de notre timeout.
- **Déduit** :
  - Aucun envoi ne continue en arrière-plan après l'abandon de l'appelant. Cela n'a jamais été constaté sur un vrai broker.
  - `max.block.ms` joue dans `send()`, avant le future, donc hors des 10 s. Pire cas d'un appel : 5 s + 9 s = environ 14 s. C'est un calcul à partir des définitions, non une mesure.
  - Ces trois valeurs sont de mon choix (« option B »), elles ne viennent pas de la documentation.
- **Vérifié (risque de doublon)** : Javadoc de `KafkaProducer` : « it is imperative to avoid application level re-sends since these cannot be de-duplicated » ; l'idempotence ne vaut que « within a single session ». Entrée `enable.idempotence` : « If 'false', producer retries due to broker failures, etc., may write duplicates ».
- **Déduit** : le `TimeoutException` de `get(10, SECONDS)` est celui de `java.util.concurrent` : il veut dire « nous avons cessé d'attendre », pas « le message n'est pas écrit ». Avec les défauts de Kafka (120 s), le producteur aurait pu écrire le message après notre abandon. Avec les valeurs choisies ci-dessus, cet écart est fermé côté producteur, mais un retry applicatif reste risqué sans déduplication côté consommateur (phase 3).

## 3. Test d'intégration de l'adaptateur avec `@EmbeddedKafka` (étape 3b)

> **Note du 16/11** : au moment de ces constats, le champ du message Kafka s'appelait `emisLe` ; il a été renommé `emiseLe` (commit `06ae463`). Les extraits de sortie réelle sont conservés tels quels.
- **Environnement** : broker embarqué du jar `spring-kafka-test` 4.1.2 (jars Kafka en 4.2.2), client `kafka-clients` 4.2.2. Test : `PublicationCotisationKafkaIntegrationTest` (3 tests), `mvn -B verify` du 16/11/2026 : `BUILD SUCCESS`.
- **Constaté, démarrage et câblage** : le contexte Spring démarre ; `KafkaTemplate<String, CotisationEmise>` est injecté ; `application.yml` est lu. Le log du broker embarqué contient `CreateTopics ... CreatableTopic(name='cotisations.emises', numPartitions=3, replicationFactor=1 ...): SUCCESS`. Le topic est créé par `KafkaAdmin` à partir du bean `NewTopic` : `@EmbeddedKafka` n'en déclare aucun.
- **Constaté, JSON brut** :
  ```
  clé=adh1 partition=2 offset=0 en-têtes=[]
  {"idEvenement":"c9feff0b-ab7c-44f7-88d0-ff3f16641ab9","idAdherent":"adh1","montant":50.00,"emisLe":"2026-11-16T10:00:00Z"}
  ```
  - `montant` est un nombre JSON, l'échelle est conservée (`50.00`).
  - `emisLe` est une chaîne ISO-8601, pas un nombre epoch.
  - La liste des en-têtes est vide : pas d'en-tête `__TypeId__` (`spring.json.add.type.headers=false`), ni aucun autre.
  - Ces trois points sont figés dans le test.
- **Constaté, routage par clé** (8 messages, clés `adh1` à `adh6`, `adh1` et `adh2` envoyées deux fois) :

  | Clé | Partition | Offsets |
  |---|---|---|
  | `adh1` | 2 | 1 et 3 |
  | `adh2` | 2 | 2 et 4 |
  | `adh3`, `adh4`, `adh6` | 0 | 0, 1, 2 |
  | `adh5` | 1 | 0 |

  (L'offset 0 de la partition 2 est le message du test JSON, exécuté avant.)
- **Comparaison avec la phase 0 : identique.** `adh1` et `adh2` en partition 2, `adh3`, `adh4` et `adh6` en partition 0, `adh5` en partition 1, comme au CLI. **Déduit** : c'est le même partitionneur par défaut du client Java qui donne ce résultat dans les deux cas. Ce n'est pas écrit dans une source lue.
- **Risque de lecture de `montant`** : un consommateur qui lit ce nombre en `double` perd en précision. Il doit le lire en `BigDecimal`.
- **Origine du format** : il vient du `JsonMapper` du sérialiseur `JacksonJsonSerializer`. Aucun lien n'est démontré avec les réglages Jackson de Boot.
- **Non vérifié** :
  - La compatibilité du client 4.2.2 avec le broker 4.4.0 : le broker embarqué n'est pas en 4.4.0, ce test ne la prouve pas. Le log du broker embarqué indique `metadata.version 4.3-IV0`, que je n'explique pas.
  - Le comportement de `send()` sans broker : voir la section 4.

## 4. `send()` sans broker (étape 3c)
- **Test** : `PublicationCotisationKafkaSansBrokerTest` (2 tests). Un port libre est obtenu par `ServerSocket(0)` puis fermé : aucun broker n'écoute. `max.block.ms` 1000, `request.timeout.ms` 1000, `delivery.timeout.ms` 2000 (réduits par rapport à la production : 5000, 5000, 9000).
- **Constaté (16/11/2026)** :
  - `send()` direct : exception synchrone après 1035 ms : `org.springframework.kafka.KafkaException` (« Send failed »), cause : `org.apache.kafka.common.errors.TimeoutException` (« Topic cotisations.emises not present in metadata after 1000 ms. »).
  - Via `PublicationCotisationKafka` : `PublicationImpossibleException` après 1007 ms, cause : la `KafkaException` de Spring, cause : le `TimeoutException` de Kafka.
  - Aucun future en échec : l'appelant n'obtient jamais de `CompletableFuture` dans ce cas.
- **Figé dans le test** : l'exception est synchrone, sa chaîne de causes contient le `TimeoutException` de Kafka, la durée est entre 900 et 5000 ms. Les messages des bibliothèques ne sont pas figés (ils changeront avec les versions).
- **Déduit, puis constaté sur un broker réel (section 7)** : `send()` lève avant d'avoir accepté le record, donc le message n'a pas été écrit : deux requêtes en échec par ce chemin, messages absents du topic. À distinguer du `TimeoutException` de `get(...)` (section 2), où le message a pu être écrit (non exercé).
- **Non vérifié** :
  - Si « Send failed » est créé par le producteur ou par `KafkaTemplate`.
  - Le comportement avec un broker joignable mais sans le topic.
  - La durée avec la configuration réelle (`max.block.ms` 5000) : environ 5 s, calcul non mesuré.

## 5. Endpoint REST `POST /cotisations` (étape 4a)
- **Test** : `CotisationControllerTest` (4 tests, `@WebMvcTest` + `MockMvcTester`, `EmettreCotisation` simulé par `@MockitoBean`, sans Kafka). `mvn -B verify` du 16/11/2026 : `BUILD SUCCESS`.
- **Contrat** : corps `{"idAdherent","montant"}` (DTO `EmettreCotisationRequete`, séparé du domaine) ; réponse 201 avec `{"idAdherent","montant","emiseLe"}` (DTO `CotisationReponse`).
- **Erreurs** (`ProblemDetail`) : `CotisationInvalideException` donne 400 ; `PublicationImpossibleException` donne 503 ; un JSON mal formé donne 400 (constaté, le cas d'usage n'est pas appelé).
- **Constaté** : la réponse 201 conserve l'échelle du montant (`"montant":50.00`, lu sur le texte brut), avec le mapper de Spring MVC. Ce n'est pas le mapper du sérialiseur Kafka (section 3) : aucun lien n'est démontré entre les deux.
- **Le 503 peut cacher un message déjà publié** : il couvre deux situations qu'un client ne peut pas distinguer.
  - Échec constaté : future en échec, ou exception synchrone de `send()` quand les métadonnées sont indisponibles (section 4). Le message n'est alors pas écrit (constaté pour l'exception synchrone, section 7 ; non exercé pour le futur en échec).
  - Délai de publication dépassé (`TimeoutException` de `get(...)`) : le producteur a pu écrire le message après l'abandon. Un client qui rejoue sa requête risque un doublon. Le texte du 503 le dit (« la cotisation a peut-être été publiée »). La protection (déduplication côté consommateur) viendra en phase 3.
- **L'appel REST est bloquant** : la requête HTTP occupe un fil Tomcat jusqu'à la fin de la publication. Durée maximale : environ 14 s (`max.block.ms` 5 s + `delivery.timeout.ms` 9 s), cohérent avec la section 2 ; **déduit, non mesuré**. Si les métadonnées arrivent après 4 s, `send()` réussit et l'attente du future peut encore durer jusqu'à `delivery.timeout.ms` (9 s). Le délai applicatif de 10 s ne borne rien tant que `delivery.timeout.ms` (9 s) est plus court. Beaucoup de requêtes simultanées avec un Kafka lent peuvent épuiser les fils de Tomcat.
- **Non vérifié** : la valeur par défaut du port (8080) n'a été lue qu'en résumé de page ; le port 8081 est fixé dans `application.yml` (constaté à l'étape 4b : `Tomcat started on port 8081`). Le comportement de bout en bout sur le broker du compose et la compatibilité du client 4.2.2 avec le broker 4.4.0 sont traités à la section 6.

## 6. Démarrage de bout en bout contre le broker du compose (étape 4b)

> **Note du 16/11** : au moment de ces constats, le champ du message Kafka s'appelait `emisLe` ; il a été renommé `emiseLe` (commit `06ae463`). Les extraits de sortie réelle sont conservés tels quels.
- **Environnement** (16/11/2026) : jar exécutable `cotisation-service-0.1.0-SNAPSHOT.jar` (construit par `mvn -B -pl cotisation-service -am package -DskipTests`), lancé par `java -jar` sur JDK 22, port 8081. Broker `apache/kafka:4.4.0` du compose (`kafka`, `kafka-ui`, volume `kafka-mutuelle-data`). Versions résolues : voir [versions.md](versions.md).
- **Constaté, démarrage de l'application** :
  ```
  10:51:41.320 INFO AppInfoParser : Kafka version: 4.2.2
  10:51:42.399 INFO TomcatWebServer : Tomcat started on port 8081 (http) with context path '/'
  10:51:42.408 INFO CotisationServiceApplication : Started CotisationServiceApplication in 3.483 seconds
  ```
  La ligne `Kafka version: 4.2.2` (client) apparaît avant `Tomcat started`, donc avant toute requête. **Déduit** : elle vient du client d'administration que `KafkaAdmin` crée au démarrage.
- **Constaté, broker** : `[2026-11-13 14:57:06,174] INFO Kafka version: 4.4.0` (log de démarrage du broker, recréé le 13/11 par le compose).
- **Constaté, topic** : `kafka-topics --describe` donne `cotisations.emises`, `PartitionCount: 3`, `ReplicationFactor: 1`, `min.insync.replicas=1`, `Leader: 1 / Isr: 1` pour les partitions 0, 1 et 2. Kafka UI : 3 partitions, réplication 1, 7 messages, offsets suivants 3, 1 et 3 pour les partitions 0, 1 et 2.
- **Déduit, topic créé par l'application** : le topic n'existait pas sur ce broker avant le démarrage (le volume est neuf depuis l'étape du compose, seul `test.persistance` y avait été créé à la main) et sa configuration (3 partitions, 1 réplique) est celle du bean `NewTopic`. La création elle-même n'a pas été constatée dans les logs.
- **Constaté, requêtes valides** (`curl`) :
  - Cas nominal (`curl -i`, non chronométré) : `HTTP/1.1 201`, `Content-Type: application/json`, `Content-Length: 80`, corps `{"idAdherent":"adh1","montant":50.00,"emiseLe":"2026-11-16T09:53:40.211861200Z"}`.
  - Six adhérents, tous en 201 :

    | Adhérent | Durée |
    |---|---|
    | `adh1` (12.50) | 0,040366 s |
    | `adh2` | 0,019365 s |
    | `adh3` | 0,021230 s |
    | `adh4` | 0,035825 s |
    | `adh5` | 0,024890 s |
    | `adh6` | 0,028185 s |

    Soit **19 à 40 ms**. **Déduit** : comme l'adaptateur attend l'acquittement de Kafka avant de répondre, ces durées incluent l'aller-retour vers le broker.
  - La première requête (cas nominal) n'a pas été chronométrée, mais ses logs la bornent : `emiseLe` à 10:53:40.211 et `ProducerId set to 1000 with epoch 0` à 10:53:40.574, donc **au moins environ 0,36 s** (contre 19 à 40 ms ensuite). **Déduit** : l'envoi attend l'identifiant du producteur idempotent.
- **Constaté, requêtes invalides** (aucun message produit : le topic contient exactement 7 messages, le cas nominal plus les six adhérents) :

  | Cas | Statut | Format | Corps |
  |---|---|---|---|
  | `montant` 0 | 400 | `application/problem+json` | `{"detail":"montant doit être strictement positif","instance":"/cotisations","status":400,"title":"Cotisation invalide"}` |
  | `idAdherent` vide | 400 | `application/problem+json` | `{"detail":"idAdherent ne doit pas être vide","instance":"/cotisations","status":400,"title":"Cotisation invalide"}` |
  | `idAdherent` absent | 400 | `application/problem+json` | `{"detail":"idAdherent ne doit pas être null","instance":"/cotisations","status":400,"title":"Cotisation invalide"}` |
  | JSON mal formé | 400 | `application/json` (format d'erreur par défaut de Spring) | `{"timestamp":"2026-11-16T09:54:01.528Z","status":400,"error":"Bad Request","path":"/cotisations"}` |

  Les trois cas du domaine passent par `GestionErreursRest` (`ProblemDetail`, sans champ `type`). Le JSON mal formé échoue avant le contrôleur et sort au format Spring par défaut (constaté dans les logs : `DefaultHandlerExceptionResolver : Resolved [HttpMessageNotReadableException: JSON parse error: Unrecognized token ...]`, 10:54:01.506). L'API a donc **deux formats d'erreur**. Cas du champ absent : la déduction de l'étape 4a (400 par `CotisationInvalideException`) est confirmée.
- **Constaté, JSON réel côté broker 4.4.0** (`console-consumer --from-beginning --timeout-ms 10000`, avec `print.headers`) :
  ```
  Partition:2  Offset:0  NO_HEADERS  adh1  {"idEvenement":"63f8e136-814d-4bf1-9631-bc4a67a40ce4","idAdherent":"adh1","montant":50.00,"emisLe":"2026-11-16T09:53:40.211861200Z"}
  Partition:2  Offset:1  NO_HEADERS  adh1  {"idEvenement":"d80b0685-737b-497f-a279-45722420ae16","idAdherent":"adh1","montant":12.50,"emisLe":"2026-11-16T09:53:53.093623100Z"}
  Partition:2  Offset:2  NO_HEADERS  adh2  {"idEvenement":"d4f358df-7ea7-4754-9de0-0b16d50cc4ae","idAdherent":"adh2","montant":30.00,"emisLe":"2026-11-16T09:53:53.170048600Z"}
  Partition:1  Offset:0  NO_HEADERS  adh5  {"idEvenement":"d2e9b94e-67ee-48db-b0dd-3d5af2b0412b","idAdherent":"adh5","montant":75.50,"emisLe":"2026-11-16T09:53:53.369725700Z"}
  Partition:0  Offset:0  NO_HEADERS  adh3  {"idEvenement":"3436ab0f-18bb-4ddf-927a-eeab73530e5c","idAdherent":"adh3","montant":45.00,"emisLe":"2026-11-16T09:53:53.230116700Z"}
  Partition:0  Offset:1  NO_HEADERS  adh4  {"idEvenement":"14b09bb2-a33c-47b5-8135-c025de282fa8","idAdherent":"adh4","montant":20.00,"emisLe":"2026-11-16T09:53:53.307807300Z"}
  Partition:0  Offset:2  NO_HEADERS  adh6  {"idEvenement":"f1ce3fd9-181e-4557-bcac-945218b73547","idAdherent":"adh6","montant":60.00,"emisLe":"2026-11-16T09:53:53.441586600Z"}
  Processed a total of 7 messages
  ```
  - **Aucun en-tête** (`NO_HEADERS`), donc pas de `__TypeId__`.
  - `montant` est un nombre JSON avec son échelle (`50.00`, `12.50`, `75.50`).
  - **L'instant garde sa précision d'origine** : `emisLe` a jusqu'à 9 chiffres de fraction (`2026-11-16T09:53:40.211861200Z`), alors que le test de la section 3 utilisait une date sans fraction. Un consommateur doit lire de l'ISO-8601 avec une fraction variable. C'est la même valeur que `emiseLe` dans la réponse REST du cas nominal : l'instant est créé une fois par l'horloge du cas d'usage.
  - Les 7 `idEvenement` sont tous différents.
  - Le message `ERROR ... TimeoutException` à la fin du consommateur est la **sortie normale de `--timeout-ms 10000`** (10 s sans nouveau message), comme en phase 0. La sortie indique `Processed a total of 7 messages`.
- **Constaté, routage** : identique à la phase 0 et à la section 3.

  | Clé | Partition | Offsets |
  |---|---|---|
  | `adh1` (2 messages), `adh2` | 2 | 0, 1, 2 |
  | `adh3`, `adh4`, `adh6` | 0 | 0, 1, 2 |
  | `adh5` | 1 | 0 |

  Cela correspond à la répartition de Kafka UI (partition 0 : 3, partition 1 : 1, partition 2 : 3).
- **Constaté, ordre** :
  - Dans chaque partition, l'ordre d'écriture est celui des instants `emisLe` (partition 2 : 09:53:40, 09:53:53.093, 09:53:53.170 ; partition 0 : …53.230, …53.307, …53.441).
  - Entre partitions, il n'y a **pas d'ordre global** : la sortie du consommateur n'est pas dans l'ordre d'envoi (`adh5`, envoyé à …53.369, est affiché avant `adh3`, envoyé à …53.230).
- **Compatibilité du client 4.2.2 avec le broker 4.4.0, pour les API utilisées seulement** :
  - **Établi** : le client 4.2.2 (log `Kafka version: 4.2.2`) a travaillé avec le broker 4.4.0 : le topic existe avec 3 partitions, 7 messages envoyés par l'application y sont stockés, lus par les outils CLI et par Kafka UI.
  - **Constaté dans le log du producteur** (10:53:40) : `enable.idempotence = true`, `acks = -1`, `Instantiated an idempotent producer`, `ProducerId set to 1000 with epoch 0` (l'initialisation du producteur idempotent a abouti contre le broker 4.4.0), et `Cluster ID: 4L6g3nShT-eMCtK--X86sw` (celui du compose).
  - **Déduit, non tracé** : la création du topic par `KafkaAdmin`. Preuve de la section 8 : après suppression, le topic a reparu avec 3 partitions après le redémarrage de l'application (effet constaté ; la création elle-même n'est pas dans les logs). Les requêtes réellement échangées n'ont pas été relevées.
  - **Non établi** : la lecture de messages par un client 4.2.2 (le consommateur est l'outil CLI du broker), les groupes de consommateurs, les transactions, et le reste du protocole.
- **Écart de nommage** : le champ de la réponse REST s'appelle `emiseLe` (`CotisationReponse`, objet du domaine) et celui du message Kafka `emisLe` (`CotisationEmise`, contrat partagé). Les deux contrats sont indépendants, mais un client qui lit les deux doit connaître l'écart. À trancher : l'aligner, ou le laisser tel quel.
  - **Résolu le 16/11** : champ renommé `emiseLe` dans le contrat Kafka (commit `06ae463`), preuve sur le fil à la section 8.
- **Non constaté** : le comportement sous charge, le lag. Le scénario 503 réel est consigné à la section 7.

## 7. Scénario 503 réel : broker arrêté, producteur déjà chaud (étape 4b)
- **Contexte** (16/11/2026, heure locale +01:00) : producteur déjà chaud (7 messages envoyés à 10:53, section 6), broker du compose arrêté puis redémarré, application non redémarrée.
- **Constaté, configuration du producteur** (log au premier envoi, 10:53:40.235) : `acks = -1`, `enable.idempotence = true`, `linger.ms = 5`, `request.timeout.ms = 5000`, `delivery.timeout.ms = 9000`, `max.block.ms = 5000`, `retries = 2147483647`, `metadata.recovery.strategy = rebootstrap`, `metadata.recovery.rebootstrap.trigger.ms = 300000`, `key.serializer = StringSerializer`, `value.serializer = JacksonJsonSerializer`.
- **Infirmé** : ma déduction « environ 9 s, par le futur, après `delivery.timeout.ms` » (producteur chaud, métadonnées en cache, lot expirant à `delivery.timeout.ms`). Relevé de l'utilisateur : 5,07 s et 5,02 s, sans futur en échec.
- **Constaté, panne** :
  ```
  11:11:09.261 INFO NetworkClient : [Producer clientId=producer-1] Node 1 disconnected.
  11:11:09.263 INFO Metadata : [Producer clientId=producer-1] Rebootstrapping with [localhost/127.0.0.1:9092]
  11:11:09.271 INFO NetworkClient : Node -1 disconnected. / Cancelled in-flight API_VERSIONS request ... (request timeout: 5000ms)
  11:11:09.276 WARN NetworkClient : Bootstrap broker localhost:9092 (id: -1 rack: null isFenced: false) disconnected
  ```
  Ces lignes se répètent environ une par seconde ; 118 lignes « Connection to node -1 (localhost/127.0.0.1:9092) could not be established. Node may not be available. » dans tout le log (comptage de l'utilisateur).
- **Constaté, chemin pris : exception synchrone de `send()`**, même signature qu'à l'étape 3c (section 4) :
  ```
  11:12:03.011 ERROR LoggingProducerListener : Exception thrown when sending a message with key='adh1' ... to topic cotisations.emises:
  org.apache.kafka.common.errors.TimeoutException: Topic cotisations.emises not present in metadata after 5000 ms.
  11:12:03.021 ERROR GestionErreursRest : Publication impossible
  PublicationImpossibleException: Échec de la publication de la cotisation de adh1
    at PublicationCotisationKafka.publier(PublicationCotisationKafka.java:53)
    at EmettreCotisation.emettre(EmettreCotisation.java:20)
  Caused by: org.springframework.kafka.KafkaException: Send failed
    at KafkaTemplate.doSend(KafkaTemplate.java:876) / observeSend(:833) / send(:609)
    at PublicationCotisationKafka.publier(PublicationCotisationKafka.java:36)
  Caused by: org.apache.kafka.common.errors.TimeoutException: Topic cotisations.emises not present in metadata after 5000 ms.
  ```
  La requête 2 (`adh2`, 88.00) donne la même chaîne (11:12:14.532 et .534). La ligne 36 est l'appel `kafkaTemplate.send(...)` ; la ligne 53 est le `throw` de la branche `catch (RuntimeException)`.
- **Cause racine** : `TimeoutException` de Kafka, « Topic cotisations.emises not present in metadata after 5000 ms ».
- **Ce que la trace établit sur les délais** :
  - **Constaté** : aucun `ExecutionException` ni `KafkaProducerException` : aucun futur n'a échoué. Ni `request.timeout.ms` ni `delivery.timeout.ms` n'a fait échouer la requête.
  - **Déduit** : ce `TimeoutException` correspond à `max.block.ms`. Cette déduction s'appuie sur la Javadoc de `send()` (section 1 : `send()` bloque jusqu'à `max.block.ms` en attente des métadonnées) et sur le message « after 5000 ms », pas sur la lecture du code.
  - `request.timeout.ms` et `max.block.ms` valent tous les deux 5000 : la durée seule (5,07 s et 5,02 s) ne les distinguait pas. C'est la trace (exception levée par `send()`, pas de futur) qui tranche entre les deux chemins.
  - Le `(request timeout: 5000ms)` de 11:11:09.271 concerne une requête `API_VERSIONS` annulée à la déconnexion, hors du chemin de `send()`.
- **Pourquoi le producteur chaud n'avait plus de métadonnées** :
  - **Constaté** : `Node 1 disconnected` puis `Rebootstrapping` 2 ms après, avec `metadata.recovery.strategy = rebootstrap`.
  - **Déduit** : les métadonnées du topic sont abandonnées à la coupure, d'où l'attente de `send()`.
  - **Non expliqué** : le rebootstrap immédiat, alors que `metadata.recovery.rebootstrap.trigger.ms` vaut 300000.
- **Constaté, reprise automatique** (sans redémarrer l'application) :
  ```
  11:13:16.575 INFO Metadata : Rebootstrapping with [localhost/127.0.0.1:9092]
  11:13:16.681 INFO Metadata : [Producer clientId=producer-1] Cluster ID: 4L6g3nShT-eMCtK--X86sw
  ```
  Même `Cluster ID` que celui du premier envoi (10:53:40.414) et que le `CLUSTER_ID` du compose, 106 ms après le début de la reprise.
- **Relevé de l'utilisateur, 503** : durées de 5,07 s et 5,02 s ; 7 messages dans le topic avant et après la panne ; les messages `99.00` et `88.00` sont absents. Ces deux 503 n'ont donc rien publié (les sorties brutes de `curl` et du consommateur ne sont pas reproduites ici).
- **À noter, double journalisation** : l'erreur est écrite deux fois, par `LoggingProducerListener` (Spring Kafka) puis par `GestionErreursRest`, à 10 ms d'écart. Aucune action pour l'instant.
- **Non exercé** :
  - Le cas « délai dépassé après envoi » (`TimeoutException` de `get(...)`), seul cas où le message peut avoir été publié malgré le 503.
  - Le chemin du futur en échec, donc la déduction « Kafka renonce avant nous » (option B des délais, section 2).
  - Le démarrage à froid avec broker arrêté (chemin B).

## 8. Renommage `emisLe` en `emiseLe` et preuve sur le fil (étape 4b, suite)
- **Contexte** (16/11/2026, heure locale +01:00) : « cotisation » est féminin, et le domaine et le REST disaient déjà `emiseLe` ; le contrat Kafka disait `emisLe` (écart de la section 6). Le champ du contrat `CotisationEmise` est renommé `emiseLe` (commit `06ae463`) : le nom du champ JSON du message Kafka change.
- **Pourquoi supprimer le topic** : `cotisations.emises` contenait 7 messages au format `emisLe` (section 6). Aucun consommateur n'existe encore dans le dépôt. **Constaté dans le code** : le constructeur compact de `CotisationEmise` lève une `NullPointerException` si `emiseLe` est `null` ; `JacksonJsonDeserializer` utilise `JacksonMapperUtils.enhancedJsonMapper()`, qui désactive `FAIL_ON_UNKNOWN_PROPERTIES` (bytecode du jar spring-kafka 4.1.2). **Déduit, non exercé** : un consommateur lisant depuis le début aurait ignoré `emisLe`, trouvé `emiseLe` absent, et échoué dans le constructeur, au risque de rester bloqué sur le premier ancien message. Le topic de démonstration a donc été supprimé (option A) plutôt que de garder deux formats. Le volume Docker n'a pas été touché et `docker compose down -v` n'a pas été utilisé.
- **Constaté, construction** : après l'arrêt de l'application (qui verrouillait le jar : `Unable to rename ... .jar to ... .jar.original`), `mvn -B verify` s'est terminé en `BUILD SUCCESS`. Le décompte des tests de ce `verify` n'a pas été relevé ; les 11 tests de `events-contract` et 26 de `cotisation-service` viennent d'un `mvn -B test` précédent.
- **À noter, log de test** : la sortie Maven contient une pile `PublicationImpossibleException ... Caused by: RuntimeException: cause` pour `CotisationControllerTest`. C'est le log `ERROR` de `GestionErreursRest` dans le test `repond_503_quand_la_publication_est_impossible` (la cause est créée par le test), pas une erreur ; la classe indique `Tests run: 4, Failures: 0, Errors: 0`.
- **Constaté, suppression puis liste** :
  ```
  PS> docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic cotisations.emises
  (aucune sortie)
  PS> docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
  __consumer_offsets
  test.persistance
  ```
  Le caractère asynchrone de la suppression n'a pas été constaté (une seule liste relevée).
- **Constaté, topic recréé** (après relance de l'application) :
  ```
  Topic: cotisations.emises  TopicId: MHgBgt8ISFuLaM9qL5d20Q  PartitionCount: 3  ReplicationFactor: 1  Configs: min.insync.replicas=1
  Partition: 0 Leader: 1 Replicas: 1 Isr: 1 / Partition: 1 Leader: 1 Replicas: 1 Isr: 1 / Partition: 2 Leader: 1 Replicas: 1 Isr: 1
  ```
  Le `TopicId` est nouveau (l'ancien, section 6, était `wz5mT0nSR6S63tNfj_nO7g`) : le topic est bien neuf.
- **Origine des 3 partitions** : le compose ne définit ni `KAFKA_NUM_PARTITIONS` ni `KAFKA_AUTO_CREATE_TOPICS_ENABLE`.
  - **Constaté** : le topic est absent après la suppression et présent avec 3 partitions après la relance de l'application.
  - **Déduit** : il a été créé par notre bean `NewTopic` via `KafkaAdmin` (c'est le seul code qui demande 3 partitions ; la valeur par défaut de `num.partitions` du broker n'a pas été relue). L'ordre entre le `describe` et le premier `POST` n'est pas établi.
  - La création n'est pas tracée dans les logs.
- **Constaté, `POST`** :
  ```
  HTTP/1.1 201
  Content-Type: application/json
  Content-Length: 77
  Date: Mon, 16 Nov 2026 10:48:21 GMT
  {"idAdherent":"adh1","montant":50.00,"emiseLe":"2026-11-16T10:48:21.724712Z"}
  ```
- **Constaté, relecture au broker 4.4.0** (`console-consumer --from-beginning --timeout-ms 10000`, avec `print.headers`) :
  ```
  Partition:2  Offset:0  NO_HEADERS  adh1  {"idEvenement":"d1c2d30a-fc4b-43c5-bf04-5a630fcb6f97","idAdherent":"adh1","montant":50.00,"emiseLe":"2026-11-16T10:48:01.506356100Z"}
  Partition:2  Offset:1  NO_HEADERS  adh1  {"idEvenement":"20e78821-f1c4-473e-b1aa-36d13f8ef394","idAdherent":"adh1","montant":50.00,"emiseLe":"2026-11-16T10:48:21.724712Z"}
  Processed a total of 2 messages
  ```
  - Le champ du message Kafka s'appelle `emiseLe` ; aucun message ne contient `emisLe`. Pas d'en-tête.
  - Les deux messages sont en partition 2 pour la clé `adh1`, comme dans les observations précédentes (même clé, même partitionneur).
  - Fractions de l'instant : 9 chiffres (`…506356100Z`) et 6 chiffres (`…724712Z`), cohérent avec la section 3 (jusqu'à 9 chiffres). **Déduit** : la longueur varie avec la valeur de l'instant à la sortie de `Instant.toString`.
  - Le message `ERROR ... TimeoutException` de fin est la sortie normale de `--timeout-ms` (comme aux sections 6 et 7).
  - Le message à l'offset 0 (`emiseLe` à 10:48:01) ne correspond à aucun `curl` reproduit ici : « premier envoi, non reproduit ». **Déduit** : probablement un doublon de manipulation de l'utilisateur (une ligne de commentaire du plan collée par erreur dans PowerShell, sans effet, et un `POST` lancé en double) ; non vérifiable.
- **À noter pour la phase 2** : l'outil CLI affiche « The consumer rebalance protocol (KIP-848) is production-ready! Set group.protocol=consumer to try it out ».
- **Non vérifié** : la création du topic par `KafkaAdmin` n'est pas tracée dans les logs ; la valeur par défaut de `num.partitions` du broker n'a pas été lue ; l'ordre entre le `describe` et le premier `POST` ; le comportement réel d'un consommateur face à un ancien message (jamais exercé, il n'y a pas de consommateur).
