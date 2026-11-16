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
- **Déduit** : `send()` lève avant d'avoir accepté le record, donc le message n'a pas été écrit. À distinguer du `TimeoutException` de `get(...)` (section 2), où le message a pu être écrit. Cela n'a pas été constaté sur un broker.
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
  - Échec constaté : future en échec, ou exception synchrone de `send()` quand les métadonnées sont indisponibles (section 4). Le message n'est alors pas écrit (déduit).
  - Délai de publication dépassé (`TimeoutException` de `get(...)`) : le producteur a pu écrire le message après l'abandon. Un client qui rejoue sa requête risque un doublon. Le texte du 503 le dit (« la cotisation a peut-être été publiée »). La protection (déduplication côté consommateur) viendra en phase 3.
- **L'appel REST est bloquant** : la requête HTTP occupe un fil Tomcat jusqu'à la fin de la publication. Durée maximale : environ 14 s (`max.block.ms` 5 s + `delivery.timeout.ms` 9 s), cohérent avec la section 2 ; **déduit, non mesuré**. Si les métadonnées arrivent après 4 s, `send()` réussit et l'attente du future peut encore durer jusqu'à `delivery.timeout.ms` (9 s). Le délai applicatif de 10 s ne borne rien tant que `delivery.timeout.ms` (9 s) est plus court. Beaucoup de requêtes simultanées avec un Kafka lent peuvent épuiser les fils de Tomcat.
- **Non vérifié** : le comportement de bout en bout sur le broker du compose (étape 4b), y compris la compatibilité du client 4.2.2 avec le broker 4.4.0. La valeur par défaut du port (8080) n'a été lue qu'en résumé de page ; le port 8081 est fixé dans `application.yml`.
