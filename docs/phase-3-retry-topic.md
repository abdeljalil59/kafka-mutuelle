# Phase 3 : retries non bloquants avec `@RetryableTopic` (`prelevement-service`)

Environnement (18/11/2026, heure locale +01:00) : Spring Boot 4.1.2, spring-kafka 4.1.2, kafka-clients 4.2.2 (versions : voir [versions.md](versions.md)). Retries bloquants et DLT de la source : [phase-3-message-invalide.md](phase-3-message-invalide.md). Écriture idempotente : [phase-3-idempotence.md](phase-3-idempotence.md).

Pour chaque point : ce qui est **constaté** (test, sortie réelle, source ou doc lue), **déduit** et **non vérifié**. Documentation : Spring for Apache Kafka 4.1.2, pages `retrytopic/*` et `kafka/receiving-messages/listener-annotation`, lues par un agent de lecture ; les citations sont reprises telles que rapportées.

## 1. Objectif
- Comprendre `@RetryableTopic` (retries non bloquants) et le comparer au `DefaultErrorHandler` (retries bloquants) qui protège `cotisations.emises`.
- Cadre : démonstration technique isolée, sans cas d'usage métier (`RetryDemoListener`, inactif par défaut).
- **Commits** : `1c62cb7` (`feat`), `16d2c3b` (`test`), puis ce journal. **Constaté** : `BUILD SUCCESS`, 50 tests pour `prelevement-service` (46 existants + 4), 11 pour `events-contract`.

## 2. Concept
- **Bloquant (`DefaultErrorHandler`)** : le message en échec est relivré depuis la même partition. **Constaté** : 10 livraisons sans pause (`FixedBackOff(0, 9)`, [phase-3-idempotence.md](phase-3-idempotence.md)). Les messages suivants de la partition attendent : **constaté** pour le message illisible sans protection ([phase-3-message-invalide.md](phase-3-message-invalide.md), section 2), **déduit** pour des réessais avec pause.
- **Non bloquant** : après un échec, le message est republié avec un horodatage d'échéance sur un topic de retry, et le consommateur passe au suivant. Doc : « The retry topic consumer then checks the timestamp and if it's not due it pauses the consumption for that topic's partition. » Après `attempts` échecs : topic DLT et `@DltHandler`.
- **Compromis** : l'ordre par clé n'est plus garanti, car un message en retry est dépassé par les suivants (**déduit** du mécanisme ; **relevé** en démo live, section 13 ; pas de test automatisé).
- **Limites de la doc (constatées)** : « Non-blocking retries are not supported with Batch Listeners. » et ils ne se combinent pas avec les transactions de container.

## 3. Choix d'implémentation
- **Listener de démonstration isolé** `adapter/in/kafka/RetryDemoListener` sur `cotisations.retry-demo` (3 partitions, `NewTopic`), groupe `retry-demo-group`. `"simulerEchec": true` lève une `RuntimeException`. Aucun code de `domain/` ni d'`application/` n'est touché.
- **Inactif par défaut** (`prelevement.kafka.retry-demo.actif=false`). **Constaté** : les 46 tests existants passent sans changement, la démo étant absente de leurs contextes.
- Payload lu en **octets** (`byte[]`), JSON lu dans le listener avec le `JsonMapper` de Boot.
- `ErreursKafkaConfig` et `CotisationEmiseListener` ne sont pas modifiés (**constaté**, `git`).

## 4. Pièges de cohabitation avec le `DefaultErrorHandler`
| Piège | Statut | Parade |
|---|---|---|
| Un second bean `CommonErrorHandler` | Boot le branche par `ObjectProvider.getIfUnique()` (**constaté**, bytecode de `KafkaAnnotationDrivenConfiguration`). Que `getIfUnique()` renvoie `null` s'il y a plusieurs candidats est la Javadoc d'`ObjectProvider`, non relue : **déduit** | Aucun second bean déclaré |
| Nommer une fabrique `kafkaListenerContainerFactory` | La fabrique de Boot est `@ConditionalOnMissingBean(name = "kafkaListenerContainerFactory")` (**constaté**, bytecode) : le listener existant passerait sur la nôtre | Fabrique de la démo : `retryDemoContainerFactory` |
| `ConsumerFactory` ou `KafkaTemplate` déclaré en bean | Ceux de Boot sont `@ConditionalOnMissingBean(ConsumerFactory.class)` / `(KafkaTemplate.class)` (**constaté**, bytecode) | `ConsumerFactory` construit dans la méthode de la fabrique ; `KafkaTemplate` de Boot réutilisé |
| Même `groupId` sur un même topic | Les deux listeners se partageraient les partitions : **déduit** (la doc lue ne le dit pas) | `retry-demo-group`. **Constaté** : le groupe du service n'a aucun offset sur les topics de la démo |
| Collision de noms de DLT | Les deux mécanismes nomment leur DLT `<topic>-dlt` (défaut `-dlt` : **constaté**, doc et annotation). Mélange sur un même topic source : **déduit**, non essayé | Topic source dédié : `cotisations.retry-demo-dlt` ≠ `cotisations.emises-dlt`. **Constaté** : chaque message ne va qu'à son DLT |
| `@RetryableTopic` modifie-t-il la fabrique ? | **Constaté** (bytecode) : le configurateur n'appelle que `createContainer` / `createListenerContainer` sur la fabrique ; son décorateur pose un `CommonErrorHandler` sur **chaque container créé**. **Constaté** (test) : démo active, un message illisible va toujours à `cotisations.emises-dlt` | Rien à faire |

## 5. Configuration
```java
@RetryableTopic(attempts = "3",
        backOff = @BackOff(delayString = "${prelevement.kafka.retry-demo.backoff-delay:5000}",
                multiplierString = "${prelevement.kafka.retry-demo.backoff-multiplier:2}"),
        autoCreateTopics = "true", numPartitions = "3",
        kafkaTemplate = "kafkaTemplate", listenerContainerFactory = "retryDemoContainerFactory")
```
- **Noms d'API (constatés, bytecode)** : `attempts` (et non `maxAttempts`), de type `String`, défaut `"3"`, **première tentative comprise** (doc : « The first attempt counts against `maxAttempts` ») ; l'annotation est `@BackOff` (et non `@Backoff`), avec `delayString` / `multiplierString` (et non `…Expression`).
- `delayString` : `${prop:défaut}` puis SpEL résolus, unité en ms, prioritaire sur `delay` ; `multiplierString` lu par `Double.valueOf` (**constaté**, bytecode). **Constaté** (test) : les propriétés `500` et `1` sont prises en compte. Défauts de production : 5000 ms et 2.
- `numPartitions = "3"` : doc : « By default the topics are autocreated with one partition » et republication « using the original partition of the received record ». **Constaté** : topics à 3 partitions, même partition (2) sur toute la chaîne.
- `kafkaTemplate` : nom explicite ; sinon `defaultRetryTopicKafkaTemplate`, puis l'unique `KafkaOperations` du contexte (bytecode ; la doc ne mentionne que l'exception).

## 6. Topologie
- **Constaté** (`delay=500`, `multiplier=1`) : exactement 3 topics `cotisations.retry-demo*` à 3 partitions : `cotisations.retry-demo`, `cotisations.retry-demo-retry`, `cotisations.retry-demo-dlt`. Le message en échec passe **deux fois** par l'unique topic de retry (offsets 0 et 1).
- Cause : `SameIntervalTopicReuseStrategy.SINGLE_TOPIC` par défaut (défaut décodé du jar ; doc 4.1 : « the default behavior is to reuse a single retry topic for the same delay intervals »). Avec un seul topic de retry, le suffixe est le `-retry` nu (bytecode de `getTopicSuffix`, lu avant le test et confirmé par lui).
- **`multiplier=2` (production)** : passages par `retry-5000` puis `retry-10000`, **relevés** en démo live (section 13), comme le laissaient prévoir l'exemple de la doc (1000, 2000, 4000 donnent `-retry-1000`, `-retry-2000`, `-retry-4000`) et le bytecode. Le nombre total de topics (4) reste **déduit** ; aucun test automatisé ne tourne avec ce multiplicateur.
- **Groupes suffixés (constaté)** : `retry-demo-group` (principal), `retry-demo-group-retry`, `retry-demo-group-dlt` ; offsets commités 1, 2 et 1 (`SuffixingRetryTopicNamesProvider.getGroupId` ajoute le suffixe du topic, bytecode).

## 7. En-têtes
- **Constaté** (message au DLT de la démo) : `kafka_original-topic` (= `cotisations.retry-demo`), `kafka_original-partition`, `-offset`, `-timestamp`, `-timestamp-type`, `kafka_dlt-original-consumer-group`, `kafka_exception-fqcn` (= `ListenerExecutionFailedException`), `kafka_exception-cause-fqcn` (= `java.lang.RuntimeException`), `kafka_exception-message`, `kafka_exception-stacktrace`, et `retry_topic-original-timestamp`, `retry_topic-attempts`, `retry_topic-backoff-timestamp` en **trois exemplaires** (un par étape).
- **Différence avec le DLT de la source** : celui du `DefaultErrorHandler` porte `kafka_dlt-*` ([phase-3-message-invalide.md](phase-3-message-invalide.md)). J'avais supposé `kafka_dlt-exception-cause-fqcn` : l'assertion a échoué, et les en-têtes ont été relevés avant correction.
- **Non vérifié** : la valeur décodée de `retry_topic-attempts` et de `retry_topic-backoff-timestamp`.

## 8. Délais
- **Garantie de la doc** : « It is guaranteed that a message will never be processed before its due time. » Précision « best-effort », dépendante du `pollTimeout`. L'ancrage du délai (instant de l'échec ou horodatage d'origine) n'est **pas dit** par les pages lues.
- **Constaté** (3 exécutions) : écart entre les deux passages du topic de retry : **529 ms**, **590 ms**, et **25 ms** quand le message d'origine avait attendu 5,3 s avant son premier échec (démarrage à froid). Écart dernier retry → DLT : 1117, 1125 et 1111 ms pour un délai de 500 ms.
- **Déduit, non vérifié** : les 25 ms sont compatibles avec un délai ancré sur l'horodatage d'origine ; les ~1,1 s avec la précision best-effort. Aucun test ne lève l'ambiguïté.
- **Assertion retenue** : le second passage est publié au moins 500 − 100 ms après le message d'origine, vrai dans les deux cas. Les écarts entre étapes ne sont pas assertés.
- **Relevé en démo live** (broker 4.4.0, délais de production) : écarts de 5 s puis 10 s entre les passages (section 13). Cela ne tranche pas l'ancrage : si le message d'origine est traité sans attente, les deux hypothèses donnent les mêmes écarts (**déduit**).

## 9. Fabrique dédiée
- `@RetryableTopic` n'exige pas de fabrique dédiée : par défaut il utilise celle du `@KafkaListener` (doc `retry-topic-lcf.html`), qu'il décore (bytecode). **Déduit** pour notre projet : la fabrique par défaut n'a jamais servi à un `@RetryableTopic` ici.
- **La fabrique dédiée vient du payload** : la fabrique par défaut désérialise en `CotisationEmise` (**constaté**, `application.yml` : `ErrorHandlingDeserializer` puis `JacksonJsonDeserializer`) et `simulerEchec` n'est pas dans le contrat. `retryDemoContainerFactory` lit des `byte[]`.
- Un `@KafkaListener(properties = "value.deserializer=…")` ne conviendrait probablement pas : les endpoints de retry et de DLT sont de nouveaux `MethodKafkaListenerEndpoint` qui ne reçoivent que `id`, `groupId`, `topics`, `clientIdPrefix`, `group`, `bean`, `autoStartup`… (**lu dans le bytecode, non exécuté**).
- **Constaté** : les octets du payload sont identiques sur les quatre passages (principal, 2 retries, DLT).

## 10. Tests
- **`RetryDemoIntegrationTest`** (4 tests, `@EmbeddedKafka`, `actif=true`, délai 500 ms, multiplicateur 1) : (1) un message sans échec est traité une fois, sans retry ni DLT ; (2) un échec simulé passe par le topic principal, deux fois par le topic de retry et une fois par le DLT (4 offsets commités) ; (3) les topics ont les noms attendus, ni plus ni moins, et 3 partitions ; (4) cohabitation : un message illisible va à `cotisations.emises-dlt` avec ses octets, un échec simulé à `cotisations.retry-demo-dlt`, rien ne se croise.
- **Technique** : ni log ni mock ; contenu des topics, horodatages et offsets commités des groupes préfixés `retry-demo-group`. « Un seul appel du `@DltHandler` » est **déduit** (un message au DLT, offset commité) : il n'y a pas de compteur d'appels.
- **Stabilisation** : les `get(5, SECONDS)` d'appels admin deviennent `TIMEOUT_ADMIN` (30 s) dans `PersistanceIntegrationTest`, `MessageInvalideIntegrationTest` et `RetryDemoIntegrationTest`. **Constaté** : une `TimeoutException` à la ligne 233 de `PersistanceIntegrationTest` sur 2 builds complets, alors que ce test passait seul et que le `HEAD` précédent passait. **Non vérifié** : la disparition de l'aléa (un seul build complet vert depuis) et la cause de la lenteur de la machine.
- **Résultat** : classe de la démo 4/4 en ordre aléatoire demandé (graine 42, ordre effectif non relevé). Durée de la classe : 17,5 à 57 s selon la charge.

## 11. `DefaultErrorHandler` et `@RetryableTopic`
| | `DefaultErrorHandler` (source) | `@RetryableTopic` (démo) |
|---|---|---|
| Retries | Bloquants, sur la même partition (**constaté**, 10 livraisons) | Non bloquants, sur des topics de retry (**constaté**) |
| Partition pendant les réessais | Occupée (**déduit** avec pause) | Libre (**déduit**) |
| Ordre par clé | Conservé (**déduit**) | Non garanti (**relevé** en démo live, section 13) |
| Délai entre tentatives | Aucun par défaut (**constaté**) | Par topic, `@BackOff` (**constaté**) |
| Topics créés | Un DLT `<topic>-dlt` | Topics de retry + DLT (**constaté** avec `multiplier=1`) |
| Groupes | Un | Un par topic, suffixés (**constaté**) |
| En-têtes | `kafka_dlt-*` | `kafka_exception-*`, `kafka_original-*`, `retry_topic-*` |
| Erreurs fatales | `DeserializationException` va au DLT sans réessai (**constaté**) | Même liste globale (doc, `dlt-strategies.html`) |
| Contraintes | Aucune | Pas de listener batch, pas de transactions de container (doc) |
| Cas d'usage (**déduit**) | Erreurs brèves, ordre à préserver | Pannes de plusieurs secondes sans bloquer le flux, ordre non requis |

## 12. Ce qui est constaté, déduit, non vérifié
| | Constaté | Déduit | Non vérifié |
|---|---|---|---|
| Cohabitation | Démo active : le message illisible va au DLT de la source, l'échec simulé au DLT de la démo ; groupes séparés | | Même topic source pour les deux mécanismes |
| Fabrique | Bytecode : décorateur, aucun setter sur la fabrique ; octets conservés | `@RetryableTopic` fonctionnerait sur la fabrique par défaut | Essai sur la fabrique par défaut ; `properties` non propagées (lu, non exécuté) |
| Topologie | `multiplier=1` : 3 topics, un retry réutilisé, groupes suffixés ; `multiplier=2` : passages par `retry-5000` puis `retry-10000` (relevé en démo live) | Nombre total de topics avec `multiplier=2` (4) | Test automatisé avec `multiplier=2` |
| En-têtes | Liste relevée (section 7) | | Valeurs de `retry_topic-attempts` et `backoff-timestamp` |
| Délais | Au moins 500 ms après l'origine ; écarts relevés (section 8) | Ancrage sur l'origine ; ~1,1 s = précision best-effort | Ancrage exact du délai |
| Ordre par clé | Relevé en démo live : B traité 11 s avant la fin du parcours de A (même clé) | Perdu avec les retries non bloquants | Test automatisé d'ordre |
| `@DltHandler` | Un message au DLT, offset commité | Un seul appel | Compteur d'appels |
| Broker | Tests automatisés sur le broker embarqué 4.2.2 ; démos live relevées sur le broker 4.4.0 du compose | | Tests automatisés sur le broker 4.4.0 |
| Stabilité | `TimeoutException` de 5 s sur 2 builds | Aléa de charge | Disparition avec 30 s |

## 13. Démos live (broker réel 4.4.0)
Exécutées par l'utilisateur sur le broker `apache/kafka:4.4.0` du compose, démo active (`--prelevement.kafka.retry-demo.actif=true`), délais de production (5 s, multiplicateur 2). **Tout ce qui suit est un relevé de l'utilisateur** (heures locales des logs) : je n'ai pas rejoué les services, et les logs bruts ne sont pas reproduits ici.

### Démo 1 : succès, puis échec avec retries et DLT
**Message succès (`CLE-OK`)** : traité immédiatement, aucun retry.

**Message échec (`CLE-FAIL`, `simulerEchec=true`)** :
- 08:49:07 : réception sur le topic principal, échec, republication vers `retry-5000` ;
- 08:49:12 : réception sur `retry-5000` (+5 s), échec, republication vers `retry-10000` ;
- 08:49:22 : réception sur `retry-10000` (+10 s), échec, republication vers le DLT ;
- 08:49:22 : `@DltHandler` exécuté (WARN).

**Constaté (relevé)** : backoff exponentiel ×2 respecté (5 s puis 10 s), 3 tentatives puis DLT.

### Démo 2 : perte d'ordre par clé (`MEME-CLE`)
Deux messages de même clé : A (`DEMO-A`, `simulerEchec=true`) puis B (`DEMO-B`, `simulerEchec=false`).
- 08:54:20 : A reçu (offset 1), échec, republication vers `retry-5000` ;
- 08:54:24 : B reçu (offset 2), traité avec succès, **avant** la fin des retries de A ;
- 08:54:25 : A sur `retry-5000`, échec, republication vers `retry-10000` ;
- 08:54:35 : A sur `retry-10000`, échec, republication vers le DLT ;
- 08:54:36 : A au DLT, échec définitif.

**Constaté (relevé)** : B (offset 2) est traité 11 s avant la fin du parcours de A (offset 1) : la garantie d'ordre par clé est perdue avec `@RetryableTopic`. Le compromis « meilleur débit contre perte d'ordre » reste **déduit** : le débit n'a pas été mesuré.

### Ce que ces démos ne prouvent pas
- Aucun test automatisé : les relevés ne sont pas rejouables (le test d'ordre reste à écrire).
- L'ancrage du délai (instant de l'échec ou horodatage d'origine) n'est pas tranché : si le message d'origine est traité sans attente, les deux hypothèses donnent les mêmes écarts (**déduit**).

## 14. Prochaines étapes
- Démos live de l'étude 1 : rejeu après `--reset-offsets`, panne de la base avec retries puis DLT ([phase-3-idempotence.md](phase-3-idempotence.md), section 8).
