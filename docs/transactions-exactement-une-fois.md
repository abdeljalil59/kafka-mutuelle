# Transactions Kafka — Exactly-Once Semantics

> Document pédagogique pour le projet kafka-mutuelle.
> Objectif : comprendre les transactions Kafka et savoir en parler en entretien technique.
>
> **Mis à jour le 20/11/2026** : le §1, la fin du §5 et le §8 décrivent l'état réel du projet (transactions activées sur
> `prelevement-service`, preuves d'intégration, `impaye-service`). Les autres sections sont pédagogiques et inchangées.

## 1. Le problème : pourquoi at-least-once ne suffit pas toujours

Dans un pipeline `read → process → write`, trois sémantiques existent :

| Sémantique | Principe | Risque |
|---|---|---|
| At-most-once | Commit de l'offset avant le traitement | Perte de message si le traitement plante |
| At-least-once | Commit de l'offset après le traitement | Doublon si le commit plante après le traitement |
| Exactly-once | Traitement et commit atomiques | Aucun (dans le périmètre Kafka) |

**Dans kafka-mutuelle aujourd'hui** : `prelevement-service` lit `cotisations.emises`, écrit la décision dans Oracle puis publie le résultat sur `prelevements.resultats`, **dans une transaction Kafka** (§8) : pour le passage d'un topic à l'autre, la lecture, le résultat et l'offset sont atomiques. **Vers Oracle, l'écriture reste at-least-once** : Oracle ne participe pas à la transaction Kafka. C'est l'idempotence applicative (clé primaire `id_evenement`, l'UUID de l'événement, écriture par `MERGE ... WHEN NOT MATCHED THEN INSERT` : un doublon est ignoré) qui protège contre les doublons en base. `cotisation-service` publie en at-least-once (outbox).

Mais l'idempotence applicative a des limites :
- Elle doit être codée pour chaque opération (contrainte UNIQUE, MERGE)
- Elle ne protège que l'écriture en base, pas un envoi vers un autre topic
- Elle ne gère pas les side effects (envoi d'email, appel REST)

## 2. Les trois couches d'exactly-once dans Kafka

### Couche 1 — Producer idempotent (KIP-98, Kafka 0.11)

Le broker attribue un **Producer ID (PID)** unique au démarrage du producer. Chaque message porte un **numéro de séquence** par partition. Si le producer retente un envoi (retry réseau), le broker détecte le numéro de séquence déjà vu et acquitte sans réécrire.

Configuration :
```yaml
# Défaut depuis Kafka 3.0
enable.idempotence: true   # active le PID + séquence
acks: all                  # acquittement de tous les réplicas
retries: 2147483647        # MAX_VALUE
max.in.flight.requests.per.connection: 5  # max pour garder l'idempotence
```

**Ce que ça résout** : les doublons causés par les retries du producer vers le broker (leg 1 : producer → broker).

**Ce que ça ne résout pas** : un consumer qui relit un message après un crash (leg 2 : broker → consumer → traitement).

### Couche 2 — Transactions (KIP-98, Kafka 0.11)

Les transactions étendent l'exactly-once au pipeline complet **read → process → write** à l'intérieur de Kafka.

Principe :
1. Le consumer lit un message du topic A
2. Le producer ouvre une transaction (`beginTransaction()`)
3. Le producer envoie le résultat dans le topic B
4. Le producer envoie l'offset du message lu (`sendOffsetsToTransaction()`)
5. Le producer commit la transaction (`commitTransaction()`)

Si un crash survient avant l'étape 5, Kafka annule tout : pas de message dans B, pas d'offset avancé. Au redémarrage, le consumer relit le message et recommence.

**Mécanisme interne** :
- Un **Transaction Coordinator** (dans le broker) gère le cycle de vie de la transaction
- L'état est persisté dans le topic interne `__transaction_state`
- Le `transactional.id` (configuré sur le producer) permet au coordinator de **fencer** les anciens producers zombies : si un producer avec le même transactional.id redémarre, l'ancien est bloqué
- Le coordinator utilise un **protocole two-phase commit** : tous les participants commitent ou avortent ensemble
- Des **marqueurs de transaction** (COMMIT ou ABORT) sont écrits dans les partitions concernées

### Couche 3 — Consumer read_committed

Côté consumer en aval, `isolation.level=read_committed` fait que le consumer ne voit que les messages dont la transaction est commitée. Les messages en cours ou avortés sont filtrés.

Configuration consumer :
```yaml
isolation.level: read_committed    # ne lit que les transactions commitées
enable.auto.commit: false          # obligatoire avec les transactions
```

## 3. Exactly-once dans Spring Kafka

Spring Kafka supporte les transactions (bibliothèque cliente Kafka 0.11.0.0 et suivantes). La configuration est simple :

### Activation (Spring Boot)

Une seule propriété suffit :
```yaml
spring:
  kafka:
    producer:
      transaction-id-prefix: tx-mutuelle-
```

Spring Boot configure automatiquement un `KafkaTransactionManager` et le câble dans le listener container.

### Comportement du listener container

Quand un `KafkaTransactionManager` est configuré :
1. Le container **ouvre une transaction** avant d'appeler le listener
2. Les opérations `KafkaTemplate` dans le listener **participent** à la transaction
3. Si le listener réussit, le container envoie les offsets via `producer.sendOffsetsToTransaction()` puis commit
4. Si le listener lève une exception, la transaction est **rollback** et le consumer est repositionné pour relire le message

### EOSMode.V2 (depuis Spring Kafka 3.0)

Spring Kafka 3.0+ ne supporte que EOSMode.V2 (KIP-447, brokers ≥ 2.5) :
- Plus besoin d'un producer par group.id/topic/partition
- Le consumer envoie ses métadonnées avec les offsets, et le broker les utilise pour détecter le fencing
- Réduit fortement le nombre de producers transactionnels nécessaires

### Synchronisation avec une base de données

Pour le cas Kafka + base de données (notre cas avec Oracle), Spring propose la synchronisation de transactions :

```java
@Transactional  // démarre la transaction DB (DataSourceTransactionManager)
public void process(List<Thing> things) {
    things.forEach(thing -> kafkaTemplate.send("topic", thing));
    updateDb(things);  // écriture en base
}
// À la sortie : DB commit d'abord, puis Kafka commit
```

**Attention** : ce n'est PAS une transaction distribuée (XA/2PC). Les deux transactions sont indépendantes, coordonnées par le `TransactionSynchronizationManager` de Spring. Si le commit Kafka échoue après le commit DB, la DB est commitée mais pas Kafka. Depuis Spring Kafka 2.8, cette exception est propagée à l'appelant.

## 4. Périmètre de la garantie : ce que exactly-once couvre et ne couvre pas

| Scénario | Exactly-once garanti ? | Raison |
|---|---|---|
| Topic A → traitement → Topic B | ✅ Oui | Tout est dans Kafka, même transaction |
| Topic A → traitement → Base Oracle | ❌ Non (at-least-once) | Oracle ne participe pas à la transaction Kafka |
| Topic A → traitement → Topic B + Oracle | ⚠️ Partiel | Exactly-once pour Topic B, at-least-once pour Oracle |
| Topic A → traitement → appel REST | ❌ Non | Le REST n'est pas annulable |

**Règle d'or** : la transaction Kafka est **entre topics Kafka**. Elle garantit l'atomicité entre « lire du topic A » et « écrire dans le topic B + commiter l'offset de A ». Tout ce qui est hors Kafka (base, API, fichier) ne participe pas.

## 5. Les alternatives pour le cas Kafka + base de données

| Approche | Principe | Complexité | Garantie |
|---|---|---|---|
| Idempotence applicative | Clé naturelle, contrainte UNIQUE (INSERT ... ON CONFLICT, ou MERGE sur Oracle) | Faible | At-least-once + dédoublonnage |
| Outbox pattern | Écriture dans une table outbox, Debezium publie vers Kafka (ou un relais planifié, comme dans cotisation-service) | Moyenne | Exactly-once effective (via CDC) |
| Synchronisation Spring | @Transactional + KafkaTransactionManager | Faible | Best-effort (fenêtre de risque au commit) |
| XA / 2PC | Transaction distribuée JTA | Élevée | Exactly-once théorique (mais fragile et lent) |

**kafka-mutuelle combine deux approches côté prelevement-service** : l'idempotence applicative (approche 1) pour l'écriture dans Oracle, et la transaction Kafka pour le passage de `cotisations.emises` à `prelevements.resultats` (§8). L'idempotence est le choix pragmatique, simple, testable, suffisant quand le coût d'un doublon est faible (un doublon ignoré par le MERGE). **Côté cotisation-service, il utilise aussi l'outbox**, avec un relais planifié et non Debezium : la publication y est at-least-once (voir [phase-3-outbox.md](phase-3-outbox.md)), le doublon éventuel étant absorbé en aval par l'idempotence du consommateur.

## 6. Coût de performance des transactions

Chiffres approximatifs (source : Conduktor, non benchmarkés) :
- **Latence** : +2 à 5 ms par message (coordination du transaction coordinator)
- **Débit** : -10 à 20% par rapport à at-least-once
- **Broker** : charge supplémentaire pour le coordinator, espace disque pour `__transaction_state`

Le producer idempotent seul (sans transaction) a un coût quasi nul.

## 7. Ce qu'il faut savoir dire en entretien

**Question type** : « Comment garantir exactly-once dans Kafka ? »

**Bonne réponse structurée** :

1. **Couche 1** : le producer idempotent élimine les doublons broker-side (PID + séquence). C'est le défaut depuis Kafka 3.0, quasi gratuit.

2. **Couche 2** : les transactions englobent read + write + offset commit dans une opération atomique. Le transaction coordinator persiste l'état dans `__transaction_state`, et le `transactional.id` fence les zombies.

3. **Couche 3** : les consumers en aval doivent lire en `read_committed` pour ne voir que les messages des transactions commitées.

4. **Limite** : exactly-once ne couvre que le périmètre Kafka (topic → topic). Pour Kafka → base de données, il faut soit de l'idempotence applicative, soit le pattern outbox, soit une synchronisation de transactions Spring (best-effort).

5. **Choix pragmatique** : on choisit la garantie en fonction du coût du doublon vs le coût de la complexité. L'idempotence applicative est souvent suffisante et bien plus simple.

**Piège classique** : « Exactly-once dans Kafka signifie qu'un message est délivré une seule fois. » → Faux. Le message peut être lu plusieurs fois (at-least-once sur le read). C'est la **séquence** read-process-write qui se produit exactly-once du point de vue de l'effet observable.

## 8. Application au projet kafka-mutuelle

État au 20/11/2026. Le « scénario possible » que ce guide proposait est réalisé, avec les écarts décrits plus bas.

### Ce qui est en place (constaté : code et tests du dépôt)
| Élément | Réalisation |
|---|---|
| Résultat publié | `prelevement-service` publie chaque décision (ACCEPTE ou REJETE) sur `prelevements.resultats` : clé `idAdherent`, Avro `ResultatPrelevement`, 3 partitions (commits `9a6a0ca`, `8010336`, `c701c19`). Un message rejoué republie la même décision |
| Transactions | `spring.kafka.producer.transaction-id-prefix: ${KAFKA_TRANSACTION_ID_PREFIX:tx-prelevement-}` : Boot configure le `KafkaTransactionManager`. Une transaction par message (listener en mode record) : lecture, résultat et offset (commit `3f78c42`). Le préfixe doit être **unique par instance** : deux instances de même préfixe se fencent |
| Isolation | `isolation-level: read-committed` sur le consommateur de `prelevement-service`, et sur celui d'`impaye-service` (qui lit `prelevements.resultats`) |
| Erreurs | `DefaultAfterRollbackProcessor` à la place du `DefaultErrorHandler` : même BackOff exponentiel (1, 2, 4, 8 s, 5 livraisons), même `DeadLetterPublishingRecoverer` vers `cotisations.emises-dlt`, `commitRecovered=true` avec le `kafkaTemplate` |
| Oracle | Hors de la transaction Kafka, comme prévu au §4 : le `MERGE` est idempotent, et la ligne survit à l'annulation d'une transaction (prouvé, ci-dessous) |
| Démo `@RetryableTopic` | Ses containers ne sont pas transactionnels mais utilisent le `kafkaTemplate` de Boot, devenu transactionnel : sans `spring.kafka.template.allow-non-transactional: true` (D8), 3 tests sur 4 de `RetryDemoIntegrationTest` échouent (`No transaction is in process`, relevé en boucle). Cette option ne concerne que le template de Boot : le template des résultats reste strictement transactionnel |
| `impaye-service` | Consomme en `read_committed`, **sans transaction Kafka** : il ne publie qu'une relance, hors transaction, après l'écriture en base. Les deux ne sont pas atomiques : un drapeau `relance_emise` en base (publier d'abord, marquer ensuite) fait rattraper une publication ratée au réessai ou au rejet suivant, et le pire cas devient une relance en double (jamais perdue) |

### Pourquoi le listener est en mode record (constaté : `TransactionsKafkaLabTest`, `281494c`)
Le point d'attention du guide d'origine (la doc renvoie à l'*After-rollback Processor*) s'est confirmé, et plus durement que prévu. Sur le broker embarqué, avec un `DefaultAfterRollbackProcessor` : un listener **batch** qui échoue voit **le lot entier redélivré indéfiniment**, sans que le recoverer soit jamais appelé et sans offset validé, y compris avec `BatchListenerFailedException(index)` et avec `commitRecovered=false` ; un listener **record** isole le message fautif (il est redélivré, puis récupéré). Le bytecode de `DefaultAfterRollbackProcessor` ne référence pas `BatchListenerFailedException`. D'où le passage du listener de `prelevement-service` du mode batch au mode record (commit `8010336`), au prix de l'écriture par lot (le `JdbcTemplate.batchUpdate` reste, avec une décision par appel).

### Preuves d'intégration (constaté : `ExactlyOnceIntegrationTest`, `9df0a31`, vrai code, broker embarqué)
1. **Commit** : le résultat ACCEPTE est visible en `read_committed`, l'offset de la source est dépassé, un seul envoi, aucune livraison échouée, une ligne Oracle.
2. **Abort** : un échec **après** l'envoi (le broker a bien écrit le message) et avant le commit annule la transaction : résultat invisible en `read_committed`, offset non validé, ligne Oracle conservée.
3. **Isolation** : sur le même scénario, un lecteur `read_uncommitted` voit le résultat annulé et un lecteur `read_committed` ne le voit pas, alors que le message est avant le LSO.
4. **Redélivrance** : un échec transitoire est redélivré (seek) : un seul résultat visible, une seule ligne ; le journal contient pourtant deux messages (l'annulé et le validé), cachés par l'isolation.
5. **DLT** : 5 livraisons sous transaction, 5 résultats écrits puis annulés (aucun visible), puis le message part au DLT avec son topic, sa partition et son offset d'origine, et l'offset de la source est validé : pas de sixième livraison.

**Méthode d'une absence** : en `read_committed`, un message qui suit une transaction ouverte est retenu jusqu'à sa fin ; constater qu'il est absent avant ne prouve rien. Les tests attendent que le LSO (`endOffsets` d'un consommateur `read_committed`) dépasse l'offset du message : sa transaction est alors terminée, et son absence est une preuve. **Constaté** dans les sources de kafka-clients 4.2.1 (le projet est en 4.2.2) : pour un consommateur `read_committed`, la fin est le LSO, « le minimum du high watermark et du plus petit offset de toute transaction ouverte ».

**`impaye-service`** : `ImpayeIntegrationTest` publie dans des transactions : un REJETE d'une transaction **annulée** n'est pas persisté. **Témoin négatif constaté** : avec le consommateur forcé en `read_uncommitted`, ce test, et lui seul, échoue (ligne persistée), donc la propriété `read-committed` est bien ce qui protège.

**Header de tentatives** : `kafka_deliveryAttempt` n'est pas posé sur le message du DLT (**constaté** : `ContainerProperties.deliveryAttemptHeader` vaut `false` par défaut, aucune propriété Boot ne l'expose, rien dans le service ne l'active). Le nombre de tentatives se prouve par le `RetryListener` (5) et par les 5 envois, pas par un en-tête.

### Supervision (constaté : capture réelle de `/actuator/prometheus` sous transactions)
Le producer expose `kafka_producer_txn_{init,begin,send_offsets,commit,abort,prepare}_time_ns_total` : des **cumuls de temps en nanosecondes** (« Total time producer has spent in commitTransaction in nanoseconds »), jamais un nombre de commits ou d'aborts ; les noms `kafka_producer_txn_commit_total` et `_abort_total` n'existent pas. Le dashboard Grafana (ligne « Transactions ») compte donc les traitements validés et annulés à partir du timer du listener, `result=success` ou `failure` : un échec du listener annule la transaction (**déduit** de la sémantique de Spring Kafka, prouvé par les tests ci-dessus ; ce n'est pas lu sur un compteur de transactions).

### Ce qui n'est pas prouvé
- **Les transactions contre le vrai broker 4.4.0 du compose** : les tests utilisent le broker embarqué 4.2.2.
- **Un crash en plein traitement** (arrêt brutal du processus entre l'envoi et le commit) : l'échec est simulé par une exception, pas par un `kill`.
- **L'atomicité de l'envoi au DLT et de la validation de l'offset** : on constate que le message arrive au DLT et que l'offset est validé (et, en test unitaire, que `sendOffsetsToTransaction` est appelé) ; qu'ils forment **une seule** transaction n'est pas observé sur un échec en plein milieu (**déduit** de la doc de `commitRecovered`).
- **Le démarrage à froid** : le premier message d'un contexte transactionnel a mis 16 à 24 s à être traité sur une machine chargée (initialisation du producer transactionnel de 2 à 7 s, puis une première invocation du listener de 8 s, dont 0,05 s d'envoi : **constaté** ; que le reste soit le premier `MERGE` Oracle est **déduit**, non isolé). Tout délai d'attente de test inférieur à 30 s y est fragile.

## Sources

- KIP-98 : Exactly Once Delivery and Transactional Messaging (Apache Kafka wiki) — non relu
- KIP-447 : Producer scalability for exactly once semantics (EOSMode.V2) — non relu
- Spring Kafka Reference — Transactions : docs.spring.io/spring-kafka/reference/kafka/transactions.html — lu le 20/11/2026 (la page affichait la version 4.2.0 ; le projet est en 4.1.2)
- Spring Kafka Reference — Exactly Once Semantics : docs.spring.io/spring-kafka/reference/kafka/exactly-once.html — lu le 20/11/2026 (même remarque)
- Apache Kafka 4.4 — Producer Configs et Consumer Configs : kafka.apache.org/44/configuration/producer-configs/ et consumer-configs/ — lus le 20/11/2026
- Conduktor — Exactly-Once Semantics in Kafka : conduktor.io/glossary/exactly-once-semantics-in-kafka — non relu
- Sources de kafka-clients 4.2.1 (`kafka-clients-4.2.1-sources.jar`, dépôt Maven local ; le projet résout la 4.2.2) : javadoc de `KafkaConsumer.endOffsets` et de `isolation.level`, noms des métriques transactionnelles de `KafkaProducerMetrics` — lus le 20/11/2026
- Dépôt : `TransactionsKafkaLabTest` (`281494c`), `ExactlyOnceIntegrationTest` (`9df0a31`), `ImpayeIntegrationTest` (`f23c741`, `81ff191`) ; capture de `/actuator/prometheus` du 20/11/2026

---

*Classification des affirmations (remplace celle du texte d'origine, qui donnait comme « constaté » des affirmations tirées de sources non relues ici) :*

**Constaté** (lu dans la documentation officielle ou dans le dépôt le 20/11/2026) :
- Spring Kafka : « With Spring Boot, it is only necessary to set the spring.kafka.producer.transaction-id-prefix property - Spring Boot will automatically configure a KafkaTransactionManager bean and wire it into the listener container » (transactions)
- Spring Kafka : « Spring for Apache Kafka version 3.0 and later only supports EOSMode.V2 », qui exige des brokers 2.5 ou plus, sans producer par group.id/topic/partition car les métadonnées du consumer accompagnent les offsets (exactly-once)
- Spring Kafka : le container envoie les offsets par `producer.sendOffsetsToTransaction()` avant le commit ; si le listener lève une exception, « the transaction is rolled back and the consumer is repositioned » (exactly-once)
- Spring Kafka : synchronisation DB + Kafka, « the database transaction will commit followed by the Kafka transaction » ; l'exception d'un commit Kafka qui échoue après celui de la DB est propagée à l'appelant depuis les versions 2.5.17, 2.6.12, 2.7.9 et 2.8.0 (transactions)
- Kafka 4.4 : `enable.idempotence` vaut `true` par défaut et exige `max.in.flight.requests.per.connection` ≤ 5 (défaut 5) ; `retries` vaut 2147483647 par défaut ; sans `transactional.id` (défaut `null`), le producer est limité à la livraison idempotente ; `isolation.level` vaut `read_uncommitted` par défaut, et les messages non transactionnels sont renvoyés dans les deux modes
- Dépôt (20/11/2026) : transactions **activées** sur `prelevement-service` (`transaction-id-prefix`, `isolation-level: read-committed`, `DefaultAfterRollbackProcessor` avec recoverer et `commitRecovered`) ; clé d'idempotence `id_evenement` (aucun `dateEcheance` n'existe) ; producers de cotisation-service et du DLT idempotents (journaux des phases 1 et 3)
- Laboratoire (`TransactionsKafkaLabTest`, broker embarqué avec `transaction.state.log.replication.factor=1` et `min.isr=1`) : commit, abort, fencing d'un producer zombie (`ProducerFencedException`), `read_committed` qui masque l'annulé ; listener batch sous transaction : lot redélivré indéfiniment, recoverer jamais appelé ; listener record : message fautif isolé
- `ExactlyOnceIntegrationTest` : les cinq preuves du §8 ; `ImpayeIntegrationTest` : `read_committed` et son témoin négatif
- kafka-clients 4.2.1 (sources) : `endOffsets` rend le LSO en `read_committed` ; `read_uncommitted` renvoie aussi les messages annulés ; le producer n'expose que des cumuls de temps de transaction (`txn-*-time-ns-total`)
- Boot : `spring.kafka.template.allow-non-transactional` existe (défaut `false`) ; sans elle, la démo `@RetryableTopic` échoue sous transaction

**Déduit** (cohérent avec les sources mais non vérifié textuellement) :
- Le coût du producer idempotent seul est quasi nul (aucune source lue ne donne ce chiffre)
- « Échec du listener = transaction annulée » lu sur le timer du listener pour le dashboard (sémantique Spring Kafka, prouvée par les tests mais non lue sur un compteur de transactions)
- Démarrage à froid : le reste de la première invocation du listener (environ 8 s moins 0,05 s d'envoi) serait le premier `MERGE` Oracle

**Non vérifié** (non relu dans cette session) :
- Fonctionnement interne : PID et numéro de séquence, transaction coordinator, `__transaction_state`, two-phase commit, marqueurs COMMIT/ABORT (KIP-98 et Conduktor non lus). Le fencing par `transactional.id` est, lui, constaté au laboratoire
- Les transactions contre le broker réel 4.4.0 du compose (les tests utilisent le broker embarqué 4.2.2) ; un crash en plein traitement ; l'atomicité de l'envoi au DLT et de la validation de l'offset sur un échec en plein milieu
- `enable.auto.commit=false` « obligatoire » avec les transactions ; `acks=all` comme défaut et « depuis Kafka 3.0 » pour l'idempotence (les pages de configuration lues donnent les valeurs par défaut, pas la version d'introduction)
- « Ce n'est PAS une transaction distribuée (XA/2PC) » et « coordonnées par le TransactionSynchronizationManager » (la page Spring Kafka lue ne mentionne ni XA ni 2PC)
- Latence +2 à 5 ms et débit -10 à 20 % (Conduktor, sans benchmark cité)
- Que l'outbox avec Debezium donne une « exactly-once effective » (la ligne d'origine du tableau du §5 est conservée telle quelle)
