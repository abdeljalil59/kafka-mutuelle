# Transactions Kafka — Exactly-Once Semantics

> Document pédagogique pour le projet kafka-mutuelle.
> Objectif : comprendre les transactions Kafka et savoir en parler en entretien technique.

## 1. Le problème : pourquoi at-least-once ne suffit pas toujours

Dans un pipeline `read → process → write`, trois sémantiques existent :

| Sémantique | Principe | Risque |
|---|---|---|
| At-most-once | Commit de l'offset avant le traitement | Perte de message si le traitement plante |
| At-least-once | Commit de l'offset après le traitement | Doublon si le commit plante après le traitement |
| Exactly-once | Traitement et commit atomiques | Aucun (dans le périmètre Kafka) |

**Dans kafka-mutuelle aujourd'hui** : le prelevement-service fonctionne en at-least-once. L'idempotence applicative (clé primaire `id_evenement`, l'UUID de l'événement, écriture par `MERGE ... WHEN NOT MATCHED THEN INSERT` : un doublon est ignoré) protège contre les doublons en base. C'est suffisant pour notre cas.

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

**kafka-mutuelle utilise l'approche 1** (idempotence applicative) côté prelevement-service : c'est le choix pragmatique, simple, testable, suffisant quand le coût d'un doublon est faible (un doublon ignoré par le MERGE). **Côté cotisation-service, il utilise aussi l'outbox**, avec un relais planifié et non Debezium : la publication y est at-least-once (voir [phase-3-outbox.md](phase-3-outbox.md)), le doublon éventuel étant absorbé en aval par l'idempotence du consommateur.

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

Le projet n'utilise pas les transactions Kafka (pas de transaction-id-prefix). Voici pourquoi et ce qu'on pourrait ajouter :

**Situation actuelle** :
- prelevement-service lit de `cotisations.emises` et écrit dans Oracle → at-least-once + idempotence applicative (MERGE sur `id_evenement`)
- Le producer de cotisation-service est déjà idempotent (couche 1) : `enable.idempotence = true`, `acks = -1`, « Instantiated an idempotent producer » (log relevé dans [phase-1-producteur.md](phase-1-producteur.md)). Le producer du DLT l'est aussi (`ProducerConfig values` dans [phase-3-message-invalide.md](phase-3-message-invalide.md))
- Le DLT (`cotisations.emises-dlt`) est géré par le `DeadLetterPublishingRecoverer` hors transaction

**Scénario possible pour explorer les transactions** :
- Faire publier par le prelevement-service chaque décision (ACCEPTE/REJETE) dans `prelevements.resultats`, le topic prévu au brief (clé `idAdherent`, 3 partitions, champ `type`), pas encore réalisé
- Activer `transaction-id-prefix` pour que la lecture de `cotisations.emises` et l'écriture dans `prelevements.resultats` soient dans la même transaction
- Configurer `isolation.level=read_committed` sur un consumer de `prelevements.resultats` (l'impaye-service prévu au brief)
- Observer le comportement en cas de crash simulé (kill du service entre le traitement et le commit)

Cela permettrait de configurer et observer : le `transactional.id`, l'`isolation.level`, le fencing de zombie, et le comportement en cas de crash.

**Point d'attention (doc Spring Kafka, non éprouvé dans le projet)** : avec un container transactionnel, la doc renvoie la gestion des enregistrements qui échouent de façon répétée à l'*After-rollback Processor* (« See After-rollback Processor for more information and for handling records that repeatedly fail »), et indique que « When using transactions, similar functionality is provided by the DefaultAfterRollbackProcessor » à la place du `DefaultErrorHandler`. Le `DefaultErrorHandler` actuel (BackOff exponentiel, `BatchListenerFailedException`, DLT) devrait donc être repensé avant d'activer les transactions.

## Sources

- KIP-98 : Exactly Once Delivery and Transactional Messaging (Apache Kafka wiki) — non relu
- KIP-447 : Producer scalability for exactly once semantics (EOSMode.V2) — non relu
- Spring Kafka Reference — Transactions : docs.spring.io/spring-kafka/reference/kafka/transactions.html — lu le 20/11/2026 (la page affichait la version 4.2.0 ; le projet est en 4.1.2)
- Spring Kafka Reference — Exactly Once Semantics : docs.spring.io/spring-kafka/reference/kafka/exactly-once.html — lu le 20/11/2026 (même remarque)
- Apache Kafka 4.4 — Producer Configs et Consumer Configs : kafka.apache.org/44/configuration/producer-configs/ et consumer-configs/ — lus le 20/11/2026
- Conduktor — Exactly-Once Semantics in Kafka : conduktor.io/glossary/exactly-once-semantics-in-kafka — non relu

---

*Classification des affirmations (remplace celle du texte d'origine, qui donnait comme « constaté » des affirmations tirées de sources non relues ici) :*

**Constaté** (lu dans la documentation officielle ou dans le dépôt le 20/11/2026) :
- Spring Kafka : « With Spring Boot, it is only necessary to set the spring.kafka.producer.transaction-id-prefix property - Spring Boot will automatically configure a KafkaTransactionManager bean and wire it into the listener container » (transactions)
- Spring Kafka : « Spring for Apache Kafka version 3.0 and later only supports EOSMode.V2 », qui exige des brokers 2.5 ou plus, sans producer par group.id/topic/partition car les métadonnées du consumer accompagnent les offsets (exactly-once)
- Spring Kafka : le container envoie les offsets par `producer.sendOffsetsToTransaction()` avant le commit ; si le listener lève une exception, « the transaction is rolled back and the consumer is repositioned » (exactly-once)
- Spring Kafka : synchronisation DB + Kafka, « the database transaction will commit followed by the Kafka transaction » ; l'exception d'un commit Kafka qui échoue après celui de la DB est propagée à l'appelant depuis les versions 2.5.17, 2.6.12, 2.7.9 et 2.8.0 (transactions)
- Kafka 4.4 : `enable.idempotence` vaut `true` par défaut et exige `max.in.flight.requests.per.connection` ≤ 5 (défaut 5) ; `retries` vaut 2147483647 par défaut ; sans `transactional.id` (défaut `null`), le producer est limité à la livraison idempotente ; `isolation.level` vaut `read_uncommitted` par défaut, et les messages non transactionnels sont renvoyés dans les deux modes
- Dépôt : aucune configuration transactionnelle (`transaction-id-prefix`, `KafkaTransactionManager`, `isolation.level`, `read_committed` absents) ; clé d'idempotence `id_evenement` (aucun `dateEcheance` n'existe) ; producers de cotisation-service et du DLT idempotents (journaux des phases 1 et 3)

**Déduit** (cohérent avec les sources mais non vérifié textuellement) :
- Le coût du producer idempotent seul est quasi nul (aucune source lue ne donne ce chiffre)

**Non vérifié** (non relu dans cette session) :
- Fonctionnement interne : PID et numéro de séquence, transaction coordinator, `__transaction_state`, two-phase commit, marqueurs COMMIT/ABORT, fencing par `transactional.id` (KIP-98 et Conduktor non lus)
- `enable.auto.commit=false` « obligatoire » avec les transactions ; `acks=all` comme défaut et « depuis Kafka 3.0 » pour l'idempotence (les pages de configuration lues donnent les valeurs par défaut, pas la version d'introduction)
- « Ce n'est PAS une transaction distribuée (XA/2PC) » et « coordonnées par le TransactionSynchronizationManager » (la page Spring Kafka lue ne mentionne ni XA ni 2PC)
- Latence +2 à 5 ms et débit -10 à 20 % (Conduktor, sans benchmark cité)
- Que l'outbox avec Debezium donne une « exactly-once effective » (la ligne d'origine du tableau du §5 est conservée telle quelle)
