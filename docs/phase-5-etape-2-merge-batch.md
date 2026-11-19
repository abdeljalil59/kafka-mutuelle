# Phase 5, étape 2 : `MERGE` Oracle idempotent et écriture par lot

Environnement (19/11/2026, heure locale +01:00) : Spring Boot 4.1.2, Spring Framework 7.0.10 (`spring-jdbc` 7.0.10), `spring-kafka` 4.1.2, `ojdbc17` 23.26.3.0.0, image `gvenzl/oracle-free:23.26.4-slim-faststart`. Versions : voir [versions.md](versions.md). Étape précédente (Oracle, Testcontainers) : [phase-5-etape-1-oracle.md](phase-5-etape-1-oracle.md).

Pour chaque point : ce qui est **constaté** (test, sortie réelle, source ou doc lue), **déduit** et **non vérifié**. Les informations sur l'Oracle local viennent de l'utilisateur : je ne m'y suis jamais connecté.

## 1. Objectif et périmètre
- Remplacer l'idempotence par exception (`INSERT` puis capture de `DuplicateKeyException`) par un `MERGE INTO … WHEN NOT MATCHED THEN INSERT` : le doublon est ignoré silencieusement, sans exception ni log.
- Écrire en **un seul appel JDBC batché** les décisions d'un même poll Kafka (`max.poll.records` = 500 d'après l'énoncé de l'utilisateur ; la valeur effective n'a pas été relue dans cette session).
- Périmètre : `prelevement-service` seul. Inchangés : `ErreursKafkaConfig`, le code Avro, `RetryDemoListener`, `cotisation-service` (H2).
- **Numérotation** : « phase 5, étape 2 » est le nom de travail (la phase 5 du brief est `kafka-lab`).
- **Commit** : `88b9ac2` (11 fichiers, 566 ajouts, 100 suppressions), local au moment de l'écriture.

## 2. Avant et après

**Avant** (`JournalDecisionsJdbc`, `JdbcClient`, une décision par appel) :
```java
try {
    jdbcClient.sql("INSERT INTO decision_prelevement (id_evenement, id_adherent, montant, decision, decide_le) VALUES (?, ?, ?, ?, ?)")
            .param(...).update();
} catch (DuplicateKeyException e) {
    LOGGER.info("doublon ignoré idEvenement={}", decision.idEvenement());
}
```
L'unicité était tranchée par la clé primaire (ORA-00001 traduit en `DuplicateKeyException`) et l'exception servait de contrôle de flux.

**Après** (`JdbcTemplate.batchUpdate`, une liste par appel) :
```sql
MERGE INTO decision_prelevement cible
USING (SELECT ? AS id_evenement, ? AS id_adherent, ? AS montant, ? AS decision, ? AS decide_le FROM DUAL) source
ON (cible.id_evenement = source.id_evenement)
WHEN NOT MATCHED THEN INSERT (id_evenement, id_adherent, montant, decision, decide_le)
VALUES (source.id_evenement, source.id_adherent, source.montant, source.decision, source.decide_le)
```
Pas de `WHEN MATCHED` : une ligne existante n'est jamais modifiée (la première valeur gagne, comme avant). Le port devient `JournalDecisions.enregistrer(List<DecisionPrelevement>)`.

## 3. Écriture par lot : `JdbcClient` n'a pas de batch
- **Constaté** (`javap` sur `spring-jdbc-7.0.10.jar`) : `JdbcClient.StatementSpec` expose `param/params/paramSource`, `query`, `update` ; aucune méthode de lot, et aucune classe `Batch` dans `org.springframework.jdbc.core.simple`.
- **Choix validé par l'utilisateur** : `JdbcTemplate.batchUpdate(String, List<Object[]>)` dans l'adaptateur seul. `PrelevementConfig` injecte le bean `JdbcTemplate` de l'autoconfiguration (**constaté** : les tests de contexte passent). `JdbcClient` reste utilisé par les tests pour lire la table.
- **Constaté** : les paramètres `String`, `BigDecimal` et `OffsetDateTime` (UTC, voir l'étape 1 pour `Instant`) passent dans le lot sans type explicite (`JournalDecisionsJdbcTest`, 8/8).
- **Constaté** : une erreur Oracle dans un lot sort en `UncategorizedSQLException` contenant `ORA-12899` (cause `BatchUpdateException`), comme pour l'`INSERT` seul.
- **Limite de la preuve** : l'appel JDBC unique est **constaté par l'expérience hors dépôt** (§5, 500 lignes en 12 à 16 ms), pas par un test du dépôt. Les tests prouvent le contenu de la table, pas le nombre d'allers-retours (Mockito est réservé aux ports ; aucun espion sur `JdbcTemplate`).

## 4. Listener batch et gestion d'erreurs

### Ce que dit la documentation (spring-kafka 4.1.2, pages lues)
Sources : `annotation-error-handling.html`, `listener-annotation.html`, `message-listener-container.html` sur https://docs.spring.io/spring-kafka/reference/ (version 4.1.2 dans l'en-tête des pages). Extraits **constatés** :
- Le mode batch se règle par fabrique (`batchListener`) ou par annotation : « you can override the factory's `batchListener` property using the `batch` property on the `@KafkaListener` annotation ». Le paramètre peut être `List<ConsumerRecord<?, ?>>`.
- `BatchListenerFailedException` : les offsets avant l'index sont validés ; si les réessais ne sont pas épuisés, le message fautif et les suivants sont redélivrés ; une fois épuisés, le recoverer ne traite que le message fautif et les suivants sont redélivrés.
- Toute autre exception d'un listener batch : le **lot entier** est réessayé, puis le recoverer est appelé **pour chaque enregistrement** du lot.
- Erreurs de désérialisation en batch : « manual handling » ; il faut tester `value() == null` et lever `BatchListenerFailedException`.
- Les retries non bloquants (`@RetryableTopic`) ne sont pas supportés avec un listener batch.

### Ce qui est implémenté
- `CotisationEmiseListener.ecouter(List<ConsumerRecord<String, CotisationEmise>>)` avec `@KafkaListener(batch = "true")`. Convertit les messages dans l'ordre jusqu'au premier message **illisible** (`value() == null`) ou **invalide** (`PrelevementInvalideException`), écrit le préfixe valide en un lot, journalise une ligne INFO par message écrit (format inchangé), puis lève `BatchListenerFailedException(index)` pour le message fautif.
- **Message illisible** : `ErrorHandlingDeserializer` laisse la valeur à `null` et l'exception dans l'en-tête ; le listener la relit (`SerializationUtils.getExceptionFromHeader`) et la place en cause, pour que le gestionnaire la classe comme `DeserializationException`.
- `TraiterDemandePrelevement.traiterLot(List<DemandePrelevement>)` : décide chaque demande, appelle le port une fois. Si l'écriture du lot échoue, **repli un par un** (le `MERGE` étant idempotent, rejouer des lignes déjà écrites est sans effet) pour trouver la première décision en échec, puis `EchecTraitementLotException(index)`. Si toutes passent une à une, l'échec du lot était passager et rien n'est levé.
- **Écart au plan validé** : c'est le **listener** (adaptateur d'entrée) qui lève `BatchListenerFailedException`, à partir de `EchecTraitementLotException` ; le cas d'usage n'importe pas spring-kafka (architecture hexagonale légère).
- **Règle ajoutée** : un lot d'une seule décision n'est pas rejoué (il n'y a rien à localiser). Sans elle, un message seul en échec produirait 2 appels du port par livraison au lieu d'un, et le test « 10 livraisons » ne compterait plus des livraisons.
- `ErreursKafkaConfig` : inchangé (comme demandé). Le `RetryDemoListener` garde sa fabrique propre et son mode record (**constaté** : `RetryDemoIntegrationTest` 4/4 sans modification).

## 5. Expérience Oracle jetable (hors dépôt)
Conteneur `gvenzl/oracle-free:23.26.4-slim-faststart` supprimé après usage, utilisateur applicatif `exp`, driver `ojdbc17` 23.26.3.0.0 (seule version dans `~/.m2`), table créée avec le même DDL que le `schema.sql` de production (recopié). Le code de l'expérience (un fichier Java dans le dossier temporaire) n'est **pas versionné** : les résultats ne sont reproductibles qu'en le réécrivant.

| Essai | Résultat **constaté** |
|---|---|
| `CREATE TABLE IF NOT EXISTS` | Accepté. |
| `MERGE` seul `WHEN NOT MATCHED`, même clé deux fois | 1er appel : compte 1 ; doublon : compte **0**, aucune exception ; 1 ligne. |
| Lot JDBC de 5 `MERGE` à une ligne (1 doublon en base, 1 doublon dans le lot) | Comptes `[1, 1, 0, 0, 1]` : doublons ignorés et comptés individuellement ; 4 lignes. |
| Un seul `MERGE` multi-lignes (`UNION ALL`) avec un doublon dans le lot | **ORA-00001** (état SQL 23000), 0 ligne. Option écartée : elle exigerait de dédoublonner en Java. |
| Échec au milieu d'un lot (65 caractères en 3e position sur 5), validation automatique | `BatchUpdateException` ORA-12899, comptes `[1, 1]` : le driver **s'arrête** à la ligne en échec ; les lignes 1 et 2 sont **validées** (lues par une autre session) ; 4 et 5 non exécutées. |
| Même échec, validation automatique coupée | Mêmes comptes ; un `rollback` annule les 2 lignes. |
| Deux sessions, même clé neuve, la première n'a pas validé | La seconde **attend** (environ 1,5 s ici) puis reçoit **ORA-00001** : `MERGE` n'élimine pas l'exception en vraie concurrence. |
| 500 lignes, 3 tours (conteneur local, machine chargée) | Un par un : 3109, 3254 et 4623 ms ; lot JDBC : 16, 16 et 12 ms ; 500 lignes comptées 1 sur 500 à chaque tour. |

- **Déduit** : le gain vient des allers-retours réseau et de l'analyse du SQL évités ; non mesuré séparément.
- **Non vérifié** : la documentation Oracle du `MERGE` n'a pas été relue ; tout ce qui précède vient du comportement du conteneur 23.26.4.

## 6. Découvertes et incidents
- **En-tête DLT en mode batch (constaté)** : pour un message fautif en base ou rejeté par le domaine, `kafka_dlt-exception-cause-fqcn` vaut désormais `org.springframework.kafka.listener.BatchListenerFailedException` (avant : `UncategorizedSQLException` ou `PrelevementInvalideException`). La cause d'origine reste lisible dans `kafka_dlt-exception-stacktrace` (`Caused by: …`). Première exécution : 3 assertions en échec sur ce point (comportement correct par ailleurs : lignes, nombre de rejets et message rejeté). Les tests vérifient maintenant `BatchListenerFailedException` en `cause-fqcn` et la vraie cause dans la trace. Pour un message illisible, l'assertion existante (`kafka_dlt-exception-fqcn` = `DeserializationException`) passe **sans modification**.
- **Logs ERROR (constaté)** : 3 logs `Error handler threw an exception` / `KafkaException: Seek to current after exception` (levé dans `FailedBatchProcessor.seekOrRecover`, trace) par exécution, à l'identique sur les trois exécutions. Ils portent sur un message illisible et sur deux messages en échec Oracle (ORA-12899, index 0 au moment du log, lu dans les causes de la trace) ; aucun pour le montant négatif. **Cause non établie** (hypothèse : redélivrance forcée des messages restants après récupération du fautif) ; l'effet fonctionnel est correct, mais ce bruit au niveau ERROR est à connaître.
- **Verrou de fichier sous Windows (constaté)** : le premier `mvn -B verify` complet a échoué au `repackage` (« Unable to rename … .jar to … .jar.original ») alors que les 69 tests passaient. Cause : un `java -jar prelevement-service-0.1.0-SNAPSHOT.jar` lancé par l'utilisateur tenait le jar. Le build avait écrasé ce jar par le jar fin. Le processus arrêté, le build est repassé sans option de contournement.

## 7. Tests
`prelevement-service` : 53 → 69 tests (+16). Total du dépôt 123 → 139.

| Classe | Avant → après | Changement |
|---|---|---|
| `JournalDecisionsJdbcTest` (Oracle, conteneur) | 4 → 8 | 4 adaptés à la liste (colonnes, doublon **sans exception ni WARN** vérifié sur le logger racine, 2 identifiants, 65 caractères). **Nouveaux** : lot de 500, doublon en base + doublon dans le lot (la première valeur gagne), lot vide, échec au milieu (lignes précédentes écrites, suivantes non). |
| `TraiterDemandePrelevementTest` (port mocké) | 6 → 10 | 5 conservés via `traiterLot` ; l'ancien test d'exception devient « lot d'une décision : pas de rejeu, index 0 ». **Nouveaux** : un seul appel du port et ordre des décisions, lot vide, localisation de la première décision en échec (rejeu une par une, la suivante jamais tentée), échec passager sans erreur. |
| `CotisationEmiseListenerTest` | 4 → 10 | 4 adaptés à la liste (montant négatif : `BatchListenerFailedException` d'index 0). **Nouveaux** : lot de 3 en un appel et journal dans l'ordre, lot vide, préfixe valide écrit avant un message invalide, `value() == null` sans en-tête, `value() == null` avec en-tête (cause `DeserializationException`), échec d'écriture signalé par l'index. |
| `PersistanceIntegrationTest` | 3 → 5 | Espion sur le port en liste ; « 10 livraisons » = 10 appels du port (**constaté**). **Nouveaux** : 5 messages reçus au même poll → **un seul appel du port**, dans l'ordre (conteneur arrêté pendant l'envoi puis redémarré) ; échec au milieu d'un lot : voisins écrits, seul le fautif au DLT. |
| `MessageInvalideIntegrationTest` | 2 → 2 | Assertion d'en-tête adaptée (§6). |
| `CotisationEmiseListenerIntegrationTest`, `RetryDemoIntegrationTest`, `ErreursKafkaConfigTest` | inchangés | Passent sans modification. |

**Constaté** : build complet `mvn -B verify`, tous modules, sans option de contournement, `BUILD SUCCESS` en 7 min 36 s (16 `events-contract`, 54 `cotisation-service`, 69 `prelevement-service`), jar exécutable reconstruit. Trois exécutions au total : (1) 3 échecs d'assertion d'en-tête, (2) échec du `repackage` (verrou), (3) vert.

## 8. Ce qui est constaté, déduit, non vérifié
| | Constaté | Déduit | Non vérifié |
|---|---|---|---|
| `MERGE` | Doublon : compte 0, pas d'exception ; doublons du lot et de la base ignorés ; première valeur conservée | | Doc Oracle du `MERGE` non relue |
| Batch JDBC | `batchUpdate` accepte les types utilisés ; 500 lignes : 12-16 ms contre 3-4,6 s | Gain dû aux allers-retours évités | Un seul aller-retour par lot (aucun test du dépôt) ; lots plus gros que 500 |
| Échec dans un lot | Le driver s'arrête à la ligne fautive ; précédentes validées en autocommit | Sans effet : le `MERGE` rejoué est idempotent | Comportement avec une transaction englobante (non utilisée) |
| Listener batch | Un lot reçu au même poll = un appel du port ; `BatchListenerFailedException(index)` : voisins écrits, fautif seul au DLT ; 10 livraisons avant DLT | | Réessais d'une `PrelevementInvalideException` et d'un message illisible en mode batch (les tests ne comptent pas les livraisons) |
| Concurrence | Deux sessions, même clé : ORA-00001 pour la seconde (expérience) | Dans le service, un doublon concurrent remonte en `DuplicateKeyException`, est réessayé par le gestionnaire d'erreurs, puis ignoré (la ligne existe) : **comportement différent d'avant**, où le `catch` l'absorbait | Non testé dans le service ; même clé = même partition = un seul consommateur du groupe, hors rééquilibrage |
| Messages sans valeur | `value() == null` avec en-tête : cause `DeserializationException` (test) | | Message sans valeur **et** sans en-tête (tombstone) : cause `null`, classement non établi |
| Journal INFO | Une ligne par message écrit, format inchangé | | Quand l'écriture d'un lot échoue, les messages qui précèdent l'index sont écrits mais **n'ont pas de ligne INFO** (limite assumée, non corrigée) |
| Durée du rejeu | 500 `MERGE` un par un : 3 à 4,6 s (expérience) | Très en deçà du `max.poll.interval.ms` par défaut de Kafka (valeur non relevée ici) | Rejeu sur un gros lot en échec avec une base lente |
| Oracle local | | | Exécution du service sur l'Oracle local de l'utilisateur avec `MERGE` et lots |

## 9. Journaux antérieurs devenus partiellement obsolètes
- [phase-3-idempotence.md](phase-3-idempotence.md) décrit l'idempotence par `INSERT` et capture du doublon, sur H2 : ne vaut plus que pour l'état antérieur de `prelevement-service`.
- [phase-5-etape-1-oracle.md](phase-5-etape-1-oracle.md) : « Inchangés : la logique d'idempotence (`INSERT` puis capture du doublon) » (§1), « Doublon : ORA-00001 est bien traduit en `DuplicateKeyException` » (§5), compte de 53 tests et §9 « Étape 2 prévue » sont ceux de l'état antérieur. Non modifié.

## 10. Suite
Rien de prévu dans le brief pour cette ligne. Pistes à arbitrer : compter les livraisons d'une `PrelevementInvalideException` en mode batch (non vérifié ci-dessus), décider du traitement des ERROR de `seekOrRecover`, tester le service sur l'Oracle local, mettre à jour les journaux obsolètes du §9.
