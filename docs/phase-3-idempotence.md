# Phase 3 : écriture idempotente des décisions de prélèvement (`prelevement-service`)

> **Mise à jour (19/11/2026)** : ce journal décrit l'état du 18/11/2026 et n'a pas été réécrit. Depuis :
> - **`prelevement-service` utilise Oracle Free 23ai, plus H2** (phase 5 étape 1, [phase-5-etape-1-oracle.md](phase-5-etape-1-oracle.md)). Les passages sur H2 (environnement, §1, §2, §3, §5, §6, §7 et §8) ne valent plus que pour l'état antérieur ; H2 reste dans `cotisation-service` (outbox). Cause d'un échec de base au DLT : `UncategorizedSQLException` (ORA-12899) au lieu de `DataIntegrityViolationException`.
> - **`cotisations.emises` et le DLT sont en Avro** (phase 4, [phase-4-avro-schema-registry.md](phase-4-avro-schema-registry.md)). Les passages sur le JSON (§6 et §7 : « JSON du `CotisationEmise` » au DLT, format JSON du DLT) sont périmés : un rejet contient un `CotisationEmise` Avro, ou les octets d'origine d'un message illisible.
> - **L'écriture est un `MERGE … WHEN NOT MATCHED`, par lot** (phase 5 étape 2, [phase-5-etape-2-merge-batch.md](phase-5-etape-2-merge-batch.md)). §3 (« INSERT puis capture du doublon », `MERGE` écarté) est périmé ; en mode batch, `kafka_dlt-exception-cause-fqcn` vaut `BatchListenerFailedException`.

Environnement (18/11/2026, heure locale +01:00) : Spring Boot 4.1.2, spring-kafka 4.1.2, kafka-clients 4.2.2, H2 2.4.240 (versions : voir [versions.md](versions.md)). Message illisible et topic de rejets : [phase-3-message-invalide.md](phase-3-message-invalide.md). Consommateur : [phase-1-consommateur.md](phase-1-consommateur.md).

Pour chaque point : ce qui est **constaté** (sortie réelle ou source lue), **déduit** et **non vérifié**.

## 1. Objectif
- Écrire la décision de prélèvement (`ACCEPTE` ou `REJETE`) dans une base, de façon **idempotente** : rejouer un message de même `idEvenement` ne crée pas de doublon.
- Clé d'idempotence = `idEvenement`, avec une contrainte `PRIMARY KEY` en base.
- **Base** : H2 en mémoire par défaut (aucune propriété `datasource` ni `spring.sql.init` dans `application.yml`, **constaté** à la lecture du fichier). Un mode fichier est prévu pour les démonstrations (rejeu après arrêt du service, lecture des lignes depuis un autre processus) ; il n'a **pas encore été exercé**.
- **Effet de bord utile** : le traitement peut maintenant échouer, donc le chemin « exception levée par le listener → réessais → topic de rejets » est exercé pour la première fois (section 5).
- **Commits** : `d551282` (`refactor`, `idEvenement` dans `DemandePrelevement`), `c3b3048` (`feat`, port, adaptateur, schéma), `eed89c6` (`test`), puis ce journal.

## 2. Modèle
- **Record du domaine** `DecisionPrelevement(UUID idEvenement, String idAdherent, BigDecimal montant, Decision decision, Instant decideLe)`.
  - `idEvenement` vient du `CotisationEmise` reçu : le listener le passe à `DemandePrelevement`, qui le transporte jusqu'au cas d'usage.
  - `decideLe` vient d'un `Clock` injecté dans `TraiterDemandePrelevement` (`Clock.systemUTC()` en production).
  - `Decision` est l'énumération existante : `ACCEPTE`, `REJETE`.
- **Table** (`src/main/resources/schema.sql`, exécuté au démarrage) :
```sql
CREATE TABLE IF NOT EXISTS decision_prelevement (
    id_evenement VARCHAR(36)   NOT NULL,
    id_adherent  VARCHAR(64)   NOT NULL,
    montant      DECIMAL(19,2) NOT NULL,
    decision     VARCHAR(10)   NOT NULL,
    decide_le    TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT pk_decision_prelevement PRIMARY KEY (id_evenement)
);
```
- **Constaté** : `idEvenement` est stocké en `VARCHAR(36)` (le type `UUID` de H2 ne serait pas portable, **déduit**). Les tests d'intégration démarrent H2 en mémoire avec `jdbc:h2:mem:<uuid>` et l'utilisateur `SA` (logs de `HikariPool`), et `schema.sql` y est appliqué.
- **Constaté (bytecode, `spring-boot-sql` et `spring-boot-jdbc` 4.1.2)** : le mode d'initialisation SQL par défaut est `EMBEDDED`, et `EmbeddedDatabaseConnection.isEmbeddedUrl` ne reconnaît que `:h2:mem`. **Déduit** : avec une URL `jdbc:h2:file:…`, le schéma ne serait pas créé sans `spring.sql.init.mode=always`.

## 3. Technique : INSERT puis capture du doublon
- L'adaptateur fait un `INSERT` ; si la clé existe déjà, Spring lève une `DuplicateKeyException` qui est capturée (journal INFO `doublon ignoré idEvenement=…`) sans exception vers l'appelant. Toute autre `DataAccessException` remonte.
- **Constaté** : `sql-error-codes.xml` (dans `spring-jdbc-7.0.10.jar`) classe H2 `23001,23505` et Oracle `1` en `duplicateKeyCodes`. La traduction en `DuplicateKeyException` est donc prévue pour les deux bases ; l'exécution sur H2 est constatée par les tests (section 6), celle sur Oracle est **non vérifiée**.
- **Pourquoi cette technique** : la contrainte d'unicité tranche, y compris entre deux consommateurs concurrents (**déduit** : le comportement en concurrence n'a pas été exercé). Une seule requête par message.
- **Alternatives écartées** :
  - `SELECT` puis `INSERT` : course entre deux lecteurs (les deux voient « absent »), la contrainte reste nécessaire.
  - `MERGE` : syntaxe différente entre H2 et Oracle ; reporté à un éventuel traitement par lot sur Oracle.

## 4. Port et adaptateur
- **Port de sortie** `application/port/out/JournalDecisions` : `void enregistrer(DecisionPrelevement decision)`, documenté comme idempotent.
- **Adaptateur** `adapter/out/jdbc/JournalDecisionsJdbc` : `JdbcClient` (`org.springframework.jdbc.core.simple.JdbcClient`, `spring-jdbc` 7.0.10), requête positionnelle avec la liste des colonnes.
- **Configuration** : `config/PrelevementConfig` déclare les beans `Clock`, `JournalDecisions` (adaptateur) et `TraiterDemandePrelevement`.
- **Hexagonal** : `domain/` et `application/` n'importent ni H2 ni Spring JDBC (l'adaptateur et la configuration sont les seuls à les connaître). Le listener n'a pas changé : il appelle `traiter()` et journalise la décision.

## 5. Transaction, offset et chemin d'erreur
- **Constaté** : aucun `@Transactional` dans le code de production ; aucune propriété `transaction-id-prefix` dans `application.yml`. Boot ne crée le `KafkaTransactionManager` que si cette propriété est posée (annotation `ConditionalOnProperty` sur `spring.kafka.producer.transaction-id-prefix`, constatée dans le bytecode de `KafkaAutoConfiguration`, jar `spring-boot-kafka` 4.1.2). Il n'y a donc **pas d'atomicité** entre l'écriture en base et le commit de l'offset (**déduit**).
- **Une seule requête par message** : l'écriture est atomique par elle-même, sans transaction explicite.
- **Scénario 1 : base écrite, plantage avant le commit de l'offset (déduit)** : l'offset n'étant pas commité, le message est relivré ; l'`INSERT` lève un doublon, qui est capturé ; une seule ligne reste en base. **Non exercé par un vrai plantage** : le chemin « doublon » est exercé en envoyant deux fois le même message (section 6).
- **Scénario 2 : échec de l'écriture (constaté)** : l'exception sort du listener ; `DefaultErrorHandler` (BackOff par défaut `FixedBackOff(0, 9)`) relivre le message 10 fois, puis le `DeadLetterPublishingRecoverer` publie sur `<topic>-dlt` et l'offset est commité. Le détail du topic de rejets est dans [phase-3-message-invalide.md](phase-3-message-invalide.md).
- **Attention (constaté)** : une base H2 en mémoire disparaît à l'arrêt du processus. Les lignes déjà écrites sont perdues alors que leurs offsets sont commités ; l'idempotence au rejeu après redémarrage ne se démontre donc qu'avec une base fichier.

## 6. Tests
- **`TraiterDemandePrelevementTest`** (6 tests, dont 3 nouveaux ; Mockito sur le port `JournalDecisions` uniquement, `Clock` fixe) : la décision acceptée et la décision rejetée sont enregistrées avec les bonnes valeurs ; une exception du journal remonte telle quelle.
- **`JournalDecisionsJdbcTest`** (4 tests, vraie base H2 en mémoire via `EmbeddedDatabaseBuilder` et le `schema.sql` de production, sans Mockito) :
  - un enregistrement donne une ligne avec toutes les colonnes (dont `decide_le`, relu en `OffsetDateTime` puis `Instant`, égal à la valeur écrite) ;
  - un doublon donne une seule ligne et la **première** valeur est conservée (la seconde écriture avec un autre adhérent, un autre montant et une autre décision est ignorée) ;
  - deux `idEvenement` différents donnent deux lignes ;
  - un `idAdherent` de 65 caractères lève une `DataIntegrityViolationException` et laisse la table vide.
- **`PersistanceIntegrationTest`** (3 tests, `@EmbeddedKafka` + H2 mémoire, garde-fou broker embarqué, attente par offsets commités, lecture du DLT par offsets de fin, aucune lecture de logs ; `@MockitoSpyBean` sur le port) :
  - `doublon_meme_idEvenement_donne_une_seule_ligne` : le même JSON envoyé deux fois (même clé) donne une seule ligne, décision `ACCEPTE`.
  - `echec_base_puis_message_valide` : un message dont l'`idAdherent` fait 65 caractères (vraie erreur H2), puis un message valide (autre `idEvenement`, même clé donc même partition). Le message invalide n'est pas en base, le valide y est (`ACCEPTE`). Le DLT contient exactement un rejet pour la clé : le JSON du `CotisationEmise` du message invalide (il contient son `idEvenement` et l'`idAdherent` de 65 `x`, pas l'`idEvenement` du valide), avec `kafka_dlt-original-topic` = `cotisations.emises`.
  - `dix_livraisons_avant_dlt` : `verify(journalDecisions, times(10))` pour l'`idEvenement` du test (l'espion est partagé par les tests de la classe, le décompte est donc limité à cet `idEvenement`), plus un rejet au DLT.
- **Constaté lors du dernier `mvn -B test -pl prelevement-service -am`** : `BUILD SUCCESS`, 46 tests pour `prelevement-service`, 11 pour `events-contract`.
  - **10 livraisons** : confirmées par `times(10)` (`FixedBackOff(0, 9)` = 1 + 9).
  - **Cause du rejet** : `kafka_dlt-exception-cause-fqcn` = `org.springframework.dao.DataIntegrityViolationException`.
  - **Contenu du DLT** : le JSON du `CotisationEmise` (sérialisé par le `DelegatingByTypeSerializer`), pas les octets bruts du message d'origine.
  - **`Instant` en JDBC** : écrit et relu sans perte sur H2 (valeur avec millisecondes).
  - **Chemin « doublon »** : exercé. La sortie Maven contient deux lignes `doublon ignoré idEvenement=…` (celle du test d'intégration sur le thread du consommateur, celle du test de l'adaptateur) ; les accents y sont altérés par l'encodage de la sortie standard. Le test d'intégration n'asserte pas ce journal : il vérifie la ligne unique en base.

## 7. Ce qui est constaté, déduit, non vérifié
| | Constaté | Déduit | Non vérifié |
|---|---|---|---|
| Idempotence | Un doublon donne une seule ligne ; la première valeur est conservée (adaptateur) ; chemin « doublon » exécuté | Fonctionnement entre deux consommateurs concurrents (la contrainte tranche) | Concurrence réelle ; rejeu après un vrai plantage ou par `--reset-offsets` |
| Traduction de l'erreur | `sql-error-codes.xml` : H2 `23001,23505`, Oracle `1` en `duplicateKeyCodes` ; H2 `22001` en `dataIntegrityViolationCodes` ; `DuplicateKeyException` et `DataIntegrityViolationException` observées sur H2 | | Comportement Oracle |
| Échec de la base | 10 livraisons puis DLT ; cause `DataIntegrityViolationException` ; JSON du `CotisationEmise` au DLT ; le message valide suivant est traité | | BackOff personnalisé ; DLT injoignable ; base indisponible (panne transitoire) |
| Transaction et offset | Aucun `@Transactional`, aucun `transaction-id-prefix` | Pas d'atomicité base / offset ; scénario « base écrite puis plantage » absorbé par le doublon | Un vrai plantage entre l'écriture et le commit |
| `Instant` et types | `Instant` écrit et relu sans perte sur H2 ; `decide_le` en `TIMESTAMP WITH TIME ZONE` | `VARCHAR(36)` plus portable que `UUID` | Portabilité Oracle (types, `Instant`, `MERGE`) |
| Format JSON du DLT | Contient l'`idEvenement` et l'`idAdherent` | | Format de `Instant` (`emiseLe`) et de `BigDecimal` (`montant`) |
| Montant | `DECIMAL(19,2)` ; valeurs à deux décimales écrites et relues | | Effet d'un montant avec plus de deux décimales (arrondi ou erreur) |
| Base en mémoire | `jdbc:h2:mem:<uuid>`, `SA`, schéma appliqué | Perdue à l'arrêt du processus ; mode fichier à activer par ligne de commande avec `spring.sql.init.mode=always` | Mode fichier, `AUTO_SERVER`, lecture depuis un autre processus (non exercés) |

## 8. Notes et limites
- **Premier arrivé, premier gardé** : un rejeu ne met pas à jour une décision déjà enregistrée. Si le plafond change entre deux traitements du même `idEvenement`, la première décision reste (constaté au niveau de l'adaptateur).
- **BackOff** : toujours celui par défaut de `DefaultErrorHandler` ; 10 livraisons sans pause ne survivent pas à une coupure de base de quelques secondes (**déduit**). À revoir avec une pause explicite (la durée totale doit rester sous `max.poll.interval.ms`).
- **Démonstrations prévues, non encore exécutées** : (1) le même message deux fois, une seule ligne ; (2) rejeu par `kafka-consumer-groups --reset-offsets` (d'abord en simulation) avec une base fichier ; (3) échec de la base, réessais puis DLT.
- **Lecture des lignes sans console web** (prévue) : `org.h2.tools.Shell` avec `AUTO_SERVER=TRUE`. La console H2 de Boot exige une application web servlet, ce que ce service n'est pas.
- **Journal précédent** : [phase-3-message-invalide.md](phase-3-message-invalide.md) (section 7) indique que le chemin « exception du listener → 10 livraisons → DLT avec un `CotisationEmise` en JSON » n'était pas exercé ; il l'est depuis `eed89c6` (section 6 ci-dessus).
