# Phase 5, étape 1 : `prelevement-service` passe de H2 à Oracle (Testcontainers)

Environnement (19/11/2026, heure locale +01:00) : Spring Boot 4.1.2, Spring Framework 7.0.10, `ojdbc17` 23.26.3.0.0, Testcontainers 2.0.5, image `gvenzl/oracle-free:23.26.4-slim-faststart`. Versions : voir [versions.md](versions.md). Idempotence (INSERT puis doublon) : [phase-3-idempotence.md](phase-3-idempotence.md).

Pour chaque point : ce qui est **constaté** (test, sortie réelle, source ou doc lue), **déduit** et **non vérifié**. Les informations sur l'Oracle local (port, utilisateur, connexion) viennent de l'utilisateur : je ne m'y suis jamais connecté.

## 1. Objectif et périmètre
- Remplacer H2 par Oracle pour la table `decision_prelevement` de `prelevement-service`. **`cotisation-service` garde H2** pour sa table `outbox`.
- Inchangés : la logique d'idempotence (`INSERT` puis capture du doublon), le code Avro, les listeners et les configurations Kafka.
- **Numérotation** : « phase 5, étape 1 » est le nom de travail. Au brief, la phase 5 est `kafka-lab` et Oracle relève de la phase 6.
- **Commit** : `57f1d13` (amendé deux fois après sa création : constantes de test, puis datasource en variables d'environnement). **Constaté** : build complet `BUILD SUCCESS`, 123 tests (16 pour `events-contract`, 54 pour `cotisation-service`, 53 pour `prelevement-service`), 6 min 15 s. Ce build précède les deux amends ; après eux, `JournalDecisionsJdbcTest` a été relancé (4/4) et le YAML contrôlé hors Spring Boot. **Non relancé** : le build complet.

## 2. Oracle et Testcontainers
- **Oracle local** (**relevé de l'utilisateur**) : `localhost:1522`, service `freepdb1`, utilisateur `kafka_mutuelle`. Version exacte **non vérifiée**.
- **Image des tests** : `gvenzl/oracle-free:23.26.4-slim-faststart`, tag figé comme `apache/kafka:4.4.0`. **Constaté** : le conteneur annonce la version 23.26.4.0.0 et un répertoire interne `26ai` (la 23ai rebaptisée). L'image crée l'utilisateur applicatif (variables `APP_USER`, doc de l'image) : **constaté**, la connexion en `kafka_mutuelle` fonctionne.
- **Conteneur singleton** : `OracleIntegrationTest` démarre un seul Oracle par JVM de test (bloc `static`), arrêté par Ryuk. Sans extension JUnit : le module `testcontainers-junit-jupiter` n'est **pas** utilisé (il aurait redémarré Oracle à chaque classe).
- **Délai de démarrage de 5 minutes** : le défaut de 60 s a expiré (**constaté**) pendant l'initialisation de la base (« first database startup, initializing… »). Démarrages ensuite relevés : 3 min 53 s à 4 min 26 s (4 exécutions). L'image « faststart » n'est donc pas rapide ici ; cause **non établie** (machine chargée : 1,2 Go de RAM libre sur 15,7 Go ; Docker : 7,6 Go et 12 CPU).
- **Poids** : 1,33 Go compressés d'après Docker Hub (`23-slim` : 0,85 Go), 5,13 Go sur disque ; premier téléchargement 6 min 5 s à 3 Mo/s (**constaté**).
- **Versions** : `ojdbc17` 23.26.3.0.0 et Testcontainers 2.0.5 sont gérés par le BOM de Spring Boot (**constaté**) : rien dans le pom parent. Le module s'appelle `testcontainers-oracle-free` en 2.x (classe `org.testcontainers.oracle.OracleContainer`, **constatée** dans le jar).

## 3. Configuration
- `application.yml` : `spring.datasource.url`, `username` et `password` en `${PRELEVEMENT_DB_URL:…}`, `${PRELEVEMENT_DB_USER:…}` et `${PRELEVEMENT_DB_PASSWORD:…}`, avec les valeurs de l'Oracle local par défaut ; `driver-class-name: oracle.jdbc.OracleDriver` ; `spring.sql.init.mode: always` (Oracle n'est pas une base embarquée).
- **Constaté** (hors Spring Boot, SnakeYAML et `PropertyPlaceholderHelper`) : le YAML est valide, les défauts sont résolus, une variable définie l'emporte, même pour l'URL qui contient des `:` et un `@`. **Non vérifié** : un démarrage réel sur l'Oracle local.
- Les tests surchargent la datasource par `@DynamicPropertySource` : ils ne lisent pas ces placeholders (**déduit** du mécanisme).
- Pas de JPA dans le projet (`JdbcClient`) : aucun dialecte Hibernate.
- Le service ne démarre plus sans base ; le build exige Docker (échec sans lui, choix assumé).

## 4. Schéma
`VARCHAR2(… CHAR)` pour les trois textes, `NUMBER(19,2)` pour le montant, `TIMESTAMP WITH TIME ZONE` inchangé, clé primaire `pk_decision_prelevement`. La limite de 64 caractères d'`id_adherent` est en caractères et non en octets (**déduit** : Oracle compte en octets par défaut). **Constaté** : `CREATE TABLE IF NOT EXISTS` s'applique sur Oracle `23.26.4` (conteneur) ; la doc Oracle 23 décrit la clause, extraction partielle, release non confirmée dans le texte lu. **Non vérifié** sur l'Oracle local.

## 5. Découvertes
- **`ojdbc17` rejette un `Instant` en paramètre**, **constaté** : `ORA-17004: Type de colonne non valide` (état SQL 99999) à l'`INSERT`. Le jar cite pourtant `Instant` dans ses convertisseurs ; la doc JDBC lue n'en parle pas. Repli : `decide_le` est lié en `OffsetDateTime` UTC (`JournalDecisionsJdbc`, seule modification de code de production). **Constaté** : les millisecondes sont relues à l'identique (`.123Z`).
- **ORA-12899 (valeur trop longue) n'est pas traduit par Spring en `DataIntegrityViolationException`**. Codes Oracle de `sql-error-codes.xml` : `1400, 1722, 2291, 2292` (**constaté**). Essai hors base (état SQL 72000 supposé) : ORA-00001 donne `DuplicateKeyException`, ORA-01400 `DataIntegrityViolationException`, ORA-12899 n'est pas reconnu. **Constaté** sur Oracle : l'erreur est une `UncategorizedSQLException` dont le message contient `ORA-12899`. Conséquence : l'en-tête `kafka_dlt-exception-cause-fqcn` du rejet vaut `org.springframework.jdbc.UncategorizedSQLException` ; les 10 livraisons avant le DLT sont inchangées. Option écartée : un `sql-error-codes.xml` personnalisé.
- **Doublon** : ORA-00001 est bien traduit en `DuplicateKeyException`, la première valeur est conservée (**constaté**).
- **Messages du driver en français** (« Type de colonne non valide ») : les tests s'appuient sur le code `ORA-…`, pas sur le texte.

## 6. Tests
- `OracleIntegrationTest` (classe de base, `DB_USER` et `DB_PASSWORD` en constantes) : les quatre `@SpringBootTest` en héritent (`CotisationEmiseListenerIntegrationTest`, `MessageInvalideIntegrationTest`, `PersistanceIntegrationTest`, `RetryDemoIntegrationTest`).
- **`JournalDecisionsJdbcTest` n'est plus un test sans base** : il utilise l'Oracle du conteneur (schéma appliqué par `ResourceDatabasePopulator`, `DELETE` avant chaque test, la table étant partagée).
- **`RetryDemoIntegrationTest`** ne touche pas la base, mais son contexte charge la datasource : il hérite aussi de la classe de base.
- Aucun test ajouté ni retiré.

## 7. Ce qui est constaté, déduit, non vérifié
| | Constaté | Déduit | Non vérifié |
|---|---|---|---|
| Schéma | `IF NOT EXISTS` sur 23.26.4 ; table créée par `schema.sql` | `CHAR` pour une limite en caractères | Oracle local ; troncature d'un montant à plus de 2 décimales (`NUMBER(19,2)`) |
| `Instant` | Rejeté (ORA-17004) ; `OffsetDateTime` relu à l'identique | | Versions futures du driver |
| Erreurs | ORA-00001 en doublon ; ORA-12899 en `UncategorizedSQLException` | | État SQL réel de ORA-12899 (72000 supposé dans l'essai hors base) ; autres codes ORA |
| Démarrage | 3 min 53 s à 4 min 26 s, délai par défaut insuffisant | Charge de la machine | Durée sur une autre machine |
| Configuration | Placeholders et YAML (hors Boot) | Tests : placeholders non lus | Démarrage sur l'Oracle local |

## 8. Journaux antérieurs devenus partiellement obsolètes
[phase-3-idempotence.md](phase-3-idempotence.md) décrit une base H2 en mémoire, les codes d'erreur H2 et une `DataIntegrityViolationException` pour la valeur trop longue ; ces passages ne valent plus que pour `cotisation-service` (outbox) et pour l'état antérieur de `prelevement-service`. Non modifié.

## 9. Suite
Étape 2 prévue : remplacer l'`INSERT` et la capture du doublon par un `MERGE` (écriture par lot).
