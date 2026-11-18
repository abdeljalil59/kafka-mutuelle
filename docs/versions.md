# Versions retenues

Relevé du 16/11/2026, mis à jour après l'étape 3a de `cotisation-service`. Environnement : JDK 22.0.2 (compilation en `release 17`), Maven 3.8.6.

## Versions choisies et résolues

| Composant | Version | Origine |
|---|---|---|
| Kafka (broker) | 4.4.0 (`apache/kafka:4.4.0`) | Choix du projet. https://kafka.apache.org/community/downloads/ |
| Spring Boot | 4.1.2 | Choix du projet. Parent `spring-boot-starter-parent`, présent sur Maven Central. |
| Java (niveau de compilation) | 17 | Brief. Boot 4.1.2 exige Java 17 minimum : https://docs.spring.io/spring-boot/system-requirements.html |
| `spring-kafka` | 4.1.2 | Résolu par `mvn dependency:tree` sur `cotisation-service`. |
| `kafka-clients` | 4.2.2 | Résolu par `mvn dependency:tree` sur `cotisation-service`. |
| `spring-kafka-test` (`@EmbeddedKafka`) | 4.1.2 | Résolu, scope test. Apporté par `spring-boot-starter-kafka-test`. |
| Broker embarqué des tests (`kafka-server`) | 4.2.2 | Résolu, scope test. |
| `spring-boot-starter-jackson` | 4.1.2 | Résolu par `mvn dependency:tree` (16/11). Apporte `tools.jackson.core:jackson-databind` 3.1.7 (Jackson 3), scope compile. |
| Sérialiseur JSON `JacksonJsonSerializer` (`spring-kafka` 4.1.2) | Jackson 3 (`tools.jackson`) | Constaté sur le jar avec `javap` : constructeurs en `tools.jackson.databind.json.JsonMapper`. `JsonSerializer`, lui, utilise Jackson 2 (`com.fasterxml`) et porte une annotation `@Deprecated` dans son fichier de classe (portée non précisée). Non utilisé. |
| `spring-boot-maven-plugin` | 4.1.2 | Version gérée par `spring-boot-starter-parent`, constatée au build (`repackage` exécuté). |
| Spring Framework | 7.0.10 | Résolu. |
| `spring-boot-starter-jdbc` | 4.1.2 | Géré par le BOM de Boot (présent dans `spring-boot-dependencies-4.1.2.pom`). Constaté : `spring-boot-starter-jdbc-4.1.2.jar`, `spring-boot-jdbc-4.1.2.jar` et `HikariCP-7.0.2.jar` sur le classpath des tests de `prelevement-service` (rapport Surefire, 18/11). |
| H2 (`com.h2database:h2`, scope runtime) | 2.4.240 | `h2.version` du BOM `spring-boot-dependencies` 4.1.2 ; constaté : `h2-2.4.240.jar` sur le classpath des tests. |
| `JdbcClient` (`spring-jdbc`) | 7.0.10 | Classe `org.springframework.jdbc.core.simple.JdbcClient` dans `spring-jdbc-7.0.10.jar`, version de Spring Framework résolue (même rapport Surefire). |
| JUnit Jupiter | 6.0.3 | Résolu, scope test. |
| Mockito | 5.23.0 | Résolu, scope test. |
| AssertJ | 3.27.7 | Résolu, scope test. |

## Starters Boot 4.x utilisés
- `spring-boot-starter-kafka` et `spring-boot-starter-kafka-test`. Source : tableau « Messaging Starters » du guide de migration https://github.com/spring-projects/spring-boot/wiki/Spring-Boot-4.0-Migration-Guide
- `spring-boot-starter-jackson` (Jackson 3). Nom : même guide de migration, tableau des starters. Le besoin est déduit (aucune dépendance `tools.jackson` dans l'arbre sans lui), puis confirmé par `dependency:tree`.
- À venir : `spring-boot-starter-webmvc` (l'ancien `spring-boot-starter-web` est déprécié, même source).

## Sources
- Matrice spring-kafka, kafka-clients et Spring Boot : https://spring.io/projects/spring-kafka (spring-kafka 4.1.x, kafka-clients 4.2.x, Boot 4.1.x).
- Versions gérées par le BOM : https://docs.spring.io/spring-boot/appendix/dependency-versions/coordinates.html
- Dépendances réellement résolues : `mvn -B dependency:tree -pl cotisation-service`.

## Compatibilité client 4.2.2 et broker 4.4.0
- **Constatée pour les seules API utilisées** (étape 4b, voir [phase-1-producteur.md](phase-1-producteur.md), section 6) : le client 4.2.2 a initialisé un producteur idempotent (ProducerId attribué), créé ou utilisé le topic et envoyé 7 messages stockés par le broker 4.4.0, lus par les outils CLI et par Kafka UI. Aucune page de documentation lue ne la confirme.
- **Constatée aussi pour la consommation en groupe** (étape 5 de [phase-1-consommateur.md](phase-1-consommateur.md), section 5) : un client 4.2.2 a rejoint un groupe (protocole `classic`, stratégie `range`), lu, commité des offsets, repris à l'offset commité et quitté le groupe proprement contre le broker 4.4.0.
- **Constatée aussi pour le rebalancing à deux membres et l'arrêt brutal** ([phase-2-rebalancing.md](phase-2-rebalancing.md)) : deux clients 4.2.2 du même groupe (protocole `classic`, assignor `range`) contre le broker 4.4.0 : répartition des partitions, reprise aux offsets commités, départ propre (`LeaveGroup`) et arrêt brutal d'un processus (détection par le broker après environ 34 à 41 s).
- **Constatée aussi pour la publication vers un topic de rejets** ([phase-3-message-invalide.md](phase-3-message-invalide.md), sections 4 et 5) : un client 4.2.2 (producteur idempotent, `DelegatingByTypeSerializer`) a publié les messages illisibles sur `<topic>-dlt` contre le broker 4.4.0, et un `NewTopic` appliqué par `KafkaAdmin` a créé le topic de rejets (3 partitions) sur ce broker.
- **Non prouvé** : le reste du protocole, dont les transactions, `group.protocol=consumer` (KIP-848), `CooperativeStickyAssignor` (présent dans la liste mais non choisi) et plus de deux membres dans un groupe.

## Non vérifié
- **Broker embarqué** : `@EmbeddedKafka` démarre un broker 4.2.2, pas 4.4.0.
- **Format JSON réel** : le format produit pour `Instant` et `BigDecimal` par `JacksonJsonSerializer` n'est pas encore lu sur un message (étape 3b). La documentation spring-kafka (https://docs.spring.io/spring-kafka/reference/kafka/serdes.html) cite `JacksonJsonSerializer` sans préciser la version de Jackson ; c'est le jar qui l'a établie (voir le tableau).
- **Fin de support de Spring Boot 4.1** : dates non trouvées dans les pages lues.
