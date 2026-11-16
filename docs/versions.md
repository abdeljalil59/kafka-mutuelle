# Versions retenues

Relevé du 16/11/2026. Environnement : JDK 22.0.2 (compilation en `release 17`), Maven 3.8.6.

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
| Spring Framework | 7.0.10 | Résolu. |
| JUnit Jupiter | 6.0.3 | Résolu, scope test. |
| Mockito | 5.23.0 | Résolu, scope test. |
| AssertJ | 3.27.7 | Résolu, scope test. |

## Starters Boot 4.x utilisés
- `spring-boot-starter-kafka` et `spring-boot-starter-kafka-test`. Source : tableau « Messaging Starters » du guide de migration https://github.com/spring-projects/spring-boot/wiki/Spring-Boot-4.0-Migration-Guide
- À venir : `spring-boot-starter-webmvc` (l'ancien `spring-boot-starter-web` est déprécié, même source).

## Sources
- Matrice spring-kafka, kafka-clients et Spring Boot : https://spring.io/projects/spring-kafka (spring-kafka 4.1.x, kafka-clients 4.2.x, Boot 4.1.x).
- Versions gérées par le BOM : https://docs.spring.io/spring-boot/appendix/dependency-versions/coordinates.html
- Dépendances réellement résolues : `mvn -B dependency:tree -pl cotisation-service`.

## Non vérifié
- **Client 4.2.2 et broker 4.4.0** : aucune page lue ne confirme la compatibilité. À prouver par un test d'intégration contre le broker du compose (étape 3 de `cotisation-service`).
- **Broker embarqué** : `@EmbeddedKafka` démarre un broker 4.2.2, pas 4.4.0.
- **Jackson** : `JacksonJsonSerializer` est cité par la documentation spring-kafka (https://docs.spring.io/spring-kafka/reference/kafka/serdes.html), sans préciser Jackson 2 ou 3. Aucune dépendance Jackson en compile pour l'instant (Jackson 2.21.7 en test seulement, apporté par `kafka-server`). Le BOM gère Jackson 3 (`tools.jackson.core`, 3.1.7) et Jackson 2 (2.21.7). À trancher avec le premier code de sérialisation.
- **Fin de support de Spring Boot 4.1** : dates non trouvées dans les pages lues.
