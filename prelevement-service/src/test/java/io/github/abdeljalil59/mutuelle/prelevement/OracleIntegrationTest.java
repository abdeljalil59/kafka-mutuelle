package io.github.abdeljalil59.mutuelle.prelevement;

import java.time.Duration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.oracle.OracleContainer;

// Classe de base des tests qui ont besoin d'une base : un Oracle Free dans un conteneur, partagé par toutes les
// classes de la JVM de test (singleton : un seul démarrage, arrêté par Ryuk en fin de JVM). Les classes
// @SpringBootTest et JournalDecisionsJdbcTest en héritent. Docker doit être démarré : sinon le démarrage échoue
// et les tests aussi (voulu : la preuve doit être visible).
// Tag figé, comme apache/kafka:4.4.0. L'image crée l'utilisateur applicatif (APP_USER) dans FREEPDB1.
public abstract class OracleIntegrationTest {

    // Utilisateur applicatif du conteneur éphémère (ce ne sont pas des secrets) : les mêmes valeurs que
    // l'Oracle local de application.yml, sans en dépendre.
    private static final String DB_USER = "kafka_mutuelle";
    private static final String DB_PASSWORD = "kafka_mutuelle";

    // Délai de démarrage porté à 5 minutes : le délai par défaut de 60 s a expiré (constaté) alors qu'Oracle
    // initialisait encore sa base au premier démarrage, sur une machine chargée.
    protected static final OracleContainer ORACLE =
            new OracleContainer("gvenzl/oracle-free:23.26.4-slim-faststart")
                    .withUsername(DB_USER)
                    .withPassword(DB_PASSWORD)
                    .withStartupTimeout(Duration.ofMinutes(5));

    static {
        ORACLE.start();
    }

    // Surcharge la datasource Oracle locale de application.yml par celle du conteneur.
    @DynamicPropertySource
    static void datasourceOracle(DynamicPropertyRegistry registre) {
        registre.add("spring.datasource.url", ORACLE::getJdbcUrl);
        registre.add("spring.datasource.username", ORACLE::getUsername);
        registre.add("spring.datasource.password", ORACLE::getPassword);
    }
}
