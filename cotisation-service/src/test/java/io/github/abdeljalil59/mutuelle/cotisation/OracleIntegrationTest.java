package io.github.abdeljalil59.mutuelle.cotisation;

import java.time.Duration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.oracle.OracleContainer;

// Classe de base des tests qui ont besoin d'une base : un Oracle Free dans un conteneur, partagé par toutes les
// classes de la JVM de test (singleton : un seul démarrage, arrêté par Ryuk en fin de JVM). Docker doit être
// démarré : sinon le démarrage échoue et les tests aussi (voulu : la preuve doit être visible).
// Tag figé, comme dans prelevement-service ; copie de sa classe de base (pas de module de support partagé).
public abstract class OracleIntegrationTest {

    // Utilisateur applicatif du conteneur éphémère (ce ne sont pas des secrets) : mêmes valeurs que l'Oracle local.
    private static final String DB_USER = "kafka_mutuelle";
    private static final String DB_PASSWORD = "kafka_mutuelle";

    // Délai de démarrage porté à 10 minutes (le défaut de 60 s a expiré dans prelevement-service ; démarrages mesurés
    // de 3 min 53 s à plus de 5 minutes).
    protected static final OracleContainer ORACLE =
            new OracleContainer("gvenzl/oracle-free:23.26.4-slim-faststart")
                    .withUsername(DB_USER)
                    .withPassword(DB_PASSWORD)
                    .withStartupTimeout(Duration.ofMinutes(10));

    static {
        ORACLE.start();
    }

    // Surcharge la source de données des classes @SpringBootTest (utilisée à l'étape suivante).
    @DynamicPropertySource
    static void datasourceOracle(DynamicPropertyRegistry registre) {
        registre.add("spring.datasource.url", ORACLE::getJdbcUrl);
        registre.add("spring.datasource.username", ORACLE::getUsername);
        registre.add("spring.datasource.password", ORACLE::getPassword);
    }
}
