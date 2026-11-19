package io.github.abdeljalil59.mutuelle.prelevement.config;

import io.github.abdeljalil59.mutuelle.prelevement.adapter.out.jdbc.JournalDecisionsJdbc;
import io.github.abdeljalil59.mutuelle.prelevement.application.TraiterDemandePrelevement;
import io.github.abdeljalil59.mutuelle.prelevement.application.port.out.JournalDecisions;
import io.github.abdeljalil59.mutuelle.prelevement.domain.PlafondPrelevement;
import java.math.BigDecimal;
import java.time.Clock;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
public class PrelevementConfig {

    // Pas de valeur par défaut dans le code : une propriété absente ou invalide fait échouer le démarrage.
    @Bean
    PlafondPrelevement plafondPrelevement(@Value("${prelevement.plafond}") String plafond) {
        return new PlafondPrelevement(new BigDecimal(plafond));
    }

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    JournalDecisions journalDecisions(JdbcTemplate jdbcTemplate) {
        return new JournalDecisionsJdbc(jdbcTemplate);
    }

    @Bean
    TraiterDemandePrelevement traiterDemandePrelevement(
            PlafondPrelevement plafond, JournalDecisions journalDecisions, Clock clock) {
        return new TraiterDemandePrelevement(plafond, journalDecisions, clock);
    }
}
