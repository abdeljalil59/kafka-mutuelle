package io.github.abdeljalil59.mutuelle.impaye.config;

import io.github.abdeljalil59.mutuelle.impaye.adapter.out.jdbc.PersistanceImpayeJdbc;
import io.github.abdeljalil59.mutuelle.impaye.application.port.out.PersistanceImpaye;
import io.github.abdeljalil59.mutuelle.impaye.application.port.out.PublicationRelance;
import io.github.abdeljalil59.mutuelle.impaye.application.usecase.TraiterRejet;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.simple.JdbcClient;

@Configuration
public class ImpayeConfig {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    PersistanceImpaye persistanceImpaye(JdbcClient jdbcClient, Clock clock, MeterRegistry meterRegistry) {
        return new PersistanceImpayeJdbc(jdbcClient, clock, meterRegistry);
    }

    // Pas de valeur par défaut dans le code : une propriété absente ou invalide fait échouer le démarrage
    // (la valeur par défaut, 3, est dans application.yml).
    @Bean
    TraiterRejet traiterRejet(PersistanceImpaye persistanceImpaye, PublicationRelance publicationRelance, Clock clock,
            @Value("${app.relance.seuil}") int seuil) {
        return new TraiterRejet(persistanceImpaye, publicationRelance, clock, seuil);
    }
}
