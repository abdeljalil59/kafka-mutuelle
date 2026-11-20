package io.github.abdeljalil59.mutuelle.impaye.config;

import io.github.abdeljalil59.mutuelle.impaye.adapter.out.jdbc.PersistanceImpayeJdbc;
import io.github.abdeljalil59.mutuelle.impaye.application.port.out.PersistanceImpaye;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
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
}
