package io.github.abdeljalil59.mutuelle.cotisation.config;

import io.github.abdeljalil59.mutuelle.cotisation.adapter.in.planification.RelaiOutbox;
import io.github.abdeljalil59.mutuelle.cotisation.adapter.out.jdbc.OutboxJdbc;
import io.github.abdeljalil59.mutuelle.cotisation.adapter.out.kafka.PublicationCotisationKafka;
import io.github.abdeljalil59.mutuelle.cotisation.application.EmettreCotisation;
import io.github.abdeljalil59.mutuelle.cotisation.application.RelayerOutbox;
import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.Outbox;
import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.PublicationCotisation;
import java.time.Clock;
import java.time.Duration;
import java.util.UUID;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import tools.jackson.databind.json.JsonMapper;

@Configuration
@EnableScheduling
public class CotisationConfig {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    NewTopic topicCotisationsEmises(@Value("${mutuelle.cotisation.kafka.topic}") String topic) {
        return TopicBuilder.name(topic).partitions(3).replicas(1).build();
    }

    @Bean
    Outbox outbox(JdbcClient jdbcClient, JsonMapper jsonMapper,
                  @Value("${mutuelle.cotisation.kafka.topic}") String topic) {
        return new OutboxJdbc(jdbcClient, jsonMapper, topic);
    }

    // Le payload de l'outbox est déjà du JSON : le template envoie des chaînes (StringSerializer).
    @Bean
    PublicationCotisation publicationCotisation(
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${mutuelle.cotisation.kafka.delai-publication-ms}") long delaiPublicationMs) {
        return new PublicationCotisationKafka(kafkaTemplate, Duration.ofMillis(delaiPublicationMs));
    }

    @Bean
    EmettreCotisation emettreCotisation(Outbox outbox, Clock clock) {
        return new EmettreCotisation(outbox, clock, UUID::randomUUID);
    }

    @Bean
    RelayerOutbox relayerOutbox(Outbox outbox, PublicationCotisation publication, Clock clock,
                                @Value("${mutuelle.cotisation.outbox.taille-lot}") int tailleLot) {
        return new RelayerOutbox(outbox, publication, clock, tailleLot);
    }

    @Bean
    @ConditionalOnProperty(name = "mutuelle.cotisation.outbox.relais-actif", havingValue = "true", matchIfMissing = true)
    RelaiOutbox relaiOutbox(RelayerOutbox relayerOutbox) {
        return new RelaiOutbox(relayerOutbox);
    }
}
