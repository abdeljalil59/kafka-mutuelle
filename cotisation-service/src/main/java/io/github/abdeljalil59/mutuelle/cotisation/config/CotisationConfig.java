package io.github.abdeljalil59.mutuelle.cotisation.config;

import io.github.abdeljalil59.mutuelle.cotisation.adapter.out.kafka.PublicationCotisationKafka;
import io.github.abdeljalil59.mutuelle.cotisation.application.EmettreCotisation;
import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.PublicationCotisation;
import io.github.abdeljalil59.mutuelle.events.CotisationEmise;
import java.time.Clock;
import java.time.Duration;
import java.util.UUID;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;

@Configuration
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
    PublicationCotisation publicationCotisation(
            KafkaTemplate<String, CotisationEmise> kafkaTemplate,
            @Value("${mutuelle.cotisation.kafka.topic}") String topic,
            @Value("${mutuelle.cotisation.kafka.delai-publication-ms}") long delaiPublicationMs) {
        return new PublicationCotisationKafka(
                kafkaTemplate, topic, Duration.ofMillis(delaiPublicationMs), UUID::randomUUID);
    }

    @Bean
    EmettreCotisation emettreCotisation(PublicationCotisation publication, Clock clock) {
        return new EmettreCotisation(publication, clock);
    }
}
