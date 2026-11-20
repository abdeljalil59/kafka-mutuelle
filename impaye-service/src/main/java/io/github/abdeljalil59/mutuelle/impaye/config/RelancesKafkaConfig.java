package io.github.abdeljalil59.mutuelle.impaye.config;

import io.github.abdeljalil59.mutuelle.impaye.adapter.out.kafka.PublicationRelanceKafka;
import io.github.abdeljalil59.mutuelle.impaye.application.port.out.PublicationRelance;
import java.time.Duration;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.json.JsonMapper;

@Configuration
public class RelancesKafkaConfig {

    // Même nombre de partitions que les autres topics du projet. Ce service est le producteur du topic : il le crée.
    private static final int PARTITIONS = 3;

    @Bean
    NewTopic topicRelances(@Value("${impaye.kafka.relances.topic}") String topic) {
        return TopicBuilder.name(topic).partitions(PARTITIONS).replicas(1).build();
    }

    // Le KafkaTemplate de Spring Boot : producteur sans transaction, clés et valeurs en chaînes (application.yml).
    @Bean
    PublicationRelance publicationRelance(
            KafkaTemplate<String, String> kafkaTemplate,
            JsonMapper jsonMapper,
            @Value("${impaye.kafka.relances.topic}") String topic,
            @Value("${impaye.kafka.relances.delai-publication-ms}") long delaiPublicationMs) {
        return new PublicationRelanceKafka(kafkaTemplate, jsonMapper, topic, Duration.ofMillis(delaiPublicationMs));
    }
}
