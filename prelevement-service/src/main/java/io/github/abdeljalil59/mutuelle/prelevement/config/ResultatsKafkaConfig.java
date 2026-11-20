package io.github.abdeljalil59.mutuelle.prelevement.config;

import io.github.abdeljalil59.mutuelle.prelevement.adapter.out.kafka.PublicationResultatsKafka;
import io.github.abdeljalil59.mutuelle.prelevement.application.port.out.PublicationResultats;
import java.time.Duration;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
public class ResultatsKafkaConfig {

    // Même nombre de partitions que cotisations.emises (clé idAdherent : l'ordre par adhérent est conservé).
    private static final int PARTITIONS_RESULTATS = 3;

    // Comme cotisations.emises : le service producteur crée son topic au démarrage (KafkaAdmin), le compose n'en crée aucun.
    @Bean
    NewTopic topicResultats(@Value("${prelevement.kafka.resultats.topic}") String topic) {
        return TopicBuilder.name(topic).partitions(PARTITIONS_RESULTATS).replicas(1).build();
    }

    // Template dédié aux résultats, séparé de celui du topic de rejets : sa série spring.kafka.template porte son propre nom
    // (name="resultatsKafkaTemplate"). Même fabrique de producteurs que le template de Boot (sérialiseur par type exact,
    // voir ErreursKafkaConfig.serialiseursRejets).
    // Le type DÉCLARÉ est KafkaOperations, volontairement : le kafkaTemplate de Boot est @ConditionalOnMissingBean(KafkaTemplate),
    // et un bean déclaré KafkaTemplate le désactiverait, ainsi que le template du topic de rejets et du RetryDemoListener.
    @Bean
    KafkaOperations<Object, Object> resultatsKafkaTemplate(ProducerFactory<Object, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    PublicationResultats publicationResultats(
            @Qualifier("resultatsKafkaTemplate") KafkaOperations<Object, Object> kafka,
            @Value("${prelevement.kafka.resultats.topic}") String topic,
            @Value("${prelevement.kafka.resultats.delai-publication-ms}") long delaiPublicationMs) {
        return new PublicationResultatsKafka(kafka, topic, Duration.ofMillis(delaiPublicationMs));
    }
}
