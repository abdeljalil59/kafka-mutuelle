package io.github.abdeljalil59.mutuelle.prelevement.config;

import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

@Configuration
@ConditionalOnProperty(name = "prelevement.kafka.retry-demo.actif", havingValue = "true")
public class RetryDemoKafkaConfig {

    // Même valeur que cotisations.emises : les topics de retry et le DLT ont le même nombre de partitions.
    private static final int PARTITIONS = 3;

    @Bean
    NewTopic topicRetryDemo(@Value("${prelevement.kafka.retry-demo.topic}") String topic) {
        return TopicBuilder.name(topic).partitions(PARTITIONS).replicas(1).build();
    }

    // Fabrique dédiée, sous un autre nom que kafkaListenerContainerFactory (celle de Boot reste intacte).
    // Le ConsumerFactory est volontairement construit ici et non exposé en bean : un bean ConsumerFactory
    // désactiverait celui de Boot (@ConditionalOnMissingBean) et changerait le listener existant.
    // Aucun CommonErrorHandler n'est déclaré : @RetryableTopic en pose un sur chacun de ses containers.
    @Bean
    ConcurrentKafkaListenerContainerFactory<String, byte[]> retryDemoContainerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> proprietes = kafkaProperties.buildConsumerProperties();
        proprietes.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        // Réglages de l'ErrorHandlingDeserializer et de Jackson : sans objet pour des octets bruts.
        proprietes.keySet().removeIf(cle -> cle.startsWith("spring.deserializer.") || cle.startsWith("spring.json."));
        ConcurrentKafkaListenerContainerFactory<String, byte[]> fabrique = new ConcurrentKafkaListenerContainerFactory<>();
        fabrique.setConsumerFactory(new DefaultKafkaConsumerFactory<>(proprietes));
        return fabrique;
    }
}
