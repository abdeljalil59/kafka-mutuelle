package io.github.abdeljalil59.mutuelle.prelevement.adapter.in.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

// Démonstration technique de @RetryableTopic (retries non bloquants) : pas de cas d'usage derrière.
// Le message est du JSON brut, lu en octets ; "simulerEchec": true provoque un échec contrôlé.
@Component
@ConditionalOnProperty(name = "prelevement.kafka.retry-demo.actif", havingValue = "true")
public class RetryDemoListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(RetryDemoListener.class);

    private final JsonMapper jsonMapper;

    public RetryDemoListener(JsonMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    // 3 tentatives : la première sur le topic principal, puis deux réessais non bloquants (5 s, puis 10 s)
    // sur des topics dédiés. La fabrique dédiée lit les octets bruts, y compris pour les topics de retry et le DLT
    // (les endpoints de retry n'héritent pas des "properties" du @KafkaListener, constaté dans le bytecode).
    @RetryableTopic(attempts = "3", backOff = @BackOff(delay = 5000, multiplier = 2),
            autoCreateTopics = "true", numPartitions = "3",
            kafkaTemplate = "kafkaTemplate", listenerContainerFactory = "retryDemoContainerFactory")
    @KafkaListener(topics = "${prelevement.kafka.retry-demo.topic}",
            groupId = "${prelevement.kafka.retry-demo.group-id}", containerFactory = "retryDemoContainerFactory")
    public void ecouter(ConsumerRecord<String, byte[]> message) {
        LOGGER.info("retry-demo : réception topic={} partition={} offset={} clé={}",
                message.topic(), message.partition(), message.offset(), message.key());
        if (jsonMapper.readTree(message.value()).path("simulerEchec").asBoolean(false)) {
            throw new RuntimeException("Échec simulé pour démonstration @RetryableTopic");
        }
        LOGGER.info("retry-demo : message traité avec succès, clé={}", message.key());
    }

    @DltHandler
    public void traiterEchecDefinitif(ConsumerRecord<String, byte[]> message) {
        LOGGER.warn("retry-demo DLT : message en échec définitif, clé={}", message.key());
    }
}
