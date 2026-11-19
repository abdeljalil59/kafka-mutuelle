package io.github.abdeljalil59.mutuelle.cotisation;

import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.github.abdeljalil59.mutuelle.events.avro.CotisationEmise;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;

// Lecture d'un topic (messages Avro) dans les tests d'intégration de l'outbox.
final class LectureTopic {

    private static final Duration DELAI_MAXIMAL = Duration.ofSeconds(10);

    private LectureTopic() {
    }

    // Consommateur positionné en fin de log avant toute publication : il ne lit que les messages de son propre test.
    // "registre" est l'URL mock:// du Schema Registry factice, la même que celle du contexte Spring du test.
    static Consumer<String, CotisationEmise> consommateurEnFinDeLog(
            EmbeddedKafkaBroker broker, String topic, String registre) {
        Map<String, Object> proprietes =
                KafkaTestUtils.consumerProps("test-" + UUID.randomUUID(), "false", broker);
        proprietes.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        proprietes.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        proprietes.put("schema.registry.url", registre);
        proprietes.put("specific.avro.reader", true);
        Consumer<String, CotisationEmise> consumer =
                new DefaultKafkaConsumerFactory<String, CotisationEmise>(proprietes).createConsumer();
        List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                .map(info -> new TopicPartition(topic, info.partition()))
                .toList();
        consumer.assign(partitions);
        consumer.seekToEnd(partitions);
        partitions.forEach(consumer::position);
        return consumer;
    }

    // Lit les messages de la clé donnée jusqu'à en avoir "attendus", puis fait une dernière lecture :
    // un message en trop (doublon inattendu) fait échouer l'assertion sur la taille.
    static List<ConsumerRecord<String, CotisationEmise>> lireLaCle(
            Consumer<String, CotisationEmise> consumer, String cle, int attendus) {
        List<ConsumerRecord<String, CotisationEmise>> messages = new ArrayList<>();
        long limite = System.nanoTime() + DELAI_MAXIMAL.toNanos();
        while (messages.size() < attendus && System.nanoTime() < limite) {
            ajouterLaCle(consumer.poll(Duration.ofMillis(500)), cle, messages);
        }
        ajouterLaCle(consumer.poll(Duration.ofMillis(500)), cle, messages);
        assertThat(messages).hasSize(attendus);
        return messages;
    }

    private static void ajouterLaCle(ConsumerRecords<String, CotisationEmise> lus, String cle,
                                     List<ConsumerRecord<String, CotisationEmise>> messages) {
        lus.forEach(message -> {
            if (cle.equals(message.key())) {
                messages.add(message);
            }
        });
    }
}
