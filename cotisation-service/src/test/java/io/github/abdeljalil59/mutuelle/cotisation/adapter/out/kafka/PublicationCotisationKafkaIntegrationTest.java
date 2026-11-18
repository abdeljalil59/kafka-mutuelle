package io.github.abdeljalil59.mutuelle.cotisation.adapter.out.kafka;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.PublicationCotisation;
import io.github.abdeljalil59.mutuelle.cotisation.domain.Cotisation;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.StreamSupport;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

// Pas de "topics" dans @EmbeddedKafka : le topic doit être créé par KafkaAdmin
// à partir du bean NewTopic, ce que le premier test vérifie.
@SpringBootTest(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@EmbeddedKafka
@DirtiesContext
class PublicationCotisationKafkaIntegrationTest {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(PublicationCotisationKafkaIntegrationTest.class);
    private static final String TOPIC = "cotisations.emises";
    private static final Instant EMISE_LE = Instant.parse("2026-11-16T10:00:00Z");
    private static final UUID ID_EVENEMENT = UUID.fromString("aaaaaaaa-0000-0000-0000-000000000001");
    private static final JsonMapper JSON = JsonMapper.builder().build();

    @Autowired
    private PublicationCotisation publication;

    @Autowired
    private KafkaAdmin kafkaAdmin;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Test
    void cree_le_topic_a_3_partitions_au_demarrage() {
        TopicDescription description = kafkaAdmin.describeTopics(TOPIC).get(TOPIC);

        assertThat(description).isNotNull();
        assertThat(description.partitions()).hasSize(3);
    }

    @Test
    void publie_un_json_sans_entete_de_type() {
        try (Consumer<String, String> consumer = consommateurEnFinDeLog()) {
            publication.publier(new Cotisation("adh1", new BigDecimal("50.00"), EMISE_LE), ID_EVENEMENT);

            ConsumerRecord<String, String> message = lire(consumer, 1).get(0);
            List<String> entetes = StreamSupport.stream(message.headers().spliterator(), false)
                    .map(Header::key)
                    .toList();
            LOGGER.info("PREUVE json : cle={} partition={} offset={} entetes={} valeur={}",
                    message.key(), message.partition(), message.offset(), entetes, message.value());

            assertThat(message.key()).isEqualTo("adh1");
            assertThat(entetes).doesNotContain("__TypeId__");

            JsonNode json = JSON.readTree(message.value());
            assertThat(json.propertyNames())
                    .containsExactlyInAnyOrder("idEvenement", "idAdherent", "montant", "emiseLe");
            assertThat(UUID.fromString(json.get("idEvenement").asString())).isEqualTo(ID_EVENEMENT);
            assertThat(json.get("idAdherent").asString()).isEqualTo("adh1");
            // Format constaté puis figé : montant en nombre JSON avec son échelle,
            // emiseLe en chaîne ISO-8601 (et non en nombre epoch).
            assertThat(json.get("montant").isNumber()).isTrue();
            assertThat(json.get("montant").decimalValue()).isEqualByComparingTo("50.00");
            assertThat(message.value()).contains("\"montant\":50.00");
            assertThat(json.get("emiseLe").isString()).isTrue();
            assertThat(json.get("emiseLe").asString()).isEqualTo("2026-11-16T10:00:00Z");
        }
    }

    @Test
    void route_les_messages_par_cle() {
        List<String> cles = List.of("adh1", "adh2", "adh3", "adh4", "adh5", "adh6", "adh1", "adh2");

        try (Consumer<String, String> consumer = consommateurEnFinDeLog()) {
            cles.forEach(cle -> publication.publier(
                    new Cotisation(cle, BigDecimal.TEN, EMISE_LE), UUID.randomUUID()));

            List<ConsumerRecord<String, String>> messages = lire(consumer, cles.size());
            messages.forEach(message -> LOGGER.info("PREUVE routage : cle={} partition={} offset={}",
                    message.key(), message.partition(), message.offset()));

            Map<String, Set<Integer>> partitionsParCle = messages.stream().collect(
                    groupingBy(ConsumerRecord::key, mapping(ConsumerRecord::partition, toSet())));
            assertThat(partitionsParCle).hasSize(6)
                    .allSatisfy((cle, partitions) -> assertThat(partitions).hasSize(1));
            Set<Integer> partitionsUtilisees = partitionsParCle.values().stream()
                    .flatMap(Set::stream)
                    .collect(toSet());
            assertThat(partitionsUtilisees).hasSizeGreaterThan(1);
        }
    }

    // Consommateur positionné en fin de log avant toute publication :
    // il ne lit que les messages de son propre test.
    private Consumer<String, String> consommateurEnFinDeLog() {
        Map<String, Object> proprietes =
                KafkaTestUtils.consumerProps("test-" + UUID.randomUUID(), "false", embeddedKafka);
        Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<>(
                proprietes, new StringDeserializer(), new StringDeserializer()).createConsumer();
        List<TopicPartition> partitions = consumer.partitionsFor(TOPIC).stream()
                .map(info -> new TopicPartition(TOPIC, info.partition()))
                .toList();
        consumer.assign(partitions);
        consumer.seekToEnd(partitions);
        partitions.forEach(consumer::position);
        return consumer;
    }

    private List<ConsumerRecord<String, String>> lire(Consumer<String, String> consumer, int attendus) {
        List<ConsumerRecord<String, String>> messages = new ArrayList<>();
        long limite = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (messages.size() < attendus && System.nanoTime() < limite) {
            consumer.poll(Duration.ofMillis(500)).forEach(messages::add);
        }
        assertThat(messages).hasSize(attendus);
        return messages;
    }
}
