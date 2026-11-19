package io.github.abdeljalil59.mutuelle.cotisation.adapter.out.kafka;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.PublicationCotisation;
import io.github.abdeljalil59.mutuelle.events.avro.CotisationEmise;
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
import org.apache.kafka.clients.consumer.ConsumerConfig;
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

// Pas de "topics" dans @EmbeddedKafka : le topic doit être créé par KafkaAdmin
// à partir du bean NewTopic, ce que le premier test vérifie.
// Le relais de l'outbox est désactivé : seul ce test publie, directement par le port.
// Registre de schémas factice (mock://) : aucun Schema Registry réel, le producteur du service et le lecteur du
// test partagent le même scope.
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.properties.schema.registry.url=" + PublicationCotisationKafkaIntegrationTest.REGISTRE,
        "mutuelle.cotisation.outbox.relais-actif=false"})
@EmbeddedKafka
@DirtiesContext
class PublicationCotisationKafkaIntegrationTest {

    static final String REGISTRE = "mock://cotisation-publication";
    private static final Logger LOGGER =
            LoggerFactory.getLogger(PublicationCotisationKafkaIntegrationTest.class);
    private static final String TOPIC = "cotisations.emises";
    private static final UUID ID_EVENEMENT = UUID.fromString("aaaaaaaa-0000-0000-0000-000000000001");
    // Payload JSON tel que le relais le lit dans l'outbox.
    private static final String PAYLOAD =
            "{\"idEvenement\":\"" + ID_EVENEMENT + "\",\"idAdherent\":\"adh1\","
                    + "\"montant\":50.00,\"emiseLe\":\"2026-11-16T10:00:00Z\"}";

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
    void publie_l_evenement_avro_sans_entete_de_type() {
        try (Consumer<String, CotisationEmise> consumer = consommateurEnFinDeLog()) {
            publication.publier(TOPIC, "adh1", PAYLOAD);

            ConsumerRecord<String, CotisationEmise> message = lire(consumer, 1).get(0);
            List<String> entetes = StreamSupport.stream(message.headers().spliterator(), false)
                    .map(Header::key)
                    .toList();
            LOGGER.info("PREUVE avro : cle={} partition={} offset={} entetes={} valeur={}",
                    message.key(), message.partition(), message.offset(), entetes, message.value());

            assertThat(message.key()).isEqualTo("adh1");
            assertThat(entetes).doesNotContain("__TypeId__");
            // Contenu décodé : le JSON de l'outbox a été converti sans perte (échelle 2, instant exact).
            assertThat(message.value().getIdEvenement()).isEqualTo(ID_EVENEMENT);
            assertThat(message.value().getIdAdherent()).isEqualTo("adh1");
            assertThat(message.value().getMontant()).isEqualTo(new BigDecimal("50.00"));
            assertThat(message.value().getEmiseLe()).isEqualTo(Instant.parse("2026-11-16T10:00:00Z"));
        }
    }

    @Test
    void route_les_messages_par_cle() {
        List<String> cles = List.of("adh1", "adh2", "adh3", "adh4", "adh5", "adh6", "adh1", "adh2");

        try (Consumer<String, CotisationEmise> consumer = consommateurEnFinDeLog()) {
            cles.forEach(cle -> publication.publier(TOPIC, cle, payload(cle)));

            List<ConsumerRecord<String, CotisationEmise>> messages = lire(consumer, cles.size());
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

    private static String payload(String idAdherent) {
        return "{\"idEvenement\":\"" + UUID.randomUUID() + "\",\"idAdherent\":\"" + idAdherent
                + "\",\"montant\":10.00,\"emiseLe\":\"2026-11-16T10:00:00Z\"}";
    }

    // Consommateur positionné en fin de log avant toute publication :
    // il ne lit que les messages de son propre test.
    private Consumer<String, CotisationEmise> consommateurEnFinDeLog() {
        Map<String, Object> proprietes =
                KafkaTestUtils.consumerProps("test-" + UUID.randomUUID(), "false", embeddedKafka);
        proprietes.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        proprietes.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        proprietes.put("schema.registry.url", REGISTRE);
        proprietes.put("specific.avro.reader", true);
        Consumer<String, CotisationEmise> consumer = new DefaultKafkaConsumerFactory<String, CotisationEmise>(
                proprietes).createConsumer();
        List<TopicPartition> partitions = consumer.partitionsFor(TOPIC).stream()
                .map(info -> new TopicPartition(TOPIC, info.partition()))
                .toList();
        consumer.assign(partitions);
        consumer.seekToEnd(partitions);
        partitions.forEach(consumer::position);
        return consumer;
    }

    private List<ConsumerRecord<String, CotisationEmise>> lire(Consumer<String, CotisationEmise> consumer, int attendus) {
        List<ConsumerRecord<String, CotisationEmise>> messages = new ArrayList<>();
        long limite = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (messages.size() < attendus && System.nanoTime() < limite) {
            consumer.poll(Duration.ofMillis(500)).forEach(messages::add);
        }
        assertThat(messages).hasSize(attendus);
        return messages;
    }
}
