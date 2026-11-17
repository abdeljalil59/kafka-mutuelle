package io.github.abdeljalil59.mutuelle.prelevement.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.abdeljalil59.mutuelle.events.CotisationEmise;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;

class ErreursKafkaConfigTest {

    private final ErreursKafkaConfig config = new ErreursKafkaConfig();

    private Serializer<Object> serialiseurDesRejets() {
        DefaultKafkaProducerFactory<Object, Object> usine = new DefaultKafkaProducerFactory<>(Map.of());
        config.serialiseursRejets().customize(usine);
        return usine.getValueSerializerSupplier().get();
    }

    @Test
    void laisse_les_octets_d_origine_intacts() {
        byte[] octets = "ceci-n-est-pas-du-json".getBytes(StandardCharsets.UTF_8);

        assertThat(serialiseurDesRejets().serialize("cotisations.emises-dlt", octets)).isEqualTo(octets);
    }

    @Test
    void serialise_un_evenement_en_json() {
        UUID idEvenement = UUID.fromString("44444444-4444-4444-4444-444444444444");
        CotisationEmise evenement = new CotisationEmise(
                idEvenement, "adh1", new BigDecimal("50.00"), Instant.parse("2026-11-17T08:00:00Z"));

        byte[] json = serialiseurDesRejets().serialize("cotisations.emises-dlt", evenement);

        assertThat(new String(json, StandardCharsets.UTF_8))
                .contains("44444444-4444-4444-4444-444444444444")
                .contains("50.00");
    }

    @Test
    void refuse_un_type_non_prevu() {
        assertThatThrownBy(() -> serialiseurDesRejets().serialize("cotisations.emises-dlt", "texte"))
                .isInstanceOf(SerializationException.class);
    }

    @Test
    void declare_le_topic_de_rejets_derive_du_topic_source_avec_trois_partitions() {
        NewTopic topic = config.topicRejetsCotisations("cotisations.emises");

        assertThat(topic.name()).isEqualTo("cotisations.emises-dlt");
        assertThat(topic.numPartitions()).isEqualTo(3);
    }
}
