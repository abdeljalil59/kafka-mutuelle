package io.github.abdeljalil59.mutuelle.prelevement;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.github.abdeljalil59.mutuelle.events.avro.CotisationEmise;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;

// Outils de test Avro : producteur, décodeur et événements. Le Schema Registry est factice (mock://) :
// il doit être le même scope que celui du contexte Spring du test (propriété spring.kafka.properties.schema.registry.url).
public final class AvroTestUtil {

    public static final String REGISTRE = "mock://prelevement-tests";

    private AvroTestUtil() {
    }

    public static CotisationEmise evenement(UUID idEvenement, String idAdherent, String montant, Instant emiseLe) {
        return CotisationEmise.newBuilder()
                .setIdEvenement(idEvenement)
                .setIdAdherent(idAdherent)
                .setMontant(new BigDecimal(montant))
                .setEmiseLe(emiseLe)
                .build();
    }

    // Fabrique de producteurs Avro vers le broker embarqué, clé en chaîne. À détruire (destroy) par l'appelant.
    public static DefaultKafkaProducerFactory<String, CotisationEmise> usineAvro(EmbeddedKafkaBroker broker) {
        Map<String, Object> proprietes = KafkaTestUtils.producerProps(broker);
        proprietes.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        proprietes.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        proprietes.put("schema.registry.url", REGISTRE);
        return new DefaultKafkaProducerFactory<>(proprietes);
    }

    // Décode la valeur d'un message Avro (octets avec identifiant de schéma) en classe générée.
    public static CotisationEmise decoder(String topic, byte[] valeur) {
        try (KafkaAvroDeserializer deserialiseur = new KafkaAvroDeserializer()) {
            deserialiseur.configure(Map.of("schema.registry.url", REGISTRE, "specific.avro.reader", true), false);
            return (CotisationEmise) deserialiseur.deserialize(topic, valeur);
        }
    }
}
