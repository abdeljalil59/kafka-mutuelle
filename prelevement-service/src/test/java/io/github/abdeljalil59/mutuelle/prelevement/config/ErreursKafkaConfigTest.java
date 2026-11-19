package io.github.abdeljalil59.mutuelle.prelevement.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.abdeljalil59.mutuelle.events.avro.CotisationEmise;
import io.github.abdeljalil59.mutuelle.prelevement.AvroTestUtil;
import io.github.abdeljalil59.mutuelle.prelevement.domain.PrelevementInvalideException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.MessageListenerContainer;

class ErreursKafkaConfigTest {

    private static final String TOPIC_REJETS = "cotisations.emises-dlt";

    private final ErreursKafkaConfig config = new ErreursKafkaConfig();

    // Le Supplier de la fabrique renvoie un sérialiseur non configuré : à la création d'un producteur, la fabrique
    // appelle configure() avec la configuration du producteur (constaté par les tests d'intégration du DLT). On fait de
    // même ici avec l'URL du registre : si DelegatingByTypeSerializer la transmet au KafkaAvroSerializer qu'il contient,
    // la sérialisation Avro fonctionne ; sinon elle échoue (« You must configure() before serialize() », constaté).
    private Serializer<Object> serialiseurDesRejets() {
        DefaultKafkaProducerFactory<Object, Object> usine = new DefaultKafkaProducerFactory<>(Map.of());
        config.serialiseursRejets().customize(usine);
        Serializer<Object> serialiseur = usine.getValueSerializerSupplier().get();
        serialiseur.configure(Map.of("schema.registry.url", AvroTestUtil.REGISTRE), false);
        return serialiseur;
    }

    @SuppressWarnings("unchecked")
    private KafkaTemplate<Object, Object> modeleDeRejetsFactice() {
        KafkaTemplate<Object, Object> modele = mock(KafkaTemplate.class);
        when(modele.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
        return modele;
    }

    private boolean recupere(DefaultErrorHandler gestionnaire, Exception exception) {
        ConsumerRecord<String, String> message = new ConsumerRecord<>("cotisations.emises", 0, 5L, "adh1", "valeur");
        return gestionnaire.handleOne(exception, message, mock(Consumer.class), mock(MessageListenerContainer.class));
    }

    @Test
    void laisse_les_octets_d_origine_intacts() {
        byte[] octets = "ceci-n-est-pas-du-json".getBytes(StandardCharsets.UTF_8);

        assertThat(serialiseurDesRejets().serialize(TOPIC_REJETS, octets)).isEqualTo(octets);
    }

    @Test
    void serialise_un_evenement_en_avro() {
        UUID idEvenement = UUID.fromString("44444444-4444-4444-4444-444444444444");
        CotisationEmise evenement = AvroTestUtil.evenement(
                idEvenement, "adh1", "50.00", Instant.parse("2026-11-17T08:00:00Z"));

        byte[] octets = serialiseurDesRejets().serialize(TOPIC_REJETS, evenement);

        // Format Confluent : octet magique 0, puis l'identifiant du schéma, puis les données Avro.
        assertThat(octets[0]).isZero();
        assertThat(AvroTestUtil.decoder(TOPIC_REJETS, octets)).isEqualTo(evenement);
    }

    @Test
    void refuse_un_type_non_prevu() {
        assertThatThrownBy(() -> serialiseurDesRejets().serialize(TOPIC_REJETS, "texte"))
                .isInstanceOf(SerializationException.class);
    }

    @Test
    void declare_le_topic_de_rejets_derive_du_topic_source_avec_trois_partitions() {
        NewTopic topic = config.topicRejetsCotisations("cotisations.emises");

        assertThat(topic.name()).isEqualTo("cotisations.emises-dlt");
        assertThat(topic.numPartitions()).isEqualTo(3);
    }

    @Test
    void ne_reessaie_pas_une_PrelevementInvalideException_et_l_envoie_aussitot_au_topic_de_rejets() {
        KafkaTemplate<Object, Object> modele = modeleDeRejetsFactice();
        DefaultErrorHandler gestionnaire =
                (DefaultErrorHandler) config.gestionnaireErreursConsommation(modele, "cotisations.emises");

        // Première livraison seulement : la classification (parcours des causes) évite tout réessai.
        boolean recupere = recupere(gestionnaire, new ListenerExecutionFailedException(
                "échec du listener", new PrelevementInvalideException("montant doit être strictement positif")));

        assertThat(recupere).isTrue();
        verify(modele).send(any(ProducerRecord.class));
    }

    @Test
    void reessaie_une_autre_exception_au_lieu_de_l_envoyer_aussitot_au_topic_de_rejets() {
        KafkaTemplate<Object, Object> modele = modeleDeRejetsFactice();
        DefaultErrorHandler gestionnaire =
                (DefaultErrorHandler) config.gestionnaireErreursConsommation(modele, "cotisations.emises");

        boolean recupere = recupere(gestionnaire, new ListenerExecutionFailedException(
                "échec du listener", new IllegalStateException("base indisponible")));

        assertThat(recupere).isFalse();
        verify(modele, never()).send(any(ProducerRecord.class));
    }
}
