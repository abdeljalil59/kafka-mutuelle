package io.github.abdeljalil59.mutuelle.prelevement.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.abdeljalil59.mutuelle.events.avro.CotisationEmise;
import io.github.abdeljalil59.mutuelle.prelevement.AvroTestUtil;
import io.github.abdeljalil59.mutuelle.prelevement.domain.PrelevementInvalideException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.BackOffExecution;

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

    // Intervalle de 1 ms : ces tests comptent des livraisons, pas des secondes. La suite réelle des pauses (1, 2, 4, 8 s) est
    // vérifiée sur le BackOff lui-même.
    private DefaultErrorHandler gestionnaire(KafkaTemplate<Object, Object> modele) {
        return (DefaultErrorHandler) config.gestionnaireErreursConsommation(modele, "cotisations.emises", 1L, 2.0, 4);
    }

    // Nombre de livraisons jusqu'à l'envoi au topic de rejets : un handleOne par livraison échouée.
    private int livraisonsJusquAuRejet(DefaultErrorHandler gestionnaire, Exception exception) {
        int livraisons = 0;
        boolean recupere = false;
        while (!recupere && livraisons < 20) {
            livraisons++;
            recupere = recupere(gestionnaire, exception);
        }
        return livraisons;
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
        DefaultErrorHandler gestionnaire = gestionnaire(modele);

        // Première livraison seulement : la classification (parcours des causes) évite tout réessai.
        boolean recupere = recupere(gestionnaire, new ListenerExecutionFailedException(
                "échec du listener", new PrelevementInvalideException("montant doit être strictement positif")));

        assertThat(recupere).isTrue();
        verify(modele).send(any(ProducerRecord.class));
    }

    @Test
    void reessaie_une_autre_exception_au_lieu_de_l_envoyer_aussitot_au_topic_de_rejets() {
        KafkaTemplate<Object, Object> modele = modeleDeRejetsFactice();
        DefaultErrorHandler gestionnaire = gestionnaire(modele);

        boolean recupere = recupere(gestionnaire, new ListenerExecutionFailedException(
                "échec du listener", new IllegalStateException("base indisponible")));

        assertThat(recupere).isFalse();
        verify(modele, never()).send(any(ProducerRecord.class));
    }

    @Test
    void les_pauses_par_defaut_valent_1_2_4_puis_8_secondes_et_le_backoff_s_arrete_ensuite() {
        BackOffExecution execution = ErreursKafkaConfig.backOffDeConsommation(1000L, 2.0, 4).start();

        assertThat(new long[] {execution.nextBackOff(), execution.nextBackOff(), execution.nextBackOff(),
                execution.nextBackOff()}).containsExactly(1000L, 2000L, 4000L, 8000L);
        assertThat(execution.nextBackOff()).isEqualTo(BackOffExecution.STOP);
    }

    @Test
    void les_pauses_suivent_l_intervalle_le_multiplicateur_et_le_nombre_de_reessais_configures() {
        BackOffExecution execution = ErreursKafkaConfig.backOffDeConsommation(10L, 3.0, 2).start();

        assertThat(new long[] {execution.nextBackOff(), execution.nextBackOff()}).containsExactly(10L, 30L);
        assertThat(execution.nextBackOff()).isEqualTo(BackOffExecution.STOP);
    }

    // Relie le fichier de configuration livré aux pauses réelles : les tests d'intégration réduisent l'intervalle à 10 ms,
    // seul ce test lit les valeurs de production (application.yml, sans surcharge).
    @Test
    void application_yml_configure_des_pauses_de_1_2_4_puis_8_secondes() {
        YamlPropertiesFactoryBean yaml = new YamlPropertiesFactoryBean();
        yaml.setResources(new ClassPathResource("application.yml"));
        Properties proprietes = yaml.getObject();

        BackOffExecution execution = ErreursKafkaConfig.backOffDeConsommation(
                Long.parseLong(proprietes.getProperty("kafka.error-handler.initial-interval")),
                Double.parseDouble(proprietes.getProperty("kafka.error-handler.multiplier")),
                Integer.parseInt(proprietes.getProperty("kafka.error-handler.max-retries"))).start();

        assertThat(new long[] {execution.nextBackOff(), execution.nextBackOff(), execution.nextBackOff(),
                execution.nextBackOff()}).containsExactly(1000L, 2000L, 4000L, 8000L);
        assertThat(execution.nextBackOff()).isEqualTo(BackOffExecution.STOP);
    }

    @Test
    void declare_le_topic_de_rejets_avec_une_retention_de_30_jours() {
        NewTopic topic = config.topicRejetsCotisations("cotisations.emises");

        // 30 x 24 x 3600 x 1000 ms
        assertThat(topic.configs()).containsEntry("retention.ms", "2592000000");
    }

    @Test
    void cinq_livraisons_pour_une_erreur_reessayable_puis_un_seul_envoi_au_topic_de_rejets() {
        KafkaTemplate<Object, Object> modele = modeleDeRejetsFactice();
        DefaultErrorHandler gestionnaire = gestionnaire(modele);

        int livraisons = livraisonsJusquAuRejet(gestionnaire, new ListenerExecutionFailedException(
                "échec du listener", new IllegalStateException("base indisponible")));

        assertThat(livraisons).isEqualTo(5);
        verify(modele, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    void une_seule_livraison_pour_une_PrelevementInvalideException_puis_un_envoi_au_topic_de_rejets() {
        KafkaTemplate<Object, Object> modele = modeleDeRejetsFactice();
        DefaultErrorHandler gestionnaire = gestionnaire(modele);

        int livraisons = livraisonsJusquAuRejet(gestionnaire, new ListenerExecutionFailedException(
                "échec du listener", new PrelevementInvalideException("montant doit être strictement positif")));

        assertThat(livraisons).isEqualTo(1);
        verify(modele, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    void une_seule_livraison_pour_un_message_illisible_puis_un_envoi_au_topic_de_rejets() {
        KafkaTemplate<Object, Object> modele = modeleDeRejetsFactice();
        DefaultErrorHandler gestionnaire = gestionnaire(modele);

        int livraisons = livraisonsJusquAuRejet(gestionnaire, new ListenerExecutionFailedException("échec du listener",
                new DeserializationException("failed to deserialize", new byte[] {1, 2, 3}, false,
                        new IllegalStateException("octet magique inconnu"))));

        assertThat(livraisons).isEqualTo(1);
        verify(modele, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    void notifie_le_retry_listener_des_echecs_de_livraison_puis_de_la_recuperation() {
        KafkaTemplate<Object, Object> modele = modeleDeRejetsFactice();
        DefaultErrorHandler gestionnaire = gestionnaire(modele);
        List<String> evenements = new ArrayList<>();
        gestionnaire.setRetryListeners(new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
                evenements.add("echec " + deliveryAttempt);
            }

            @Override
            public void recovered(ConsumerRecord<?, ?> record, Exception ex) {
                evenements.add("recupere");
            }
        });

        livraisonsJusquAuRejet(gestionnaire, new ListenerExecutionFailedException(
                "échec du listener", new IllegalStateException("base indisponible")));

        // Constaté : failedDelivery est notifié à CHAQUE livraison échouée, y compris la dernière (la 5e), avant la
        // récupération. Le nombre de livraisons est donc le nombre de failedDelivery (et non ce nombre plus un).
        assertThat(evenements).containsExactly("echec 1", "echec 2", "echec 3", "echec 4", "echec 5", "recupere");
    }
}
