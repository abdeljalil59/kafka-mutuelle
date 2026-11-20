package io.github.abdeljalil59.mutuelle.prelevement.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.github.abdeljalil59.mutuelle.events.ResultatPrelevement;
import io.github.abdeljalil59.mutuelle.events.TypeResultat;
import io.github.abdeljalil59.mutuelle.events.avro.CotisationEmise;
import io.github.abdeljalil59.mutuelle.prelevement.AvroTestUtil;
import io.github.abdeljalil59.mutuelle.prelevement.domain.PrelevementInvalideException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
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
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AfterRollbackProcessor;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultAfterRollbackProcessor;
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
    @SuppressWarnings("unchecked")
    private DefaultAfterRollbackProcessor<Object, Object> traitement(KafkaTemplate<Object, Object> modele) {
        return (DefaultAfterRollbackProcessor<Object, Object>) config.traitementApresRollback(
                modele, "cotisations.emises", 1L, 2.0, 4);
    }

    // Une livraison échouée : le container annule la transaction, puis appelle le processeur avec le message en échec.
    @SuppressWarnings("unchecked")
    private void livrerUnEchec(DefaultAfterRollbackProcessor<Object, Object> traitement, Exception exception) {
        ConsumerRecord<Object, Object> message = new ConsumerRecord<>("cotisations.emises", 0, 5L, "adh1", "valeur");
        MessageListenerContainer conteneur = mock(MessageListenerContainer.class);
        when(conteneur.getContainerProperties()).thenReturn(new ContainerProperties("cotisations.emises"));
        // Le recoverer vérifie l'existence de la partition sur le topic de rejets via le CONSOMMATEUR (partitionsFor), pas
        // via le template (bytecode de DeadLetterPublishingRecoverer.checkPartition) : sans réponse, il laisse la partition
        // vide et le broker choisit. Le topic de rejets a 3 partitions, comme le topic source.
        Consumer<Object, Object> consommateur = mock(Consumer.class);
        when(consommateur.partitionsFor(eq(TOPIC_REJETS), any(Duration.class))).thenReturn(List.of(
                new PartitionInfo(TOPIC_REJETS, 0, null, null, null),
                new PartitionInfo(TOPIC_REJETS, 1, null, null, null),
                new PartitionInfo(TOPIC_REJETS, 2, null, null, null)));
        traitement.process(List.of(message), consommateur, conteneur, exception, true, ContainerProperties.EOSMode.V2);
    }

    // Le rejet est publié par le recoverer au moment où les réessais sont épuisés (ou aussitôt, pour une erreur fatale).
    private static boolean rejetEnvoye(KafkaTemplate<Object, Object> modele) {
        return mockingDetails(modele).getInvocations().stream()
                .anyMatch(appel -> appel.getMethod().getName().equals("send"));
    }

    // Nombre de livraisons échouées jusqu'à l'envoi au topic de rejets.
    private int livraisonsJusquAuRejet(DefaultAfterRollbackProcessor<Object, Object> traitement,
            KafkaTemplate<Object, Object> modele, Exception exception) {
        int livraisons = 0;
        while (livraisons < 20 && !rejetEnvoye(modele)) {
            livraisons++;
            livrerUnEchec(traitement, exception);
        }
        return livraisons;
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
    void serialise_un_resultat_de_prelevement_en_avro() {
        ResultatPrelevement resultat = ResultatPrelevement.newBuilder()
                .setIdEvenement(UUID.fromString("44444444-4444-4444-4444-444444444444"))
                .setType(TypeResultat.REJETE)
                .setIdAdherent("adh1")
                .setMontant(new java.math.BigDecimal("150.00"))
                .setDecideLe(Instant.parse("2026-11-20T09:00:00Z"))
                .build();

        byte[] octets = serialiseurDesRejets().serialize("prelevements.resultats", resultat);

        // Format Confluent : octet magique 0, puis l'identifiant du schéma, puis les données Avro.
        assertThat(octets[0]).isZero();
        try (KafkaAvroDeserializer deserialiseur = new KafkaAvroDeserializer()) {
            deserialiseur.configure(Map.of("schema.registry.url", AvroTestUtil.REGISTRE, "specific.avro.reader", true), false);
            assertThat(deserialiseur.deserialize("prelevements.resultats", octets)).isEqualTo(resultat);
        }
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

        // Première livraison seulement : la classification (parcours des causes) évite tout réessai.
        livrerUnEchec(traitement(modele), new ListenerExecutionFailedException(
                "échec du listener", new PrelevementInvalideException("montant doit être strictement positif")));

        verify(modele).send(any(ProducerRecord.class));
    }

    @Test
    void reessaie_une_autre_exception_au_lieu_de_l_envoyer_aussitot_au_topic_de_rejets() {
        KafkaTemplate<Object, Object> modele = modeleDeRejetsFactice();

        livrerUnEchec(traitement(modele), new ListenerExecutionFailedException(
                "échec du listener", new IllegalStateException("base indisponible")));

        verify(modele, never()).send(any(ProducerRecord.class));
    }

    // Sous transaction, c'est le processeur d'après-rollback qui gère l'échec, et non un DefaultErrorHandler.
    @Test
    @SuppressWarnings("unchecked")
    void le_bean_est_un_DefaultAfterRollbackProcessor_et_publie_au_topic_de_rejets_sur_la_partition_d_origine() {
        KafkaTemplate<Object, Object> modele = modeleDeRejetsFactice();
        AfterRollbackProcessor<Object, Object> bean = config.traitementApresRollback(
                modele, "cotisations.emises", 1L, 2.0, 4);

        assertThat(bean).isInstanceOf(DefaultAfterRollbackProcessor.class);

        livrerUnEchec((DefaultAfterRollbackProcessor<Object, Object>) bean, new ListenerExecutionFailedException(
                "échec du listener", new PrelevementInvalideException("montant doit être strictement positif")));

        ArgumentCaptor<ProducerRecord<Object, Object>> rejet = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(modele).send(rejet.capture());
        assertThat(rejet.getValue().topic()).isEqualTo(TOPIC_REJETS);
        assertThat(rejet.getValue().partition()).isZero();
    }

    // commitRecovered : l'offset du message récupéré est validé dans la transaction du processeur, avec l'envoi au topic de
    // rejets. Sans cela, le message serait redélivré (et envoyé une seconde fois au rejet) après un redémarrage.
    @Test
    @SuppressWarnings("unchecked")
    void valide_l_offset_du_message_recupere_dans_la_transaction() {
        KafkaTemplate<Object, Object> modele = modeleDeRejetsFactice();
        when(modele.isTransactional()).thenReturn(true);

        livrerUnEchec(traitement(modele), new ListenerExecutionFailedException(
                "échec du listener", new PrelevementInvalideException("montant doit être strictement positif")));

        ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> offsets = ArgumentCaptor.forClass(Map.class);
        verify(modele).sendOffsetsToTransaction(offsets.capture(), any());
        assertThat(offsets.getValue()).containsOnlyKeys(new TopicPartition("cotisations.emises", 0));
        // Offset du message récupéré (5) + 1 : le prochain à lire.
        assertThat(offsets.getValue().get(new TopicPartition("cotisations.emises", 0)).offset()).isEqualTo(6L);
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

        int livraisons = livraisonsJusquAuRejet(traitement(modele), modele, new ListenerExecutionFailedException(
                "échec du listener", new IllegalStateException("base indisponible")));

        assertThat(livraisons).isEqualTo(5);
        verify(modele, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    void une_seule_livraison_pour_une_PrelevementInvalideException_puis_un_envoi_au_topic_de_rejets() {
        KafkaTemplate<Object, Object> modele = modeleDeRejetsFactice();

        int livraisons = livraisonsJusquAuRejet(traitement(modele), modele, new ListenerExecutionFailedException(
                "échec du listener", new PrelevementInvalideException("montant doit être strictement positif")));

        assertThat(livraisons).isEqualTo(1);
        verify(modele, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    void une_seule_livraison_pour_un_message_illisible_puis_un_envoi_au_topic_de_rejets() {
        KafkaTemplate<Object, Object> modele = modeleDeRejetsFactice();

        int livraisons = livraisonsJusquAuRejet(traitement(modele), modele, new ListenerExecutionFailedException("échec du listener",
                new DeserializationException("failed to deserialize", new byte[] {1, 2, 3}, false,
                        new IllegalStateException("octet magique inconnu"))));

        assertThat(livraisons).isEqualTo(1);
        verify(modele, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    void notifie_le_retry_listener_des_echecs_de_livraison_puis_de_la_recuperation() {
        KafkaTemplate<Object, Object> modele = modeleDeRejetsFactice();
        DefaultAfterRollbackProcessor<Object, Object> traitement = traitement(modele);
        List<String> evenements = new ArrayList<>();
        traitement.setRetryListeners(new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
                evenements.add("echec " + deliveryAttempt);
            }

            @Override
            public void recovered(ConsumerRecord<?, ?> record, Exception ex) {
                evenements.add("recupere");
            }
        });

        livraisonsJusquAuRejet(traitement, modele, new ListenerExecutionFailedException(
                "échec du listener", new IllegalStateException("base indisponible")));

        // Constaté : failedDelivery est notifié à CHAQUE livraison échouée, y compris la dernière (la 5e), avant la
        // récupération. Le nombre de livraisons est donc le nombre de failedDelivery (et non ce nombre plus un).
        assertThat(evenements).containsExactly("echec 1", "echec 2", "echec 3", "echec 4", "echec 5", "recupere");
    }
}
