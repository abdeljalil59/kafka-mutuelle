package io.github.abdeljalil59.mutuelle.prelevement.adapter.in.kafka;

import static io.github.abdeljalil59.mutuelle.prelevement.adapter.in.kafka.SupportIntegration.decisionEnBase;
import static io.github.abdeljalil59.mutuelle.prelevement.adapter.in.kafka.SupportIntegration.evenement;
import static io.github.abdeljalil59.mutuelle.prelevement.adapter.in.kafka.SupportIntegration.nombreDeLignes;
import static io.github.abdeljalil59.mutuelle.prelevement.adapter.in.kafka.SupportIntegration.texte;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.github.abdeljalil59.mutuelle.events.ResultatPrelevement;
import io.github.abdeljalil59.mutuelle.events.TypeResultat;
import io.github.abdeljalil59.mutuelle.events.avro.CotisationEmise;
import io.github.abdeljalil59.mutuelle.prelevement.AvroTestUtil;
import io.github.abdeljalil59.mutuelle.prelevement.OracleIntegrationTest;
import io.github.abdeljalil59.mutuelle.prelevement.application.PublicationResultatImpossibleException;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AfterRollbackProcessor;
import org.springframework.kafka.listener.DefaultAfterRollbackProcessor;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

// Publication des résultats sur prelevements.resultats, de bout en bout : Kafka embarqué, registre factice (mock://) et
// Oracle du conteneur partagé, comme les autres intégrations du service. Pas de transaction Kafka à ce stade.
// Le plafond est de 100,00 (application.yml) : jusqu'à 100,00 inclus la décision est ACCEPTE, au-delà REJETE.
// Pauses de réessai ramenées à 10 ms : le nombre de livraisons ne dépend pas de leur durée.
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.properties.schema.registry.url=" + AvroTestUtil.REGISTRE,
        "kafka.error-handler.initial-interval=10"})
@EmbeddedKafka(topics = "cotisations.emises", partitions = 3, brokerProperties = {
        "transaction.state.log.replication.factor=1",
        "transaction.state.log.min.isr=1"})
@DirtiesContext
class PublicationResultatsIntegrationTest extends OracleIntegrationTest {

    private static final String TOPIC = "cotisations.emises";
    private static final String TOPIC_RESULTATS = "prelevements.resultats";
    private static final String TOPIC_REJETS = "cotisations.emises-dlt";
    private static final Duration TIMEOUT_ADMIN = Duration.ofSeconds(30);

    // Messages envoyés par toute la classe : référence de l'attente des offsets, quel que soit l'ordre des tests.
    private static final AtomicInteger MESSAGES_ENVOYES = new AtomicInteger();

    private DefaultKafkaProducerFactory<String, CotisationEmise> usine;
    private KafkaTemplate<String, CotisationEmise> producteur;
    private CompteurDeLivraisons livraisons;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Autowired
    private JdbcClient jdbcClient;

    // Le même processeur que celui du conteneur : on y pose un RetryListener pour compter les livraisons.
    @Autowired
    private AfterRollbackProcessor<Object, Object> traitementApresRollback;

    // Espion sur le template des résultats : il appelle le vrai template, sauf là où un test fait échouer l'envoi. Le nom
    // est obligatoire : le template de Boot (kafkaTemplate, celui du topic de rejets) est aussi un KafkaOperations.
    @MockitoSpyBean(name = "resultatsKafkaTemplate")
    private KafkaOperations<Object, Object> resultatsKafkaTemplate;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @BeforeEach
    void prepare_le_producteur_et_le_compteur() {
        usine = AvroTestUtil.usineAvro(embeddedKafka);
        producteur = new KafkaTemplate<>(usine);
        livraisons = new CompteurDeLivraisons();
        ((DefaultAfterRollbackProcessor<?, ?>) traitementApresRollback).setRetryListeners(livraisons);
    }

    @AfterEach
    void libere_le_producteur_et_le_compteur() {
        ((DefaultAfterRollbackProcessor<?, ?>) traitementApresRollback).setRetryListeners();
        usine.destroy();
    }

    @Test
    void un_message_accepte_est_persiste_puis_publie_sur_le_topic_des_resultats() throws Exception {
        String adherent = "adh-res-accepte";
        UUID idEvenement = UUID.randomUUID();

        envoyer(adherent, evenement(idEvenement, adherent, "75.00"));

        attendreQueLeGroupeAitToutCommite();
        List<ConsumerRecord<String, ResultatPrelevement>> resultats = lireLesResultatsDeLaCle(adherent);

        assertThat(resultats).hasSize(1);
        ConsumerRecord<String, ResultatPrelevement> message = resultats.get(0);
        assertThat(message.key()).isEqualTo(adherent);
        ResultatPrelevement resultat = message.value();
        assertThat(resultat.getType()).isEqualTo(TypeResultat.ACCEPTE);
        assertThat(resultat.getIdAdherent()).isEqualTo(adherent);
        assertThat(resultat.getMontant()).isEqualTo(new BigDecimal("75.00"));
        assertThat(resultat.getIdEvenement()).isEqualTo(idEvenement);
        assertThat(resultat.getDecideLe()).isNotNull();
        assertThat(nombreDeLignes(jdbcClient, idEvenement)).isEqualTo(1);
        assertThat(decisionEnBase(jdbcClient, idEvenement)).isEqualTo("ACCEPTE");
    }

    @Test
    void un_message_au_dela_du_plafond_est_publie_avec_le_type_REJETE() throws Exception {
        String adherent = "adh-res-rejete";
        UUID idEvenement = UUID.randomUUID();

        envoyer(adherent, evenement(idEvenement, adherent, "150.00"));

        attendreQueLeGroupeAitToutCommite();
        List<ConsumerRecord<String, ResultatPrelevement>> resultats = lireLesResultatsDeLaCle(adherent);

        assertThat(resultats).hasSize(1);
        assertThat(resultats.get(0).key()).isEqualTo(adherent);
        ResultatPrelevement resultat = resultats.get(0).value();
        assertThat(resultat.getType()).isEqualTo(TypeResultat.REJETE);
        assertThat(resultat.getIdAdherent()).isEqualTo(adherent);
        assertThat(resultat.getMontant()).isEqualTo(new BigDecimal("150.00"));
        assertThat(resultat.getIdEvenement()).isEqualTo(idEvenement);
        assertThat(resultat.getDecideLe()).isNotNull();
        assertThat(decisionEnBase(jdbcClient, idEvenement)).isEqualTo("REJETE");
    }

    // D4 : le MERGE ignore le doublon, mais la décision est republiée, avec le même idEvenement et le même type.
    // Un message déjà accepté ne devient pas REJETE parce qu'il est rejoué.
    @Test
    void un_message_rejoue_est_republie_a_l_identique_et_n_ajoute_aucune_ligne() throws Exception {
        String adherent = "adh-res-replay";
        UUID idEvenement = UUID.randomUUID();
        CotisationEmise cotisation = evenement(idEvenement, adherent, "75.00");

        envoyer(adherent, cotisation);
        envoyer(adherent, cotisation);

        attendreQueLeGroupeAitToutCommite();
        List<ConsumerRecord<String, ResultatPrelevement>> resultats = lireLesResultatsDeLaCle(adherent);

        assertThat(resultats).hasSize(2);
        assertThat(resultats).extracting(message -> message.value().getIdEvenement()).containsOnly(idEvenement);
        assertThat(resultats).extracting(message -> message.value().getType()).containsOnly(TypeResultat.ACCEPTE);
        assertThat(nombreDeLignes(jdbcClient, idEvenement)).isEqualTo(1);
    }

    // D14, reproductible : l'envoi sur prelevements.resultats échoue à chaque livraison, après le MERGE. Le message est
    // réessayé (5 livraisons), puis part au topic de rejets ; la décision, elle, reste écrite en base et aucun résultat
    // n'a été publié.
    @Test
    void un_echec_de_publication_apres_le_merge_envoie_le_message_au_dlt_mais_garde_la_ligne() throws Exception {
        String adherent = "adh-res-echec";
        UUID idEvenement = UUID.randomUUID();
        doReturn(CompletableFuture.<SendResult<Object, Object>>failedFuture(new IllegalStateException("broker injoignable")))
                .when(resultatsKafkaTemplate).send(eq(TOPIC_RESULTATS), eq(adherent), any());

        envoyer(adherent, evenement(idEvenement, adherent, "75.00"));

        attendreQueLeGroupeAitToutCommite();
        List<ConsumerRecord<String, byte[]>> rejets = lireLesRejetsDeLaCle(adherent);

        // 1 livraison initiale + 4 réessais, puis un seul envoi au topic de rejets.
        assertThat(livraisons.livraisonsEchouees(adherent)).as(livraisons.toString()).isEqualTo(5);
        assertThat(livraisons.envoisAuTopicDeRejets(adherent)).as(livraisons.toString()).isEqualTo(1);
        verify(resultatsKafkaTemplate, times(5)).send(eq(TOPIC_RESULTATS), eq(adherent), any());
        assertThat(rejets).hasSize(1);
        CotisationEmise rejete = AvroTestUtil.decoder(TOPIC_REJETS, rejets.get(0).value());
        assertThat(rejete.getIdEvenement()).isEqualTo(idEvenement);
        assertThat(texte(rejets.get(0), "kafka_dlt-exception-cause-fqcn"))
                .isEqualTo(PublicationResultatImpossibleException.class.getName());
        // Écrit avant la publication, donc conservé malgré l'échec ; aucun résultat n'est sorti.
        assertThat(nombreDeLignes(jdbcClient, idEvenement)).isEqualTo(1);
        assertThat(lireLesResultatsDeLaCle(adherent)).isEmpty();
    }

    private void envoyer(String cle, CotisationEmise evenement) throws Exception {
        producteur.send(TOPIC, cle, evenement).get(10, TimeUnit.SECONDS);
        MESSAGES_ENVOYES.incrementAndGet();
    }

    // Chaque message envoyé (traité ou rejeté) doit être dépassé par le groupe : les offsets commités valent ensemble le
    // nombre de messages envoyés par la classe. Le commit suit le traitement : une fois cette attente terminée, la ligne
    // est écrite, le résultat publié (ou le rejet publié au DLT).
    private void attendreQueLeGroupeAitToutCommite() {
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> assertThat(sommeDesOffsetsCommites()).isEqualTo(MESSAGES_ENVOYES.get()));
    }

    // Relit un topic depuis le début jusqu'aux offsets de fin relevés à l'instant de l'appel, puis ne garde que la clé donnée.
    // À appeler après attendreQueLeGroupeAitToutCommite() : la publication précède le commit de l'offset, donc tout ce qui
    // a été publié est avant la fin relevée ici, et l'absence d'un message est une preuve, pas l'effet d'un délai trop court.
    private <V> List<ConsumerRecord<String, V>> lireLaCle(
            Map<String, Object> proprietes, String topic, String cle) {
        try (KafkaConsumer<String, V> consommateur = new KafkaConsumer<>(proprietes)) {
            List<TopicPartition> partitions = consommateur.partitionsFor(topic).stream()
                    .map(info -> new TopicPartition(info.topic(), info.partition()))
                    .toList();
            consommateur.assign(partitions);
            consommateur.seekToBeginning(partitions);
            Map<TopicPartition, Long> offsetsDeFin = consommateur.endOffsets(partitions);

            List<ConsumerRecord<String, V>> lus = new ArrayList<>();
            await().atMost(Duration.ofSeconds(15)).until(() -> {
                consommateur.poll(Duration.ofMillis(200)).forEach(lus::add);
                return offsetsDeFin.entrySet().stream()
                        .allMatch(fin -> consommateur.position(fin.getKey()) >= fin.getValue());
            });
            return lus.stream().filter(message -> cle.equals(message.key())).toList();
        }
    }

    private List<ConsumerRecord<String, ResultatPrelevement>> lireLesResultatsDeLaCle(String cle) {
        Map<String, Object> proprietes = proprietesLecteur(KafkaAvroDeserializer.class);
        proprietes.put("schema.registry.url", AvroTestUtil.REGISTRE);
        proprietes.put("specific.avro.reader", true);
        return lireLaCle(proprietes, TOPIC_RESULTATS, cle);
    }

    private List<ConsumerRecord<String, byte[]>> lireLesRejetsDeLaCle(String cle) {
        return lireLaCle(proprietesLecteur(ByteArrayDeserializer.class), TOPIC_REJETS, cle);
    }

    // Lecteur de test : sans commit, groupe à usage unique.
    private Map<String, Object> proprietesLecteur(Class<?> deserialiseurDeValeur) {
        Map<String, Object> proprietes = new HashMap<>();
        proprietes.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
        proprietes.put(ConsumerConfig.GROUP_ID_CONFIG, "lecteur-" + UUID.randomUUID());
        proprietes.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        proprietes.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        proprietes.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserialiseurDeValeur);
        return proprietes;
    }

    private long sommeDesOffsetsCommites() throws Exception {
        try (Admin admin = Admin.create(Map.<String, Object>of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString()))) {
            Map<TopicPartition, OffsetAndMetadata> offsets = admin.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata().get(TIMEOUT_ADMIN.toSeconds(), TimeUnit.SECONDS);
            return offsets.entrySet().stream()
                    .filter(entree -> entree.getKey().topic().equals(TOPIC))
                    .mapToLong(entree -> entree.getValue().offset())
                    .sum();
        }
    }
}
