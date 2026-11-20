package io.github.abdeljalil59.mutuelle.prelevement.adapter.in.kafka;

import io.github.abdeljalil59.mutuelle.prelevement.OracleIntegrationTest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.github.abdeljalil59.mutuelle.events.avro.CotisationEmise;
import io.github.abdeljalil59.mutuelle.prelevement.AvroTestUtil;
import io.github.abdeljalil59.mutuelle.prelevement.application.port.out.JournalDecisions;
import io.github.abdeljalil59.mutuelle.prelevement.domain.DecisionPrelevement;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AfterRollbackProcessor;
import org.springframework.kafka.listener.DefaultAfterRollbackProcessor;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

// Seul le topic source est déclaré ici : le topic de rejets (cotisations.emises-dlt) est créé par le service.
// Le test ne doit jamais atteindre le broker du compose (localhost:9092) : voir le garde-fou.
// Aucune lecture de logs : la base, le topic de rejets et les offsets commités du groupe suffisent.
// Les événements sont de l'Avro (registre factice mock://), le rejet d'une exception du listener aussi.
// Pauses de réessai ramenées de 1000 à 10 ms (10, 20, 40, 80 ms) pour ne pas attendre 15 s par échec : le nombre de
// livraisons ne dépend pas de la durée des pauses. Les vraies pauses (1, 2, 4, 8 s) sont vérifiées sur le BackOff,
// dans ErreursKafkaConfigTest, à partir de application.yml.
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.properties.schema.registry.url=" + AvroTestUtil.REGISTRE,
        "kafka.error-handler.initial-interval=10"})
@EmbeddedKafka(topics = "cotisations.emises", partitions = 3, brokerProperties = {
        "transaction.state.log.replication.factor=1",
        "transaction.state.log.min.isr=1"})
@DirtiesContext
class PersistanceIntegrationTest extends OracleIntegrationTest {

    private static final String TOPIC = "cotisations.emises";
    private static final String TOPIC_REJETS = "cotisations.emises-dlt";
    // Oracle : ORA-12899 (valeur trop longue) n est pas classé par Spring parmi les violations d intégrité.
    private static final String CAUSE_ATTENDUE = "org.springframework.jdbc.UncategorizedSQLException";
    // Délai des appels à l'Admin Kafka : 5 s ne suffisent pas quand la machine est chargée (TimeoutException constatée).
    private static final Duration TIMEOUT_ADMIN = Duration.ofSeconds(30);

    private static final UUID ID_DOUBLON = UUID.fromString("bbbbbbbb-0000-0000-0000-000000000011");
    private static final UUID ID_ECHEC = UUID.fromString("bbbbbbbb-0000-0000-0000-000000000012");
    private static final UUID ID_VALIDE_APRES_ECHEC = UUID.fromString("bbbbbbbb-0000-0000-0000-000000000013");
    private static final UUID ID_CINQ_LIVRAISONS = UUID.fromString("bbbbbbbb-0000-0000-0000-000000000014");

    // Une vraie erreur Oracle (ORA-12899) : la colonne id_adherent est limitée à 64 caractères.
    private static final String ID_ADHERENT_TROP_LONG = "x".repeat(65);

    // Messages envoyés par toute la classe : référence de l'attente des offsets, quel que soit l'ordre des tests.
    private static final AtomicInteger MESSAGES_ENVOYES = new AtomicInteger();

    private DefaultKafkaProducerFactory<String, CotisationEmise> usine;
    private KafkaTemplate<String, CotisationEmise> producteur;
    private KafkaConsumer<String, byte[]> consommateurRejets;
    private CompteurDeLivraisons livraisons;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private KafkaListenerEndpointRegistry registreDesListeners;

    // Le même processeur que celui du conteneur : on y pose un RetryListener pour compter les livraisons.
    @Autowired
    private AfterRollbackProcessor<Object, Object> traitementApresRollback;

    // Espion sur le port : il appelle le vrai adaptateur JDBC et compte les appels.
    @MockitoSpyBean
    private JournalDecisions journalDecisions;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @BeforeEach
    void prepare_le_producteur_et_le_consommateur_des_rejets() {
        // Garde-fou : le contexte doit pointer vers le broker embarqué, jamais vers localhost:9092.
        assertThat(bootstrapServers).isEqualTo(embeddedKafka.getBrokersAsString());

        // Producteur de test : événements Avro (KafkaAvroSerializer, registre factice).
        usine = AvroTestUtil.usineAvro(embeddedKafka);
        producteur = new KafkaTemplate<>(usine);

        livraisons = new CompteurDeLivraisons();
        ((DefaultAfterRollbackProcessor<?, ?>) traitementApresRollback).setRetryListeners(livraisons);

        // Lecteur du DLT : sans commit ; ses partitions sont assignées à la demande (voir lireLesRejetsDeLaCle).
        consommateurRejets = new KafkaConsumer<>(Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString(),
                ConsumerConfig.GROUP_ID_CONFIG, "lecteur-rejets-" + UUID.randomUUID(),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false),
                new StringDeserializer(), new ByteArrayDeserializer());
    }

    @AfterEach
    void libere_le_producteur_et_le_consommateur() {
        ((DefaultAfterRollbackProcessor<?, ?>) traitementApresRollback).setRetryListeners();
        consommateurRejets.close();
        usine.destroy();
    }

    @Test
    void doublon_meme_idEvenement_donne_une_seule_ligne() throws Exception {
        String cle = "cle-doublon";
        CotisationEmise message = evenement(ID_DOUBLON, "adh-doublon", "50.00");

        // Le même message deux fois : même clé, donc même partition.
        envoyer(cle, message);
        envoyer(cle, message);

        attendreQueLeGroupeAitToutCommite();

        assertThat(nombreDeLignes(ID_DOUBLON)).isEqualTo(1);
        assertThat(decisionEnBase(ID_DOUBLON)).isEqualTo("ACCEPTE");
    }

    @Test
    void echec_base_puis_message_valide() throws Exception {
        // Même clé pour les deux messages : même partition, le valide est derrière l'invalide.
        String cle = "cle-echec-base";

        envoyer(cle, evenement(ID_ECHEC, ID_ADHERENT_TROP_LONG, "50.00"));
        envoyer(cle, evenement(ID_VALIDE_APRES_ECHEC, "adh-valide", "50.00"));

        attendreQueLeGroupeAitToutCommite();
        List<ConsumerRecord<String, byte[]>> rejets = lireLesRejetsDeLaCle(cle);

        assertThat(nombreDeLignes(ID_ECHEC)).isZero();
        assertThat(nombreDeLignes(ID_VALIDE_APRES_ECHEC)).isEqualTo(1);
        assertThat(decisionEnBase(ID_VALIDE_APRES_ECHEC)).isEqualTo("ACCEPTE");

        // Exactement un rejet pour cette clé : celui du message invalide (pas le valide, d'un autre idEvenement),
        // republié en Avro par le KafkaAvroSerializer du DelegatingByTypeSerializer.
        assertThat(rejets).hasSize(1);
        ConsumerRecord<String, byte[]> rejet = rejets.get(0);
        CotisationEmise rejete = AvroTestUtil.decoder(TOPIC_REJETS, rejet.value());
        assertThat(rejete.getIdEvenement()).isEqualTo(ID_ECHEC);
        assertThat(rejete.getIdAdherent()).isEqualTo(ID_ADHERENT_TROP_LONG);
        assertThat(texte(rejet, "kafka_dlt-original-topic")).isEqualTo(TOPIC);
        assertCauseDeLEchecDansLeRejet(rejet);
    }

    @Test
    void cinq_livraisons_avant_dlt() throws Exception {
        String cle = "cle-cinq-livraisons";

        envoyer(cle, evenement(ID_CINQ_LIVRAISONS, ID_ADHERENT_TROP_LONG, "50.00"));

        attendreQueLeGroupeAitToutCommite();
        List<ConsumerRecord<String, byte[]>> rejets = lireLesRejetsDeLaCle(cle);

        assertThat(rejets).hasSize(1);
        // 1 livraison initiale + 4 réessais (ExponentialBackOffWithMaxRetries, kafka.error-handler.max-retries) avant
        // l'envoi au DLT : un appel du port par livraison. Le décompte est limité à cet idEvenement : l'espion est partagé
        // par tous les tests de la classe. Deux mesures indépendantes : les appels du port, et les notifications du
        // RetryListener.
        verify(journalDecisions, times(5)).enregistrer(argThat(decisions -> contient(decisions, ID_CINQ_LIVRAISONS)));
        assertThat(livraisons.livraisonsEchouees(cle)).as(livraisons.toString()).isEqualTo(5);
        assertThat(livraisons.envoisAuTopicDeRejets(cle)).as(livraisons.toString()).isEqualTo(1);
    }

    // Les trois messages sont reçus au même poll (conteneur arrêté pendant l'envoi) : en mode record, chacun est traité
    // séparément, seul le message en échec est réessayé puis envoyé au DLT, et ses voisins sont écrits normalement.
    @Test
    void echec_entre_deux_messages_valides_ecrit_les_voisins_et_envoie_seul_le_message_en_echec_au_dlt() throws Exception {
        String cle = "cle-lot-echec";
        UUID avant = idLot(11);
        UUID enEchec = idLot(12);
        UUID apres = idLot(13);

        envoyerPendantArretDuConteneur(cle, List.of(
                evenement(avant, "adh-avant", "50.00"),
                evenement(enEchec, ID_ADHERENT_TROP_LONG, "50.00"),
                evenement(apres, "adh-apres", "50.00")));

        attendreQueLeGroupeAitToutCommite();
        List<ConsumerRecord<String, byte[]>> rejets = lireLesRejetsDeLaCle(cle);

        assertThat(nombreDeLignes(avant)).isEqualTo(1);
        assertThat(nombreDeLignes(enEchec)).isZero();
        assertThat(nombreDeLignes(apres)).isEqualTo(1);
        assertThat(rejets).hasSize(1);
        assertThat(AvroTestUtil.decoder(TOPIC_REJETS, rejets.get(0).value()).getIdEvenement()).isEqualTo(enEchec);
        assertCauseDeLEchecDansLeRejet(rejets.get(0));
    }

    // Listener par record : le recoverer reçoit un ListenerExecutionFailedException dont la cause directe est l'erreur
    // Oracle d'origine, que l'en-tête du DLT porte directement.
    private static void assertCauseDeLEchecDansLeRejet(ConsumerRecord<String, byte[]> rejet) {
        assertThat(texte(rejet, "kafka_dlt-exception-cause-fqcn")).isEqualTo(CAUSE_ATTENDUE);
    }

    private void envoyer(String cle, CotisationEmise evenement) throws Exception {
        producteur.send(TOPIC, cle, evenement).get(10, TimeUnit.SECONDS);
        MESSAGES_ENVOYES.incrementAndGet();
    }

    // Le conteneur est arrêté pendant l'envoi puis redémarré : le premier poll qui suit ramène tous les messages
    // (même clé, donc même partition), que le listener traite ensuite un par un.
    private void envoyerPendantArretDuConteneur(String cle, List<CotisationEmise> evenements) throws Exception {
        registreDesListeners.getListenerContainers().forEach(MessageListenerContainer::stop);
        try {
            for (CotisationEmise evenement : evenements) {
                envoyer(cle, evenement);
            }
        } finally {
            registreDesListeners.getListenerContainers().forEach(MessageListenerContainer::start);
        }
    }

    private static UUID idLot(int numero) {
        return UUID.fromString(String.format("bbbbbbbb-0000-0000-0000-%012d", 100 + numero));
    }

    private static boolean contient(List<DecisionPrelevement> decisions, UUID idEvenement) {
        return decisions.stream().anyMatch(decision -> idEvenement.equals(decision.idEvenement()));
    }

    private static CotisationEmise evenement(UUID idEvenement, String idAdherent, String montant) {
        return AvroTestUtil.evenement(idEvenement, idAdherent, montant, Instant.parse("2026-11-18T08:00:00Z"));
    }

    private int nombreDeLignes(UUID idEvenement) {
        return jdbcClient.sql("SELECT COUNT(*) FROM decision_prelevement WHERE id_evenement = ?")
                .param(idEvenement.toString())
                .query(Integer.class)
                .single();
    }

    private String decisionEnBase(UUID idEvenement) {
        return jdbcClient.sql("SELECT decision FROM decision_prelevement WHERE id_evenement = ?")
                .param(idEvenement.toString())
                .query(String.class)
                .single();
    }

    // Chaque message envoyé (traité ou rejeté) doit être dépassé par le groupe : les offsets commités
    // valent ensemble le nombre de messages envoyés par la classe. Le commit suit le traitement (AckMode BATCH) :
    // une fois cette attente terminée, les lignes de base sont écrites et les rejets publiés.
    private void attendreQueLeGroupeAitToutCommite() {
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> assertThat(sommeDesOffsetsCommites()).isEqualTo(MESSAGES_ENVOYES.get()));
    }

    // Les tests partagent le DLT : filtre par clé, propre à chaque test.
    // À appeler après attendreQueLeGroupeAitToutCommite() : le rejet est publié avant le commit de son offset,
    // donc les offsets de fin relevés ici (après le commit) incluent tout rejet déjà publié. Lire jusqu'à ces
    // offsets rend la lecture complète : l'absence d'un message est alors une preuve, pas l'effet d'une fenêtre
    // de temps trop courte.
    private List<ConsumerRecord<String, byte[]>> lireLesRejetsDeLaCle(String cle) {
        List<TopicPartition> partitions = consommateurRejets.partitionsFor(TOPIC_REJETS).stream()
                .map(info -> new TopicPartition(info.topic(), info.partition()))
                .toList();
        consommateurRejets.assign(partitions);
        consommateurRejets.seekToBeginning(partitions);
        Map<TopicPartition, Long> offsetsDeFin = consommateurRejets.endOffsets(partitions);

        List<ConsumerRecord<String, byte[]>> rejets = new ArrayList<>();
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            consommateurRejets.poll(Duration.ofMillis(200)).forEach(rejets::add);
            return offsetsDeFin.entrySet().stream()
                    .allMatch(fin -> consommateurRejets.position(fin.getKey()) >= fin.getValue());
        });
        return rejets.stream().filter(rejet -> cle.equals(rejet.key())).toList();
    }

    private static String texte(ConsumerRecord<?, ?> rejet, String entete) {
        Header header = rejet.headers().lastHeader(entete);
        assertThat(header).as("en-tête %s", entete).isNotNull();
        return new String(header.value(), StandardCharsets.UTF_8);
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
