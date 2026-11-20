package io.github.abdeljalil59.mutuelle.prelevement.adapter.in.kafka;

import io.github.abdeljalil59.mutuelle.prelevement.OracleIntegrationTest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.github.abdeljalil59.mutuelle.events.avro.CotisationEmise;
import io.github.abdeljalil59.mutuelle.prelevement.AvroTestUtil;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.micrometer.metrics.test.autoconfigure.AutoConfigureMetrics;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

// Le service ne crée pas le topic : il est déclaré ici (3 partitions, comme en production).
// Le test ne doit jamais atteindre le broker du compose (localhost:9092) : voir le garde-fou.
// Registre de schémas factice (mock://) partagé entre le service et le producteur de test.
// Serveur web sur un port aléatoire et export Prometheus réel (@AutoConfigureMetrics) : sans cette annotation, un
// @SpringBootTest n'autoconfigure aucun registre autre qu'en mémoire (doc Spring Boot, testing) et /actuator/prometheus
// n'existe pas.
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.properties.schema.registry.url=" + AvroTestUtil.REGISTRE})
@AutoConfigureMetrics
@EmbeddedKafka(topics = "cotisations.emises", partitions = 3, brokerProperties = {
        "transaction.state.log.replication.factor=1",
        "transaction.state.log.min.isr=1"})
@DirtiesContext
class CotisationEmiseListenerIntegrationTest extends OracleIntegrationTest {

    private static final String TOPIC = "cotisations.emises";
    // Messages envoyés par toute la classe : sert de référence au test de commit, quel que soit l'ordre des tests.
    private static final AtomicInteger MESSAGES_ENVOYES = new AtomicInteger();

    private final Logger logger = (Logger) LoggerFactory.getLogger(CotisationEmiseListener.class);
    private final ListAppender<ILoggingEvent> journal = new ListAppender<>();

    private DefaultKafkaProducerFactory<String, CotisationEmise> usine;
    private KafkaTemplate<String, CotisationEmise> producteur;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${local.server.port}")
    private int portWeb;

    @BeforeEach
    void prepare_le_journal_et_le_producteur() {
        // Garde-fou : le contexte doit pointer vers le broker embarqué, jamais vers localhost:9092.
        assertThat(bootstrapServers).isEqualTo(embeddedKafka.getBrokersAsString());

        // L'écouteur journalise depuis un autre thread : liste thread-safe.
        journal.list = new CopyOnWriteArrayList<>();
        logger.setLevel(Level.INFO);
        journal.start();
        logger.addAppender(journal);

        // Producteur de test : événements Avro (KafkaAvroSerializer, registre factice).
        usine = AvroTestUtil.usineAvro(embeddedKafka);
        producteur = new KafkaTemplate<>(usine);
    }

    @AfterEach
    void libere_le_journal_et_le_producteur() {
        logger.detachAppender(journal);
        journal.stop();
        usine.destroy();
    }

    @Test
    void consomme_l_evenement_avro_du_contrat() throws Exception {
        UUID idEvenement = UUID.randomUUID();

        envoyer("adh-avro", evenement(idEvenement, "adh-avro", "50.00", "2026-11-16T10:48:01.506356Z"));

        assertThat(attendreDecision("adh-avro")).matches(
                "partition=\\d+ offset=\\d+ cle=adh-avro idEvenement=" + idEvenement
                        + " montant=50.00 decision=ACCEPTE");
    }

    @Test
    void applique_la_regle_a_la_limite_du_plafond() throws Exception {
        envoyer("adh-limite-egale", evenement(UUID.randomUUID(), "adh-limite-egale", "100.00", "2026-11-16T10:48:01Z"));
        envoyer("adh-limite-depassee", evenement(UUID.randomUUID(), "adh-limite-depassee", "100.01", "2026-11-16T10:48:01Z"));

        assertThat(attendreDecision("adh-limite-egale")).contains("montant=100.00").endsWith("decision=ACCEPTE");
        assertThat(attendreDecision("adh-limite-depassee")).contains("montant=100.01").endsWith("decision=REJETE");
    }

    @Test
    void commite_les_offsets_du_groupe() throws Exception {
        for (int i = 1; i <= 3; i++) {
            String cle = "adh-commit-" + i;
            envoyer(cle, evenement(UUID.randomUUID(), cle, "10.00", "2026-11-16T10:48:01Z"));
        }
        for (int i = 1; i <= 3; i++) {
            attendreDecision("adh-commit-" + i);
        }

        // Le commit (AckMode BATCH) suit le traitement : on attend qu'il soit visible côté broker.
        await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> assertThat(sommeDesOffsetsCommites()).isEqualTo(MESSAGES_ENVOYES.get()));
    }

    // Le scrape de Prometheus : GET /actuator/prometheus sur le vrai serveur web du contexte, après un vrai message.
    @Test
    void expose_les_metriques_au_format_prometheus() throws Exception {
        envoyer("adh-metriques", evenement(UUID.randomUUID(), "adh-metriques", "50.00", "2026-11-16T10:48:01Z"));
        attendreDecision("adh-metriques");

        // Le timer du listener est enregistré juste après le retour du listener : on attend qu'il apparaisse.
        await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(200)).untilAsserted(() -> {
            HttpResponse<String> reponse = lireLesMetriques();

            assertThat(reponse.statusCode()).isEqualTo(200);
            assertThat(nomsDeMetriques(reponse.body())).contains(
                    "jvm_memory_used_bytes",
                    "hikaricp_connections_active",
                    "spring_kafka_listener_seconds_count",
                    // Lag maximum du client, présent dès la création du consommateur. Le lag par partition
                    // (records_lag) n'est lié qu'à une actualisation périodique de Micrometer : non asserté ici.
                    "kafka_consumer_fetch_manager_records_lag_max");
            // Compteur custom de JournalDecisionsJdbc. La série existe dès le démarrage, à 0 : sa seule présence ne
            // prouverait rien, c'est sa valeur après le message (50.00 sous le plafond, donc ACCEPTE) qui le prouve.
            assertThat(valeurDeLaSerie(reponse.body(), "prelevement_decisions_total{decision=\"ACCEPTE\"}"))
                    .isGreaterThanOrEqualTo(1.0);
        });
    }

    // Valeur d'une série du texte Prometheus identifiée par son nom et ses étiquettes exacts, ou -1 si elle est absente.
    private static double valeurDeLaSerie(String corps, String serie) {
        return corps.lines()
                .filter(ligne -> ligne.startsWith(serie + " "))
                .map(ligne -> Double.parseDouble(ligne.substring(serie.length() + 1).split(" ")[0]))
                .findFirst()
                .orElse(-1.0);
    }

    private HttpResponse<String> lireLesMetriques() throws Exception {
        HttpRequest requete = HttpRequest.newBuilder(URI.create("http://localhost:" + portWeb + "/actuator/prometheus"))
                .GET().build();
        return HttpClient.newHttpClient().send(requete, HttpResponse.BodyHandlers.ofString());
    }

    // Noms des séries du texte Prometheus (sans les commentaires # HELP / # TYPE, sans les étiquettes ni la valeur).
    private static Set<String> nomsDeMetriques(String corps) {
        return corps.lines()
                .filter(ligne -> !ligne.isBlank() && !ligne.startsWith("#"))
                .map(ligne -> ligne.split("[{ ]", 2)[0])
                .collect(Collectors.toSet());
    }

    private void envoyer(String cle, CotisationEmise evenement) throws Exception {
        producteur.send(TOPIC, cle, evenement).get(10, TimeUnit.SECONDS);
        MESSAGES_ENVOYES.incrementAndGet();
    }

    private static CotisationEmise evenement(UUID idEvenement, String idAdherent, String montant, String emiseLe) {
        return AvroTestUtil.evenement(idEvenement, idAdherent, montant, Instant.parse(emiseLe));
    }

    // Ligne de journal de la décision pour cette clé (l'espace final évite de confondre deux clés préfixées).
    private String attendreDecision(String cle) {
        return await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(100))
                .until(() -> journal.list.stream()
                                .map(ILoggingEvent::getFormattedMessage)
                                .filter(ligne -> ligne.contains("cle=" + cle + " "))
                                .findFirst()
                                .orElse(null),
                        Objects::nonNull);
    }

    private long sommeDesOffsetsCommites() throws Exception {
        try (Admin admin = Admin.create(Map.<String, Object>of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString()))) {
            Map<TopicPartition, OffsetAndMetadata> offsets = admin.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
            return offsets.entrySet().stream()
                    .filter(entree -> entree.getKey().topic().equals(TOPIC))
                    .mapToLong(entree -> entree.getValue().offset())
                    .sum();
        }
    }
}
