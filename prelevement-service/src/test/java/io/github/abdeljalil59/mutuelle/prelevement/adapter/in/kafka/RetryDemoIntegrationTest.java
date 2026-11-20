package io.github.abdeljalil59.mutuelle.prelevement.adapter.in.kafka;

import io.github.abdeljalil59.mutuelle.prelevement.OracleIntegrationTest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

// Démonstration @RetryableTopic active, délais réduits : 500 ms entre chaque tentative (multiplicateur 1).
// Avec des délais identiques, spring-kafka 4.1 réutilise UN SEUL topic de retry (stratégie SINGLE_TOPIC par défaut),
// nommé <topic>-retry sans valeur de délai (déduit du bytecode, constaté ici) ; le multiplicateur 2 de production,
// qui donne deux topics distincts, n'est pas rejoué.
// Seul le topic source est déclaré : les autres topics sont créés par le service. Le test ne doit jamais atteindre
// le broker du compose (localhost:9092) : voir le garde-fou. Aucune lecture de logs, aucun mock : le contenu des
// topics, leurs horodatages et les offsets commités des groupes suffisent.
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "prelevement.kafka.retry-demo.actif=true",
        "prelevement.kafka.retry-demo.backoff-delay=500",
        "prelevement.kafka.retry-demo.backoff-multiplier=1"})
@EmbeddedKafka(topics = "cotisations.emises", partitions = 3, brokerProperties = {
        "transaction.state.log.replication.factor=1",
        "transaction.state.log.min.isr=1"})
@DirtiesContext
class RetryDemoIntegrationTest extends OracleIntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(RetryDemoIntegrationTest.class);
    private static final String TOPIC_SOURCE = "cotisations.emises";
    private static final String TOPIC_REJETS_SOURCE = "cotisations.emises-dlt";
    private static final String TOPIC_DEMO = "cotisations.retry-demo";
    private static final String TOPIC_RETRY = TOPIC_DEMO + "-retry";
    private static final String TOPIC_DLT_DEMO = TOPIC_DEMO + "-dlt";
    private static final List<String> TOPICS_DEMO = List.of(TOPIC_DEMO, TOPIC_RETRY, TOPIC_DLT_DEMO);
    // Les groupes des topics de retry et du DLT portent ce préfixe (le suffixe du topic est ajouté au groupe).
    private static final String PREFIXE_GROUPES_DEMO = "retry-demo-group";
    private static final long DELAI_MS = 500;
    // Délai des appels à l'Admin Kafka : 5 s ne suffisent pas quand la machine est chargée (TimeoutException constatée).
    private static final Duration TIMEOUT_ADMIN = Duration.ofSeconds(30);
    // Marge sur les délais : le délai est calculé juste avant la publication sur le topic suivant.
    private static final long MARGE_MS = 100;

    // Offsets que les groupes doivent avoir commités, pour toute la classe : référence de l'attente,
    // quel que soit l'ordre des tests.
    private static final AtomicInteger COMMITS_DEMO_ATTENDUS = new AtomicInteger();
    private static final AtomicInteger COMMITS_SOURCE_ATTENDUS = new AtomicInteger();

    private DefaultKafkaProducerFactory<String, byte[]> usine;
    private KafkaTemplate<String, byte[]> producteur;
    private KafkaConsumer<String, byte[]> lecteur;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupeService;

    @BeforeEach
    void prepare_le_producteur_et_le_lecteur() {
        // Garde-fou : le contexte doit pointer vers le broker embarqué, jamais vers localhost:9092.
        assertThat(bootstrapServers).isEqualTo(embeddedKafka.getBrokersAsString());

        // Producteur de test : octets bruts, donc aucun en-tête de type JSON.
        usine = new DefaultKafkaProducerFactory<>(
                KafkaTestUtils.producerProps(embeddedKafka), new StringSerializer(), new ByteArraySerializer());
        producteur = new KafkaTemplate<>(usine);

        // Lecteur de topics : sans commit ; ses partitions sont assignées à la demande (voir messages).
        lecteur = new KafkaConsumer<>(Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString(),
                ConsumerConfig.GROUP_ID_CONFIG, "lecteur-retry-" + UUID.randomUUID(),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false),
                new StringDeserializer(), new ByteArrayDeserializer());
    }

    @AfterEach
    void libere_le_producteur_et_le_lecteur() {
        lecteur.close();
        usine.destroy();
    }

    @Test
    void un_message_sans_echec_est_traite_une_fois_sans_passer_par_les_retries() throws Exception {
        String cle = "demo-succes";

        envoyerSurLaDemo(cle, demo(cle, false), 1);   // un commit attendu : le topic principal seul

        attendreLesCommits();
        assertThat(messages(TOPIC_DEMO, cle)).hasSize(1);
        assertThat(messages(TOPIC_RETRY, cle)).isEmpty();
        assertThat(messages(TOPIC_DLT_DEMO, cle)).isEmpty();
    }

    @Test
    void un_echec_simule_est_reessaye_sans_bloquer_puis_arrive_une_fois_au_dlt() throws Exception {
        String cle = "demo-echec";
        byte[] payload = demo(cle, true);

        // Quatre commits : le topic principal (tentative 1), le topic de retry (tentatives 2 et 3), le DLT.
        envoyerSurLaDemo(cle, payload, 4);

        attendreLesCommits();
        ConsumerRecord<String, byte[]> principal = unique(TOPIC_DEMO, cle);
        List<ConsumerRecord<String, byte[]>> retries = messages(TOPIC_RETRY, cle);
        ConsumerRecord<String, byte[]> dlt = unique(TOPIC_DLT_DEMO, cle);

        // Un message n'est publié sur le topic suivant qu'après l'échec de la tentative précédente :
        // deux passages dans le topic de retry (échecs des tentatives 1 et 2), un seul au DLT (échec de la 3e).
        assertThat(retries).hasSize(2);
        ConsumerRecord<String, byte[]> retry1 = retries.get(0);
        ConsumerRecord<String, byte[]> retry2 = retries.get(1);
        LOGGER.info("PREUVE retry : principal p={} o={} t={} | retry#1 p={} o={} t={} | retry#2 p={} o={} t={} "
                        + "| dlt p={} o={} t={} | écarts retry#2-retry#1={} ms, dlt-retry#2={} ms | offsets commités={}",
                principal.partition(), principal.offset(), principal.timestamp(),
                retry1.partition(), retry1.offset(), retry1.timestamp(),
                retry2.partition(), retry2.offset(), retry2.timestamp(),
                dlt.partition(), dlt.offset(), dlt.timestamp(),
                retry2.timestamp() - retry1.timestamp(), dlt.timestamp() - retry2.timestamp(),
                offsetsCommitesParGroupe());
        LOGGER.info("PREUVE en-têtes du message au DLT : {}", resumeEntetes(dlt));
        assertThat(retry1.offset()).isLessThan(retry2.offset());
        // Le payload n'est pas modifié et la partition d'origine est conservée.
        assertThat(List.of(retry1.value(), retry2.value(), dlt.value())).allMatch(v -> Arrays.equals(v, payload));
        assertThat(List.of(retry1.partition(), retry2.partition(), dlt.partition())).containsOnly(principal.partition());
        // Retries non bloquants. Garanti par la doc : "a message will never be processed before its due time".
        // Le premier réessai n'est donc consommé qu'après le délai, et le second passage est publié au moins un délai
        // après le message d'origine. On n'asserte pas l'écart entre deux étapes consécutives : la doc ne dit pas si
        // le délai court depuis l'échec ou depuis l'horodatage d'origine, et 25 ms ont été constatés entre les deux
        // passages du topic de retry quand le message d'origine avait déjà attendu 5 s (démarrage à froid).
        assertThat(retry2.timestamp() - principal.timestamp()).isGreaterThanOrEqualTo(DELAI_MS - MARGE_MS);
        // En-têtes du mécanisme de retry : préfixe kafka_exception- et kafka_original- (et non kafka_dlt-, celui du
        // DLT du DefaultAfterRollbackProcessor de la source), constaté ci-dessus. Le topic d'origine reste le topic principal.
        assertThat(texte(dlt, "kafka_exception-cause-fqcn")).isEqualTo("java.lang.RuntimeException");
        assertThat(texte(dlt, "kafka_original-topic")).isEqualTo(TOPIC_DEMO);
    }

    @Test
    void les_topics_de_la_demo_portent_les_noms_attendus_avec_trois_partitions() throws Exception {
        try (Admin admin = admin()) {
            Set<String> noms = admin.listTopics().names().get(TIMEOUT_ADMIN.toSeconds(), TimeUnit.SECONDS).stream()
                    .filter(nom -> nom.startsWith(TOPIC_DEMO))
                    .collect(Collectors.toSet());

            // Ni plus ni moins : pas de second topic de retry avec des délais identiques.
            assertThat(noms).containsExactlyInAnyOrderElementsOf(TOPICS_DEMO);
            admin.describeTopics(TOPICS_DEMO).allTopicNames().get(TIMEOUT_ADMIN.toSeconds(), TimeUnit.SECONDS)
                    .forEach((nom, description) -> assertThat(description.partitions()).as(nom).hasSize(3));
        }
    }

    @Test
    void le_dlt_de_la_source_et_celui_de_la_demo_fonctionnent_independamment() throws Exception {
        String clePoison = "adh-poison";
        String cleDemo = "demo-cohabitation";
        byte[] illisible = "ceci-n-est-pas-du-json".getBytes(StandardCharsets.UTF_8);

        envoyer(TOPIC_SOURCE, clePoison, illisible);
        COMMITS_SOURCE_ATTENDUS.incrementAndGet();
        envoyerSurLaDemo(cleDemo, demo(cleDemo, true), 4);

        attendreLesCommits();
        // Message illisible : un seul rejet au DLT de la source, avec ses octets d'origine, et rien sur la démo.
        assertThat(messages(TOPIC_REJETS_SOURCE, clePoison)).singleElement()
                .satisfies(rejet -> assertThat(rejet.value()).isEqualTo(illisible));
        TOPICS_DEMO.forEach(topic -> assertThat(messages(topic, clePoison)).as(topic).isEmpty());
        // Échec simulé : un seul rejet au DLT de la démo, rien au DLT de la source ni sur la source.
        assertThat(messages(TOPIC_DLT_DEMO, cleDemo)).hasSize(1);
        assertThat(messages(TOPIC_REJETS_SOURCE, cleDemo)).isEmpty();
        assertThat(messages(TOPIC_SOURCE, cleDemo)).isEmpty();
        // Les consommateurs de la démo n'utilisent pas le groupe du service, et inversement.
        assertThat(offsetsCommites(groupe -> groupe.equals(groupeService), TOPICS_DEMO::contains)).isZero();
        assertThat(offsetsCommites(groupe -> groupe.startsWith(PREFIXE_GROUPES_DEMO), TOPIC_SOURCE::equals)).isZero();
    }

    private void envoyer(String topic, String cle, byte[] valeur) throws Exception {
        producteur.send(topic, cle, valeur).get(10, TimeUnit.SECONDS);
    }

    private void envoyerSurLaDemo(String cle, byte[] valeur, int commitsAttendus) throws Exception {
        envoyer(TOPIC_DEMO, cle, valeur);
        COMMITS_DEMO_ATTENDUS.addAndGet(commitsAttendus);
    }

    private static byte[] demo(String idAdherent, boolean simulerEchec) {
        return ("{\"idAdherent\":\"" + idAdherent + "\",\"simulerEchec\":" + simulerEchec + "}")
                .getBytes(StandardCharsets.UTF_8);
    }

    // Les offsets commités des groupes de la démo valent ensemble le nombre de passages attendus
    // (un par topic traversé), et ceux du groupe du service le nombre de messages envoyés sur la source.
    private void attendreLesCommits() {
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200)).untilAsserted(() -> {
            assertThat(offsetsCommites(groupe -> groupe.startsWith(PREFIXE_GROUPES_DEMO), TOPICS_DEMO::contains))
                    .as(() -> "offsets commités par les groupes de la démo : " + offsetsCommitesParGroupe())
                    .isEqualTo(COMMITS_DEMO_ATTENDUS.get());
            assertThat(offsetsCommites(groupe -> groupe.equals(groupeService), TOPIC_SOURCE::equals))
                    .as("offsets commités par le groupe du service sur la source")
                    .isEqualTo(COMMITS_SOURCE_ATTENDUS.get());
        });
    }

    // Lit tout le topic jusqu'aux offsets de fin relevés à l'appel. À appeler après attendreLesCommits() :
    // un message publié par le relais d'une étape est publié avant le commit de l'offset qui l'a produit,
    // donc cette lecture est complète et l'absence d'un message est une preuve, pas l'effet d'une fenêtre trop courte.
    private List<ConsumerRecord<String, byte[]>> messages(String topic, String cle) {
        List<TopicPartition> partitions = lecteur.partitionsFor(topic).stream()
                .map(info -> new TopicPartition(info.topic(), info.partition()))
                .toList();
        lecteur.assign(partitions);
        lecteur.seekToBeginning(partitions);
        Map<TopicPartition, Long> offsetsDeFin = lecteur.endOffsets(partitions);

        List<ConsumerRecord<String, byte[]>> lus = new ArrayList<>();
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            lecteur.poll(Duration.ofMillis(200)).forEach(lus::add);
            return offsetsDeFin.entrySet().stream()
                    .allMatch(fin -> lecteur.position(fin.getKey()) >= fin.getValue());
        });
        return lus.stream()
                .filter(message -> cle.equals(message.key()))
                .sorted(Comparator.comparingLong(ConsumerRecord::offset))
                .toList();
    }

    private ConsumerRecord<String, byte[]> unique(String topic, String cle) {
        List<ConsumerRecord<String, byte[]>> trouves = messages(topic, cle);
        assertThat(trouves).as(topic).hasSize(1);
        return trouves.get(0);
    }

    private static String texte(ConsumerRecord<?, ?> message, String entete) {
        Header header = message.headers().lastHeader(entete);
        assertThat(header).as("en-tête %s", entete).isNotNull();
        return new String(header.value(), StandardCharsets.UTF_8);
    }

    // Nom de chaque en-tête ; texte pour ceux qui décrivent l'exception et le topic d'origine, taille sinon (binaires).
    private static List<String> resumeEntetes(ConsumerRecord<?, ?> message) {
        List<String> resume = new ArrayList<>();
        for (Header header : message.headers()) {
            boolean textuel = header.key().contains("exception-fqcn") || header.key().contains("cause")
                    || header.key().contains("original-topic") || header.key().contains("exception-message");
            resume.add(header.key() + "=" + (textuel
                    ? new String(header.value(), StandardCharsets.UTF_8) : header.value().length + " octets"));
        }
        return resume;
    }

    private Admin admin() {
        return Admin.create(Map.<String, Object>of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString()));
    }

    // Somme des offsets commités par les groupes retenus, sur les topics retenus.
    private long offsetsCommites(Predicate<String> groupes, Predicate<String> topics) throws Exception {
        long somme = 0;
        try (Admin admin = admin()) {
            for (ConsumerGroupListing groupe : admin.listConsumerGroups().all().get(TIMEOUT_ADMIN.toSeconds(), TimeUnit.SECONDS)) {
                if (groupes.test(groupe.groupId())) {
                    Map<TopicPartition, OffsetAndMetadata> offsets = admin.listConsumerGroupOffsets(groupe.groupId())
                            .partitionsToOffsetAndMetadata().get(TIMEOUT_ADMIN.toSeconds(), TimeUnit.SECONDS);
                    somme += offsets.entrySet().stream()
                            .filter(entree -> topics.test(entree.getKey().topic()))
                            .mapToLong(entree -> entree.getValue().offset())
                            .sum();
                }
            }
        }
        return somme;
    }

    // Détail pour les messages d'échec et la preuve : groupe, topic, partition et offset commité.
    private String offsetsCommitesParGroupe() {
        try (Admin admin = admin()) {
            List<String> detail = new ArrayList<>();
            for (ConsumerGroupListing groupe : admin.listConsumerGroups().all().get(TIMEOUT_ADMIN.toSeconds(), TimeUnit.SECONDS)) {
                admin.listConsumerGroupOffsets(groupe.groupId()).partitionsToOffsetAndMetadata()
                        .get(TIMEOUT_ADMIN.toSeconds(), TimeUnit.SECONDS)
                        .forEach((partition, offset) -> {
                            if (offset.offset() > 0) {
                                detail.add(groupe.groupId() + " " + partition + "=" + offset.offset());
                            }
                        });
            }
            return detail.toString();
        } catch (Exception e) {
            return "indisponible : " + e;
        }
    }
}
