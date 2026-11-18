package io.github.abdeljalil59.mutuelle.prelevement.adapter.in.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
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
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

// Seul le topic source est déclaré ici : le topic de rejets (cotisations.emises-dlt) doit être créé par le service.
// Le test ne doit jamais atteindre le broker du compose (localhost:9092) : voir le garde-fou.
// Aucune lecture de logs : le DLT et les offsets commités du groupe suffisent.
@SpringBootTest(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@EmbeddedKafka(topics = "cotisations.emises", partitions = 3)
@DirtiesContext
class MessageInvalideIntegrationTest {

    private static final String TOPIC = "cotisations.emises";
    private static final String TOPIC_REJETS = "cotisations.emises-dlt";
    // Délai des appels à l'Admin Kafka : 5 s ne suffisent pas quand la machine est chargée (TimeoutException constatée).
    private static final Duration TIMEOUT_ADMIN = Duration.ofSeconds(30);
    // Messages envoyés par toute la classe : référence du test de commit, quel que soit l'ordre des tests.
    private static final AtomicInteger MESSAGES_ENVOYES = new AtomicInteger();

    private DefaultKafkaProducerFactory<String, byte[]> usine;
    private KafkaTemplate<String, byte[]> producteur;
    private KafkaConsumer<String, byte[]> consommateurRejets;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @BeforeEach
    void prepare_le_producteur_et_le_consommateur_des_rejets() {
        // Garde-fou : le contexte doit pointer vers le broker embarqué, jamais vers localhost:9092.
        assertThat(bootstrapServers).isEqualTo(embeddedKafka.getBrokersAsString());

        // Producteur de test : octets bruts, donc aucun en-tête de type JSON.
        usine = new DefaultKafkaProducerFactory<>(
                KafkaTestUtils.producerProps(embeddedKafka), new StringSerializer(), new ByteArraySerializer());
        producteur = new KafkaTemplate<>(usine);

        // Lecteur du DLT : sans commit ; ses partitions sont assignées à la demande (voir lireLesRejetsDeLaCle).
        consommateurRejets = new KafkaConsumer<>(Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString(),
                ConsumerConfig.GROUP_ID_CONFIG, "lecteur-rejets-" + UUID.randomUUID(),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false),
                new StringDeserializer(), new ByteArrayDeserializer());
    }

    @AfterEach
    void libere_le_producteur_et_le_consommateur() {
        consommateurRejets.close();
        usine.destroy();
    }

    @Test
    void un_message_illisible_va_au_dlt_avec_ses_octets_d_origine_sans_bloquer_la_partition() throws Exception {
        String cle = "adh-illisible";
        byte[] illisible = "ceci-n-est-pas-du-json".getBytes(StandardCharsets.UTF_8);

        SendResult<String, byte[]> envoiIllisible = envoyer(cle, illisible);
        envoyer(cle, valide(cle));

        attendreQueLeGroupeAitToutCommite();
        List<ConsumerRecord<String, byte[]>> rejets = lireLesRejetsDeLaCle(cle);

        // Exactement un rejet pour cette clé : le message valide (même clé) n'y est pas.
        assertThat(rejets).hasSize(1);
        ConsumerRecord<String, byte[]> rejet = rejets.get(0);
        assertThat(rejet.value()).isEqualTo(illisible);
        assertThat(rejet.partition()).isEqualTo(envoiIllisible.getRecordMetadata().partition());
        assertThat(texte(rejet, "kafka_dlt-original-topic")).isEqualTo(TOPIC);
        assertThat(ByteBuffer.wrap(valeur(rejet, "kafka_dlt-original-partition")).getInt())
                .isEqualTo(envoiIllisible.getRecordMetadata().partition());
        assertThat(ByteBuffer.wrap(valeur(rejet, "kafka_dlt-original-offset")).getLong())
                .isEqualTo(envoiIllisible.getRecordMetadata().offset());
        assertThat(texte(rejet, "kafka_dlt-exception-fqcn"))
                .isEqualTo("org.springframework.kafka.support.serializer.DeserializationException");
        assertThat(rejet.headers().lastHeader("springDeserializerExceptionValue")).isNull();
    }

    // Prouve (ou infirme) que PrelevementInvalideException est inatteignable via Kafka : le contrat
    // CotisationEmise rejette déjà un montant négatif, donc l'échec survient à la désérialisation.
    @Test
    void un_montant_negatif_est_rejete_a_la_deserialisation_et_va_au_dlt() throws Exception {
        String cle = "adh-negatif";
        byte[] montantNegatif = json(UUID.randomUUID(), cle, "-5").getBytes(StandardCharsets.UTF_8);

        envoyer(cle, montantNegatif);
        envoyer(cle, valide(cle));

        attendreQueLeGroupeAitToutCommite();
        List<ConsumerRecord<String, byte[]>> rejets = lireLesRejetsDeLaCle(cle);

        assertThat(rejets).hasSize(1);
        ConsumerRecord<String, byte[]> rejet = rejets.get(0);
        assertThat(rejet.value()).isEqualTo(montantNegatif);
        assertThat(texte(rejet, "kafka_dlt-exception-fqcn"))
                .isEqualTo("org.springframework.kafka.support.serializer.DeserializationException");
    }

    private SendResult<String, byte[]> envoyer(String cle, byte[] valeur) throws Exception {
        SendResult<String, byte[]> resultat = producteur.send(TOPIC, cle, valeur).get(10, TimeUnit.SECONDS);
        MESSAGES_ENVOYES.incrementAndGet();
        return resultat;
    }

    private static byte[] valide(String idAdherent) {
        return json(UUID.randomUUID(), idAdherent, "50.00").getBytes(StandardCharsets.UTF_8);
    }

    private static String json(UUID idEvenement, String idAdherent, String montant) {
        return "{\"idEvenement\":\"" + idEvenement + "\",\"idAdherent\":\"" + idAdherent
                + "\",\"montant\":" + montant + ",\"emiseLe\":\"2026-11-17T08:00:00Z\"}";
    }

    // Chaque message envoyé (traité ou rejeté) doit être dépassé par le groupe : les offsets commités
    // valent ensemble le nombre de messages envoyés par la classe.
    private void attendreQueLeGroupeAitToutCommite() {
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> assertThat(sommeDesOffsetsCommites()).isEqualTo(MESSAGES_ENVOYES.get()));
    }

    // Les deux tests partagent le DLT : filtre par clé, propre à chaque test.
    // À appeler après attendreQueLeGroupeAitToutCommite() : le rejet est publié avant le commit de son offset,
    // donc les offsets de fin relevés ici (après le commit) incluent tout rejet déjà publié pour les messages
    // envoyés. Lire jusqu'à ces offsets rend la lecture complète : l'absence d'un message est alors une preuve,
    // pas l'effet d'une fenêtre de temps trop courte.
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

    private static byte[] valeur(ConsumerRecord<?, ?> rejet, String entete) {
        Header header = rejet.headers().lastHeader(entete);
        assertThat(header).as("en-tête %s", entete).isNotNull();
        return header.value();
    }

    private static String texte(ConsumerRecord<?, ?> rejet, String entete) {
        return new String(valeur(rejet, entete), StandardCharsets.UTF_8);
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
