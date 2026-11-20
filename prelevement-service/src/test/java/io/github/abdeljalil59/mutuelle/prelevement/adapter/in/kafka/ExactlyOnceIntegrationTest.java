package io.github.abdeljalil59.mutuelle.prelevement.adapter.in.kafka;

import static io.github.abdeljalil59.mutuelle.prelevement.adapter.in.kafka.SupportIntegration.decisionEnBase;
import static io.github.abdeljalil59.mutuelle.prelevement.adapter.in.kafka.SupportIntegration.evenement;
import static io.github.abdeljalil59.mutuelle.prelevement.adapter.in.kafka.SupportIntegration.nombreDeLignes;
import static io.github.abdeljalil59.mutuelle.prelevement.adapter.in.kafka.SupportIntegration.texte;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.github.abdeljalil59.mutuelle.events.ResultatPrelevement;
import io.github.abdeljalil59.mutuelle.events.TypeResultat;
import io.github.abdeljalil59.mutuelle.events.avro.CotisationEmise;
import io.github.abdeljalil59.mutuelle.prelevement.AvroTestUtil;
import io.github.abdeljalil59.mutuelle.prelevement.OracleIntegrationTest;
import io.github.abdeljalil59.mutuelle.prelevement.application.PublicationResultatImpossibleException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AfterRollbackProcessor;
import org.springframework.kafka.listener.DefaultAfterRollbackProcessor;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

// Preuves exactly-once de bout en bout du VRAI code de production (listener, cas d'usage, adaptateurs, MERGE Oracle,
// DefaultAfterRollbackProcessor et DLT), sous transactions Kafka : broker embarqué, registre factice (mock://) et Oracle
// du conteneur partagé. Le code de production n'est pas modifié ; la seule intrusion est un espion sur le template des
// résultats, qui envoie réellement puis fait échouer, pour reproduire D14 : un échec APRÈS l'envoi et AVANT le commit.
//
// Réglages de production conservés : pauses de réessai 1, 2, 4, 8 s (5 livraisons), isolation.level=read_committed.
//
// COMMENT PROUVER UNE ABSENCE. En read_committed, un message qui suit une transaction OUVERTE est retenu jusqu'à la fin
// de cette transaction (doc isolation.level, kafka-clients). Constater qu'un résultat est absent AVANT la fin de la
// transaction ne prouverait donc rien : il pourrait simplement être en attente. On n'attend donc pas un délai : on attend
// que le LSO (last stable offset, endOffsets d'un consommateur read_committed : « le minimum du high watermark et du plus
// petit offset de toute transaction ouverte ») dépasse l'offset du message écrit. À cet instant sa transaction est
// terminée, et s'il est quand même absent en read_committed c'est qu'elle a été annulée : l'absence est une preuve.
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.properties.schema.registry.url=" + AvroTestUtil.REGISTRE})
@EmbeddedKafka(topics = "cotisations.emises", partitions = 3, brokerProperties = {
        "transaction.state.log.replication.factor=1",
        "transaction.state.log.min.isr=1"})
@DirtiesContext
class ExactlyOnceIntegrationTest extends OracleIntegrationTest {

    private static final String TOPIC = "cotisations.emises";
    private static final String TOPIC_RESULTATS = "prelevements.resultats";
    private static final String TOPIC_REJETS = "cotisations.emises-dlt";
    private static final String READ_COMMITTED = "read_committed";
    private static final String READ_UNCOMMITTED = "read_uncommitted";

    // Attente d'un message attendu, une fois l'événement qui le rend visible survenu.
    private static final Duration ATTENTE_PRESENCE = Duration.ofSeconds(10);
    // Attente de l'aboutissement d'un traitement : démarrage à froid du container et du coordinateur de transactions.
    private static final Duration ATTENTE_TRAITEMENT = Duration.ofSeconds(30);
    // Cinq livraisons séparées par 1 + 2 + 4 + 8 = 15 s de pause : marge pour le traitement lui-même.
    private static final Duration ATTENTE_EPUISEMENT_DES_REESSAIS = Duration.ofSeconds(60);

    private DefaultKafkaProducerFactory<String, CotisationEmise> usine;
    private KafkaTemplate<String, CotisationEmise> producteur;
    private CompteurDeLivraisons livraisons;
    private Admin admin;
    private final List<Lecteur<?>> lecteurs = new ArrayList<>();

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Autowired
    private JdbcClient jdbcClient;

    // Le même processeur que celui du conteneur : on y pose un RetryListener pour compter les livraisons.
    @Autowired
    private AfterRollbackProcessor<Object, Object> traitementApresRollback;

    // Espion sur le template des résultats : sans stubbing il appelle le vrai template. Le nom est obligatoire : le
    // template de Boot (kafkaTemplate, celui du topic de rejets) est aussi un KafkaOperations.
    @MockitoSpyBean(name = "resultatsKafkaTemplate")
    private KafkaOperations<Object, Object> resultatsKafkaTemplate;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @BeforeEach
    void prepare_le_producteur_le_compteur_et_l_admin() {
        usine = AvroTestUtil.usineAvro(embeddedKafka);
        producteur = new KafkaTemplate<>(usine);
        livraisons = new CompteurDeLivraisons();
        ((DefaultAfterRollbackProcessor<?, ?>) traitementApresRollback).setRetryListeners(livraisons);
        admin = Admin.create(Map.<String, Object>of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString()));
    }

    @AfterEach
    void libere_les_ressources() {
        ((DefaultAfterRollbackProcessor<?, ?>) traitementApresRollback).setRetryListeners();
        lecteurs.forEach(Lecteur::close);
        admin.close();
        usine.destroy();
    }

    // ------------------------------------------------------------------------------------------------ 1. commit

    // PROUVE : le commit de la transaction rend visibles ENSEMBLE le résultat et l'offset de la source. Un consommateur
    // read_committed reçoit le résultat ACCEPTE, le groupe a dépassé le message (il n'est pas redélivré : un seul appel
    // d'envoi, aucune livraison échouée), et la décision est en base.
    @Test
    void le_commit_rend_visibles_le_resultat_et_l_offset_et_le_message_n_est_pas_redelivre() throws Exception {
        String adherent = "adh-eo-commit";
        UUID idEvenement = UUID.randomUUID();

        RecordMetadata source = envoyer(adherent, evenement(idEvenement, adherent, "75.00"));

        attendreLeCommitDe(source, ATTENTE_TRAITEMENT);
        Lecteur<ResultatPrelevement> valides = lecteurDeResultats(READ_COMMITTED);
        List<ConsumerRecord<String, ResultatPrelevement>> resultats =
                attendre(valides, adherent, 1, ATTENTE_PRESENCE);

        ResultatPrelevement resultat = resultats.get(0).value();
        assertThat(resultat.getType()).isEqualTo(TypeResultat.ACCEPTE);
        assertThat(resultat.getIdEvenement()).isEqualTo(idEvenement);
        assertThat(resultat.getIdAdherent()).isEqualTo(adherent);
        assertThat(offsetCommite(partitionDe(source))).isGreaterThan(source.offset());
        // Pas de redélivrance : un seul traitement, donc un seul envoi, et aucune livraison échouée.
        verify(resultatsKafkaTemplate, times(1)).send(eq(TOPIC_RESULTATS), eq(adherent), any());
        assertThat(livraisons.livraisonsEchouees(adherent)).as(livraisons.toString()).isZero();
        assertThat(nombreDeLignes(jdbcClient, idEvenement)).isEqualTo(1);
        assertThat(decisionEnBase(jdbcClient, idEvenement)).isEqualTo("ACCEPTE");
    }

    // ------------------------------------------------------------------------------------------------- 2. abort

    // PROUVE : un échec APRÈS l'envoi et AVANT le commit annule la transaction. Les octets ont bien été écrits par le
    // broker (vus par un lecteur read_uncommitted), mais le résultat reste invisible en read_committed, l'offset de la
    // source n'est pas validé (le message est redélivré) et la décision Oracle, hors transaction Kafka, est conservée.
    @Test
    void un_echec_apres_l_envoi_annule_la_transaction_le_resultat_reste_invisible_et_l_offset_non_valide()
            throws Exception {
        String adherent = "adh-eo-abort";
        try (AbortGele abort = new AbortGele(adherent)) {
            Lecteur<ResultatPrelevement> nonValides = lecteurDeResultats(READ_UNCOMMITTED);
            Lecteur<ResultatPrelevement> valides = lecteurDeResultats(READ_COMMITTED);

            // Les octets sont physiquement dans le journal : sans cela, « invisible » ne voudrait rien dire.
            ConsumerRecord<String, ResultatPrelevement> ecrit =
                    attendre(nonValides, adherent, 1, ATTENTE_PRESENCE).get(0);
            attendreQueLaTransactionSoitTerminee(valides, ecrit);

            assertThat(valides.enregistrementsDeLaCle(adherent)).as(valides.toString()).isEmpty();
            // Source non validée : le groupe n'a pas dépassé le message (la redélivrance est en attente, voir AbortGele).
            assertThat(offsetCommite(partitionDe(abort.source))).isLessThanOrEqualTo(abort.source.offset());
            // La livraison a échoué une fois, puis le message a été redélivré (seek) : 2e traitement en cours.
            assertThat(livraisons.livraisonsEchouees(adherent)).as(livraisons.toString()).isEqualTo(1);
            // Oracle ne participe pas à la transaction Kafka : le MERGE du 1er traitement survit à l'abort.
            assertThat(nombreDeLignes(jdbcClient, abort.idEvenement)).isEqualTo(1);
        }
    }

    // ----------------------------------------------------------------------------------- 3. read_committed / non

    // PROUVE : c'est bien l'isolation.level qui protège, et non l'absence de message. Même scénario d'abort : un lecteur
    // read_uncommitted VOIT le résultat annulé, un lecteur read_committed ne le voit pas, alors que le message se
    // trouve AVANT le LSO (il n'est donc pas simplement en retard ni en attente d'une transaction ouverte).
    @Test
    void read_uncommitted_voit_le_resultat_annule_et_read_committed_ne_le_voit_pas() throws Exception {
        String adherent = "adh-eo-isolation";
        try (AbortGele abort = new AbortGele(adherent)) {
            Lecteur<ResultatPrelevement> nonValides = lecteurDeResultats(READ_UNCOMMITTED);
            Lecteur<ResultatPrelevement> valides = lecteurDeResultats(READ_COMMITTED);

            ConsumerRecord<String, ResultatPrelevement> annule =
                    attendre(nonValides, adherent, 1, ATTENTE_PRESENCE).get(0);
            attendreQueLaTransactionSoitTerminee(valides, annule);

            // Le message annulé est un vrai résultat, complet : ce n'est pas un message vide qu'on aurait masqué.
            assertThat(annule.value().getType()).isEqualTo(TypeResultat.ACCEPTE);
            assertThat(annule.value().getIdEvenement()).isEqualTo(abort.idEvenement);
            assertThat(valides.enregistrementsDeLaCle(adherent)).as(valides.toString()).isEmpty();
            // Le LSO a dépassé le message : la lecture read_committed est allée au-delà de lui, et ne l'a pas rendu.
            assertThat(valides.fin(annule.partition())).isGreaterThan(annule.offset());
        }
    }

    // ------------------------------------------------------------------------------------------ 4. redélivrance

    // PROUVE : l'exactly-once de bout en bout. Le 1er traitement échoue après l'envoi (résultat écrit puis annulé), le
    // message est redélivré (seek du DefaultAfterRollbackProcessor), le 2e traitement réussit : un lecteur read_committed
    // reçoit UN seul résultat, une seule ligne est en base (MERGE idempotent), rien n'est parti au DLT. Le doublon
    // existe physiquement dans le journal (read_uncommitted en voit deux) mais il est invisible : ni perte, ni doublon.
    @Test
    void un_echec_transitoire_est_redelivre_et_ne_produit_ni_perte_ni_doublon_visible() throws Exception {
        String adherent = "adh-eo-redelivrance";
        UUID idEvenement = UUID.randomUUID();
        AtomicInteger appels = new AtomicInteger();
        doAnswer(invocation -> appels.incrementAndGet() == 1
                ? envoyerPuisEchouer(invocation)
                : invocation.callRealMethod())
                .when(resultatsKafkaTemplate).send(eq(TOPIC_RESULTATS), eq(adherent), any());

        RecordMetadata source = envoyer(adherent, evenement(idEvenement, adherent, "75.00"));

        attendreLeCommitDe(source, ATTENTE_TRAITEMENT);
        Lecteur<ResultatPrelevement> valides = lecteurDeResultats(READ_COMMITTED);
        Lecteur<ResultatPrelevement> nonValides = lecteurDeResultats(READ_UNCOMMITTED);
        List<ConsumerRecord<String, ResultatPrelevement>> resultats =
                attendre(valides, adherent, 1, ATTENTE_PRESENCE);

        assertThat(resultats.get(0).value().getType()).isEqualTo(TypeResultat.ACCEPTE);
        assertThat(resultats.get(0).value().getIdEvenement()).isEqualTo(idEvenement);
        // Redélivré une fois exactement : 1 livraison échouée, 2 envois, aucun rejet.
        verify(resultatsKafkaTemplate, times(2)).send(eq(TOPIC_RESULTATS), eq(adherent), any());
        assertThat(livraisons.livraisonsEchouees(adherent)).as(livraisons.toString()).isEqualTo(1);
        assertThat(livraisons.envoisAuTopicDeRejets(adherent)).as(livraisons.toString()).isZero();
        assertThat(lecteurDeRejets().enregistrementsDeLaCle(adherent)).isEmpty();
        // Une seule ligne : le MERGE du 2e traitement a ignoré le doublon.
        assertThat(nombreDeLignes(jdbcClient, idEvenement)).isEqualTo(1);
        // Le doublon est dans le journal (annulé + validé), caché par l'isolation : c'est elle qui fait l'exactly-once.
        List<ConsumerRecord<String, ResultatPrelevement>> dansLeJournal = nonValides.enregistrementsDeLaCle(adherent);
        assertThat(dansLeJournal).hasSize(2);
        assertThat(dansLeJournal.get(1).offset()).isGreaterThan(dansLeJournal.get(0).offset());
    }

    // ------------------------------------------------------------------------------------------------ 5. DLT

    // PROUVE : le circuit complet réessais -> DLT fonctionne sous transaction, sans boucle infinie. Le traitement échoue à
    // chaque livraison, après l'envoi : 5 livraisons (1 + 4 réessais), chacune écrit un résultat puis l'annule, donc
    // aucun résultat visible en read_committed. Au bout, le message part au DLT (même transaction que le commit de son
    // offset, commitRecovered) : l'offset de la source est dépassé, il n'y a pas de 6e livraison.
    @Test
    void apres_l_epuisement_des_reessais_le_message_part_au_dlt_et_l_offset_est_valide() throws Exception {
        String adherent = "adh-eo-dlt";
        UUID idEvenement = UUID.randomUUID();
        doAnswer(ExactlyOnceIntegrationTest::envoyerPuisEchouer)
                .when(resultatsKafkaTemplate).send(eq(TOPIC_RESULTATS), eq(adherent), any());

        RecordMetadata source = envoyer(adherent, evenement(idEvenement, adherent, "75.00"));

        attendreLeCommitDe(source, ATTENTE_EPUISEMENT_DES_REESSAIS);
        Lecteur<byte[]> rejets = lecteurDeRejets();
        List<ConsumerRecord<String, byte[]>> auDlt = attendre(rejets, adherent, 1, ATTENTE_PRESENCE);
        Lecteur<ResultatPrelevement> nonValides = lecteurDeResultats(READ_UNCOMMITTED);
        Lecteur<ResultatPrelevement> valides = lecteurDeResultats(READ_COMMITTED);
        List<ConsumerRecord<String, ResultatPrelevement>> annules = attendre(nonValides, adherent, 5, ATTENTE_PRESENCE);
        attendreQueLaTransactionSoitTerminee(valides, annules.get(annules.size() - 1));

        // 5 livraisons échouées, un seul envoi au DLT, pas de 6e livraison : commitRecovered a validé l'offset.
        assertThat(livraisons.livraisonsEchouees(adherent)).as(livraisons.toString()).isEqualTo(5);
        assertThat(livraisons.envoisAuTopicDeRejets(adherent)).as(livraisons.toString()).isEqualTo(1);
        verify(resultatsKafkaTemplate, times(5)).send(eq(TOPIC_RESULTATS), eq(adherent), any());
        assertThat(offsetCommite(partitionDe(source))).isGreaterThan(source.offset());
        // Cinq résultats ont été écrits puis annulés : le journal en contient 5, read_committed n'en rend aucun.
        assertThat(valides.enregistrementsDeLaCle(adherent)).as(valides.toString()).isEmpty();
        // Le DLT porte le message d'origine et sa provenance exacte.
        ConsumerRecord<String, byte[]> rejet = auDlt.get(0);
        assertThat(AvroTestUtil.decoder(TOPIC_REJETS, rejet.value()).getIdEvenement()).isEqualTo(idEvenement);
        assertThat(texte(rejet, "kafka_dlt-original-topic")).isEqualTo(TOPIC);
        assertThat(ByteBuffer.wrap(valeur(rejet, "kafka_dlt-original-partition")).getInt()).isEqualTo(source.partition());
        assertThat(ByteBuffer.wrap(valeur(rejet, "kafka_dlt-original-offset")).getLong()).isEqualTo(source.offset());
        assertThat(texte(rejet, "kafka_dlt-exception-cause-fqcn"))
                .isEqualTo(PublicationResultatImpossibleException.class.getName());
        // Pas d'en-tête du nombre de tentatives : kafka_deliveryAttempt n'est posé que si ContainerProperties.
        // deliveryAttemptHeader est activé (faux par défaut, aucune propriété Boot, rien dans le service). Le nombre
        // de tentatives se prouve donc par le RetryListener (5) et par les 5 envois, pas par un en-tête.
        assertThat(rejet.headers().toArray()).extracting(Header::key).doesNotContain("kafka_deliveryAttempt");
        // Écrit avant la publication, donc conservé malgré les 5 annulations.
        assertThat(nombreDeLignes(jdbcClient, idEvenement)).isEqualTo(1);
    }

    // ---------------------------------------------------------------------------------------- injection d'échec

    // D14 reproductible : le VRAI template envoie (le broker a écrit le message dans la transaction ouverte, on attend
    // son accusé), puis l'exception est levée avant que le container ne valide la transaction.
    private static Object envoyerPuisEchouer(InvocationOnMock invocation) throws Throwable {
        CompletableFuture<?> envoi = (CompletableFuture<?>) invocation.callRealMethod();
        envoi.get(10, TimeUnit.SECONDS);
        throw new IllegalStateException("échec simulé après l'envoi, avant le commit");
    }

    // Scénario d'abort FIGÉ, pour observer l'état entre l'abort et la redélivrance. Le 1er traitement envoie puis échoue
    // (transaction annulée). La redélivrance, qui suit après la pause du BackOff, est BLOQUÉE dans l'espion avant son
    // envoi par un verrou : l'état observé (résultat annulé, offset non validé) ne dépend donc d'aucun délai. Au close,
    // le verrou est levé et la redélivrance aboutit, ce qui laisse le service au repos pour le test suivant.
    private final class AbortGele implements AutoCloseable {

        private final UUID idEvenement = UUID.randomUUID();
        private final CountDownLatch redelivrance = new CountDownLatch(1);
        private final CountDownLatch liberation = new CountDownLatch(1);
        private final RecordMetadata source;

        AbortGele(String adherent) throws Exception {
            AtomicInteger appels = new AtomicInteger();
            doAnswer(invocation -> {
                if (appels.incrementAndGet() == 1) {
                    return envoyerPuisEchouer(invocation);
                }
                redelivrance.countDown();
                liberation.await(ATTENTE_TRAITEMENT.toSeconds(), TimeUnit.SECONDS);
                return invocation.callRealMethod();
            }).when(resultatsKafkaTemplate).send(eq(TOPIC_RESULTATS), eq(adherent), any());

            source = envoyer(adherent, evenement(idEvenement, adherent, "75.00"));

            // La 2e livraison a démarré : le 1er traitement a donc échoué, sa transaction est annulée et le message est
            // redélivré. C'est le signal d'attente, à la place d'un délai.
            assertThat(redelivrance.await(ATTENTE_TRAITEMENT.toSeconds(), TimeUnit.SECONDS))
                    .as("redélivrance du message %s après l'abort", adherent).isTrue();
        }

        @Override
        public void close() throws Exception {
            liberation.countDown();
            attendreLeCommitDe(source, ATTENTE_TRAITEMENT);
        }
    }

    // --------------------------------------------------------------------------------------------- Kafka, Oracle

    private RecordMetadata envoyer(String cle, CotisationEmise evenement) throws Exception {
        return producteur.send(TOPIC, cle, evenement).get(10, TimeUnit.SECONDS).getRecordMetadata();
    }

    private static TopicPartition partitionDe(RecordMetadata metadata) {
        return new TopicPartition(metadata.topic(), metadata.partition());
    }

    private static byte[] valeur(ConsumerRecord<?, ?> message, String entete) {
        Header header = message.headers().lastHeader(entete);
        assertThat(header).as("en-tête %s", entete).isNotNull();
        return header.value();
    }

    // Offset validé du groupe sur la partition (prochain offset à lire), 0 s'il n'a jamais rien validé.
    private long offsetCommite(TopicPartition partition) throws Exception {
        OffsetAndMetadata offset = admin.listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS).get(partition);
        return offset == null ? 0 : offset.offset();
    }

    // Le groupe a dépassé le message : son offset est validé, par le commit de la transaction ou par commitRecovered.
    private void attendreLeCommitDe(RecordMetadata source, Duration delai) {
        await("offset de la source " + partitionDe(source) + "@" + source.offset())
                .atMost(delai).pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> assertThat(offsetCommite(partitionDe(source))).isGreaterThan(source.offset()));
    }

    // Le LSO a dépassé le message : la transaction qui l'a écrit est terminée (validée ou annulée). Voir l'en-tête de classe.
    private void attendreQueLaTransactionSoitTerminee(Lecteur<?> readCommitted, ConsumerRecord<?, ?> ecrit) {
        await("fin de la transaction du message " + ecrit.partition() + "@" + ecrit.offset())
                .atMost(ATTENTE_PRESENCE).pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> assertThat(readCommitted.fin(ecrit.partition())).isGreaterThan(ecrit.offset()));
    }

    // Attend que le lecteur voie exactement `attendu` messages de cette clé, et les rend.
    private <V> List<ConsumerRecord<String, V>> attendre(Lecteur<V> lecteur, String cle, int attendu, Duration delai) {
        await("messages de " + cle + " vus par " + lecteur)
                .atMost(delai).pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> assertThat(lecteur.enregistrementsDeLaCle(cle)).hasSize(attendu));
        return lecteur.enregistrementsDeLaCle(cle);
    }

    private Lecteur<ResultatPrelevement> lecteurDeResultats(String isolation) {
        Map<String, Object> proprietes = proprietesLecteur(isolation, KafkaAvroDeserializer.class);
        proprietes.put("schema.registry.url", AvroTestUtil.REGISTRE);
        proprietes.put("specific.avro.reader", true);
        return ouvrir(new Lecteur<>(proprietes, TOPIC_RESULTATS, isolation));
    }

    // Le DLT se lit en read_committed : son envoi est dans la même transaction que le commit de l'offset de la source.
    private Lecteur<byte[]> lecteurDeRejets() {
        return ouvrir(new Lecteur<>(proprietesLecteur(READ_COMMITTED, ByteArrayDeserializer.class),
                TOPIC_REJETS, READ_COMMITTED));
    }

    private <V> Lecteur<V> ouvrir(Lecteur<V> lecteur) {
        lecteurs.add(lecteur);
        return lecteur;
    }

    // Lecteur de test : sans commit, groupe à usage unique, isolation.level au choix.
    private Map<String, Object> proprietesLecteur(String isolation, Class<?> deserialiseurDeValeur) {
        Map<String, Object> proprietes = new HashMap<>();
        proprietes.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
        proprietes.put(ConsumerConfig.GROUP_ID_CONFIG, "lecteur-eo-" + UUID.randomUUID());
        proprietes.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        proprietes.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolation);
        proprietes.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        proprietes.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserialiseurDeValeur);
        return proprietes;
    }

    // KafkaConsumer brut (pas un @KafkaListener) pour maîtriser l'isolation.level. Toutes les partitions du topic sont
    // assignées ; chaque lecture repart du début et s'arrête à la fin DE L'ISOLATION : le LSO en read_committed, le
    // high watermark en read_uncommitted (endOffsets). La lecture est donc complète, sans délai à deviner.
    private final class Lecteur<V> implements AutoCloseable {

        private final KafkaConsumer<String, V> consommateur;
        private final List<TopicPartition> partitions;
        private final String description;

        Lecteur(Map<String, Object> proprietes, String topic, String isolation) {
            this.consommateur = new KafkaConsumer<>(proprietes);
            this.partitions = consommateur.partitionsFor(topic).stream()
                    .map(info -> new TopicPartition(info.topic(), info.partition()))
                    .toList();
            this.consommateur.assign(partitions);
            this.description = "lecteur " + isolation + " de " + topic;
        }

        long fin(int partition) {
            TopicPartition cible = partitions.stream().filter(p -> p.partition() == partition).findFirst().orElseThrow();
            return consommateur.endOffsets(List.of(cible)).get(cible);
        }

        List<ConsumerRecord<String, V>> enregistrementsDeLaCle(String cle) {
            consommateur.seekToBeginning(partitions);
            Map<TopicPartition, Long> fins = consommateur.endOffsets(partitions);
            List<ConsumerRecord<String, V>> lus = new ArrayList<>();
            await().atMost(ATTENTE_PRESENCE).until(() -> {
                consommateur.poll(Duration.ofMillis(200)).forEach(lus::add);
                return fins.entrySet().stream()
                        .allMatch(fin -> consommateur.position(fin.getKey()) >= fin.getValue());
            });
            return lus.stream().filter(message -> cle.equals(message.key())).toList();
        }

        @Override
        public void close() {
            consommateur.close();
        }

        @Override
        public String toString() {
            return description;
        }
    }
}
