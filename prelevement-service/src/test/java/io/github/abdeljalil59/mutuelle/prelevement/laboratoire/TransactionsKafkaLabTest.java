package io.github.abdeljalil59.mutuelle.prelevement.laboratoire;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.listener.BatchMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultAfterRollbackProcessor;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.util.backoff.FixedBackOff;

// Laboratoire des transactions Kafka (phase transactions, étape 0) : aucun code de production, des FAITS.
// Les tests 1 à 5 utilisent les clients Kafka natifs (pas de Spring, pas d'Avro, chaînes de caractères) ; les tests 6 et 7
// utilisent le container de spring-kafka (sans contexte Spring Boot) pour trancher le comportement du
// DefaultAfterRollbackProcessor en mode batch et en mode record.
//
// Indépendance des tests : chaque test crée SES topics (lab.input-<id>, lab.output-<id>, 1 partition) sur le broker
// embarqué partagé par la classe, avec ses propres groupes de consommateurs et son transactional.id. Un topic commun
// aurait mélangé les messages des tests entre eux (le lot de 5 messages du test 6 aurait été relu par le test 7).
//
// Le broker embarqué n'est pas configuré pour les transactions par défaut (R5) : le facteur de réplication et le
// min.isr de __transaction_state valent 1 ici, comme dans infra/docker-compose.yml.
@EmbeddedKafka(partitions = 1, brokerProperties = {
        "transaction.state.log.replication.factor=1",
        "transaction.state.log.min.isr=1"})
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class TransactionsKafkaLabTest {

    private static final Duration SONDAGE = Duration.ofSeconds(5);

    private static EmbeddedKafkaBroker broker() {
        return EmbeddedKafkaCondition.getBroker();
    }

    // ---------------------------------------------------------------- 1 à 3 : commit, abort, fencing

    @Test
    void transaction_commitee_visible_en_read_committed() throws Exception {
        String sortie = creerTopic("lab.output");

        try (KafkaProducer<String, String> producteur = producteur("lab-tx-1")) {
            producteur.beginTransaction();
            producteur.send(new ProducerRecord<>(sortie, "cle", "message-commite")).get();
            producteur.commitTransaction();
        }

        try (KafkaConsumer<String, String> committed = consommateur("lab-g1-committed", "read_committed");
             KafkaConsumer<String, String> uncommitted = consommateur("lab-g1-uncommitted", "read_uncommitted")) {
            assertThat(valeurs(lire(committed, sortie, 1, Duration.ofSeconds(10)))).containsExactly("message-commite");
            assertThat(valeurs(lire(uncommitted, sortie, 1, Duration.ofSeconds(10)))).containsExactly("message-commite");
        }
    }

    @Test
    void transaction_avortee_invisible_en_read_committed() throws Exception {
        String sortie = creerTopic("lab.output");

        try (KafkaProducer<String, String> producteur = producteur("lab-tx-2")) {
            producteur.beginTransaction();
            producteur.send(new ProducerRecord<>(sortie, "cle", "message-avorte")).get();
            producteur.abortTransaction();
        }

        try (KafkaConsumer<String, String> committed = consommateur("lab-g2-committed", "read_committed");
             KafkaConsumer<String, String> uncommitted = consommateur("lab-g2-uncommitted", "read_uncommitted")) {
            // L'absence se prouve en sondant pendant toute la durée du délai, pas en s'arrêtant au premier poll vide.
            assertThat(valeurs(lire(committed, sortie, Integer.MAX_VALUE, SONDAGE))).isEmpty();
            // Le message avorté est bien dans le journal du topic : seul read_uncommitted le rend.
            assertThat(valeurs(lire(uncommitted, sortie, 1, Duration.ofSeconds(10)))).containsExactly("message-avorte");
        }
    }

    @Test
    void fencing_zombie_par_transactional_id() throws Exception {
        String sortie = creerTopic("lab.output");
        KafkaProducer<String, String> zombie = producteur("lab-tx-zombie");
        try {
            zombie.beginTransaction();
            zombie.send(new ProducerRecord<>(sortie, "cle", "message-du-zombie")).get();

            // Le second producteur, de même transactional.id, fence le premier (incrémente l'époque) et fait avorter sa
            // transaction en cours.
            try (KafkaProducer<String, String> remplacant = producteur("lab-tx-zombie")) {
                assertThatThrownBy(zombie::commitTransaction).isInstanceOf(ProducerFencedException.class);

                remplacant.beginTransaction();
                remplacant.send(new ProducerRecord<>(sortie, "cle", "message-du-remplacant")).get();
                remplacant.commitTransaction();
            }
        } finally {
            fermerSansErreur(zombie);
        }

        try (KafkaConsumer<String, String> committed = consommateur("lab-g3-committed", "read_committed");
             KafkaConsumer<String, String> uncommitted = consommateur("lab-g3-uncommitted", "read_uncommitted")) {
            assertThat(valeurs(lire(committed, sortie, 1, Duration.ofSeconds(10)))).containsExactly("message-du-remplacant");
            // Le message du zombie a été écrit puis avorté : read_uncommitted le voit encore, read_committed jamais.
            assertThat(valeurs(lire(uncommitted, sortie, 2, Duration.ofSeconds(10))))
                    .containsExactlyInAnyOrder("message-du-zombie", "message-du-remplacant");
        }
    }

    // ---------------------------------------------------------------- 4 et 5 : read -> process -> write

    @Test
    void read_process_write_atomique() throws Exception {
        String entree = creerTopic("lab.input");
        String sortie = creerTopic("lab.output");
        String groupe = "lab-g4";
        envoyerSansTransaction(entree, "bonjour");

        try (KafkaConsumer<String, String> lecteur = consommateur(groupe, "read_committed");
             KafkaProducer<String, String> producteur = producteur("lab-tx-4")) {
            ConsumerRecord<String, String> lu = lire(lecteur, entree, 1, Duration.ofSeconds(10)).get(0);

            producteur.beginTransaction();
            producteur.send(new ProducerRecord<>(sortie, lu.key(), lu.value().toUpperCase())).get();
            producteur.sendOffsetsToTransaction(
                    Map.of(new TopicPartition(entree, lu.partition()), new OffsetAndMetadata(lu.offset() + 1)),
                    lecteur.groupMetadata());
            producteur.commitTransaction();
        }

        try (KafkaConsumer<String, String> committed = consommateur("lab-g4-sortie", "read_committed")) {
            assertThat(valeurs(lire(committed, sortie, 1, Duration.ofSeconds(10)))).containsExactly("BONJOUR");
        }
        // L'offset du groupe a avancé dans la même transaction : le message d'entrée (offset 0) est consommé.
        assertThat(offsetsDuGroupe(groupe)).containsEntry(new TopicPartition(entree, 0), new OffsetAndMetadata(1));
    }

    @Test
    void rollback_read_process_write() throws Exception {
        String entree = creerTopic("lab.input");
        String sortie = creerTopic("lab.output");
        String groupe = "lab-g5";
        envoyerSansTransaction(entree, "bonjour");

        try (KafkaConsumer<String, String> lecteur = consommateur(groupe, "read_committed");
             KafkaProducer<String, String> producteur = producteur("lab-tx-5")) {
            ConsumerRecord<String, String> lu = lire(lecteur, entree, 1, Duration.ofSeconds(10)).get(0);

            producteur.beginTransaction();
            producteur.send(new ProducerRecord<>(sortie, lu.key(), lu.value().toUpperCase())).get();
            producteur.sendOffsetsToTransaction(
                    Map.of(new TopicPartition(entree, lu.partition()), new OffsetAndMetadata(lu.offset() + 1)),
                    lecteur.groupMetadata());
            producteur.abortTransaction();
        }

        try (KafkaConsumer<String, String> committed = consommateur("lab-g5-sortie", "read_committed")) {
            assertThat(valeurs(lire(committed, sortie, Integer.MAX_VALUE, SONDAGE))).isEmpty();
        }
        // L'offset n'a pas avancé : le groupe n'a aucun offset validé sur l'entrée...
        assertThat(offsetsDuGroupe(groupe)).doesNotContainKey(new TopicPartition(entree, 0));
        // ... et le message est redisponible pour un nouveau consommateur du même groupe.
        try (KafkaConsumer<String, String> relecteur = consommateur(groupe, "read_committed")) {
            assertThat(valeurs(lire(relecteur, entree, 1, Duration.ofSeconds(10)))).containsExactly("bonjour");
        }
    }

    // ---------------------------------------------------------------- 6 et 7 : container transactionnel

    // Preuve de R1 : listener BATCH sous transaction, échec (RuntimeException) sur le 3e message. C'est le test qui tranche D2.
    //
    // CONSTATÉ le 20/11/2026 (spring-kafka 4.1.2, kafka-clients 4.2.2, broker embarqué 4.2.2), avec un
    // DefaultAfterRollbackProcessor(FixedBackOff(0, 1), recoverer collecteur, commitRecovered) :
    //  - le recoverer n'est JAMAIS appelé : ni m3 seul, ni le lot (aucune récupération en 10 s) ;
    //  - le lot ENTIER [m1..m5] est redélivré en boucle (22 livraisons en environ 13 s), alors que le BackOff n'en
    //    autorise que 2 ;
    //  - m4 et m5 ne sont jamais livrés à part, et aucun offset n'est validé.
    // Essai de contrôle jetable (non conservé) avec commitRecovered=false : même résultat, 65 livraisons en 35 s, aucune
    // récupération. Le bytecode de DefaultAfterRollbackProcessor 4.1.2 ne contient d'ailleurs aucune référence à
    // BatchListenerFailedException (voir le test suivant).
    // DÉDUIT : un message empoisonné bloque la partition indéfiniment. Ce n'est pas « tout le lot part au DLT » (hypothèse
    // du rapport d'analyse, contredite ici) : il n'y a pas de DLT du tout.
    @Test
    void batch_listener_sous_transaction_perd_isolation() throws Exception {
        Constat constat = jouerScenario(true, false, "6");

        assertThat(constat.recuperationObservee()).as(resume(constat)).isFalse();
        assertThat(constat.recuperes()).as(resume(constat)).isEmpty();
        // Plus de livraisons que les 2 permises par FixedBackOff(0, 1), et toujours le lot entier.
        assertThat(constat.livraisons()).as(resume(constat)).hasSizeGreaterThan(2);
        assertThat(constat.livraisons().stream().distinct().toList()).containsExactly("lot [m1, m2, m3, m4, m5]");
        assertThat(constat.offsets()).as(resume(constat)).isEmpty();
    }

    // Complément au test 6 : même scénario, mais le listener lève BatchListenerFailedException(index), comme le listener
    // de production du prelevement-service.
    //
    // CONSTATÉ le 20/11/2026 (mêmes versions) : identique au test précédent (26 livraisons du lot entier en environ 13 s,
    // aucune récupération, aucun offset validé). L'index porté par BatchListenerFailedException(2) n'est pas exploité
    // sous transaction : l'isolation du message fautif, que le DefaultErrorHandler assure hors transaction, est perdue.
    // NON VÉRIFIÉ : la doc de spring-kafka 4.2 annonce que le DefaultAfterRollbackProcessor récupère aussi les
    // enregistrements d'un listener batch ; ce comportement n'est pas éprouvé ici (le projet est en 4.1.2).
    @Test
    void batch_listener_sous_transaction_avec_BatchListenerFailedException() throws Exception {
        Constat constat = jouerScenario(true, true, "6b");

        assertThat(constat.recuperationObservee()).as(resume(constat)).isFalse();
        assertThat(constat.recuperes()).as(resume(constat)).isEmpty();
        assertThat(constat.livraisons()).as(resume(constat)).hasSizeGreaterThan(2);
        assertThat(constat.livraisons().stream().distinct().toList()).containsExactly("lot [m1, m2, m3, m4, m5]");
        assertThat(constat.offsets()).as(resume(constat)).isEmpty();
    }

    // Preuve que B fonctionne : listener par RECORD sous transaction, échec sur le 3e message.
    //
    // CONSTATÉ le 20/11/2026 (mêmes versions et même DefaultAfterRollbackProcessor) : m1 et m2 sont traités, m3 est livré
    // 2 fois (1 livraison + 1 réessai du FixedBackOff(0, 1)) puis envoyé au recoverer, et SEUL m3 l'atteint ; m4 et m5 sont
    // ensuite traités normalement. L'offset validé du groupe est 5 : l'offset du message récupéré est validé dans la
    // transaction du processeur (commitRecovered), comme le dit la doc. Le résultat est identique sur deux exécutions.
    @Test
    void record_listener_sous_transaction_isole() throws Exception {
        Constat constat = jouerScenario(false, false, "7");

        assertThat(constat.livraisons()).as(resume(constat)).containsExactly("m1", "m2", "m3", "m3", "m4", "m5");
        assertThat(constat.recuperes()).as(resume(constat)).containsExactly("m3");
        assertThat(constat.recuperationObservee()).isTrue();
        assertThat(constat.offsets().values()).as(resume(constat)).extracting(OffsetAndMetadata::offset).containsExactly(5L);
    }

    // Résultat observé d'un scénario : ce que le listener a reçu à chaque livraison, ce que le recoverer a collecté,
    // si la récupération a eu lieu et les offsets validés du groupe à la fin.
    private record Constat(boolean parLot, List<String> livraisons, List<String> recuperes, boolean recuperationObservee,
                           Map<TopicPartition, OffsetAndMetadata> offsets) {
    }

    // 5 messages m1..m5 dans l'entrée (même clé, une partition), un container transactionnel (KafkaTransactionManager),
    // un DefaultAfterRollbackProcessor (FixedBackOff(0, 1) : 2 livraisons, recoverer qui collecte, commitRecovered) et un
    // listener qui lève une exception sur m3.
    private Constat jouerScenario(boolean parLot, boolean parBatchListenerFailedException, String numero)
            throws Exception {
        String entree = creerTopic("lab.input");
        String groupe = "lab-g" + numero;
        try (KafkaProducer<String, String> amorce = producteur(null)) {
            for (int i = 1; i <= 5; i++) {
                amorce.send(new ProducerRecord<>(entree, "cle", "m" + i)).get();
            }
        }

        List<String> livraisons = new CopyOnWriteArrayList<>();
        List<String> recuperes = new CopyOnWriteArrayList<>();

        DefaultKafkaProducerFactory<String, String> usine = new DefaultKafkaProducerFactory<>(proprietesProducteur(null));
        usine.setTransactionIdPrefix("lab-tx-" + numero + "-");
        DefaultKafkaConsumerFactory<String, String> usineConsommateur =
                new DefaultKafkaConsumerFactory<>(proprietesConsommateur(groupe, "read_committed"));

        DefaultAfterRollbackProcessor<String, String> traitement = new DefaultAfterRollbackProcessor<>(
                (message, exception) -> recuperes.add(String.valueOf(message.value())),
                new FixedBackOff(0L, 1L), new KafkaTemplate<>(usine), true);

        ContainerProperties proprietes = new ContainerProperties(entree);
        proprietes.setGroupId(groupe);
        proprietes.setKafkaAwareTransactionManager(new KafkaTransactionManager<>(usine));
        if (parLot) {
            BatchMessageListener<String, String> ecouteur = lot -> {
                livraisons.add("lot " + lot.stream().map(ConsumerRecord::value).toList());
                for (int index = 0; index < lot.size(); index++) {
                    if ("m3".equals(lot.get(index).value())) {
                        if (parBatchListenerFailedException) {
                            throw new BatchListenerFailedException("échec volontaire sur m3", index);
                        }
                        throw new IllegalStateException("échec volontaire sur m3");
                    }
                }
            };
            proprietes.setMessageListener(ecouteur);
        } else {
            MessageListener<String, String> ecouteur = message -> {
                livraisons.add(message.value());
                echouerSurM3(message);
            };
            proprietes.setMessageListener(ecouteur);
        }

        ConcurrentMessageListenerContainer<String, String> conteneur =
                new ConcurrentMessageListenerContainer<>(usineConsommateur, proprietes);
        conteneur.setBeanName("lab-" + numero);
        conteneur.setAfterRollbackProcessor(traitement);

        boolean recuperationObservee = true;
        try {
            conteneur.start();
            try {
                await().atMost(Duration.ofSeconds(10)).until(() -> !recuperes.isEmpty());
            } catch (ConditionTimeoutException aucuneRecuperation) {
                recuperationObservee = false;
            }
            // Laisse au container le temps de traiter (ou non) les messages qui suivent la récupération.
            Thread.sleep(3_000);
        } finally {
            conteneur.stop();
            usine.destroy();
        }
        return new Constat(parLot, List.copyOf(livraisons), List.copyOf(recuperes), recuperationObservee,
                offsetsDuGroupe(groupe));
    }

    private static String resume(Constat constat) {
        return "livraisons=" + constat.livraisons().size() + " distinctes=" + constat.livraisons().stream().distinct().toList()
                + " premieres=" + constat.livraisons().stream().limit(8).toList()
                + " recuperes=" + constat.recuperes() + " recuperationObservee=" + constat.recuperationObservee()
                + " offsets=" + constat.offsets();
    }

    private static void echouerSurM3(ConsumerRecord<String, String> message) {
        if ("m3".equals(message.value())) {
            throw new IllegalStateException("échec volontaire sur m3");
        }
    }

    // ---------------------------------------------------------------- outils

    private static String creerTopic(String base) {
        String nom = base + "-" + UUID.randomUUID().toString().substring(0, 8);
        broker().addTopics(nom);
        return nom;
    }

    private static Map<String, Object> proprietesProducteur(String transactionalId) {
        Map<String, Object> proprietes = new HashMap<>();
        proprietes.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker().getBrokersAsString());
        proprietes.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        proprietes.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // Un broker sans transactions ferait attendre initTransactions() jusqu'à ce délai (60 s par défaut) : on le réduit.
        proprietes.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 30_000);
        if (transactionalId != null) {
            proprietes.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        }
        return proprietes;
    }

    // Producteur natif ; transactionnel (initTransactions déjà appelé) si un transactional.id est fourni.
    private static KafkaProducer<String, String> producteur(String transactionalId) {
        KafkaProducer<String, String> producteur = new KafkaProducer<>(proprietesProducteur(transactionalId));
        if (transactionalId != null) {
            producteur.initTransactions();
        }
        return producteur;
    }

    private static Map<String, Object> proprietesConsommateur(String groupe, String isolation) {
        Map<String, Object> proprietes = new HashMap<>();
        proprietes.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker().getBrokersAsString());
        proprietes.put(ConsumerConfig.GROUP_ID_CONFIG, groupe);
        proprietes.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        proprietes.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        proprietes.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        proprietes.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        proprietes.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolation);
        return proprietes;
    }

    private static KafkaConsumer<String, String> consommateur(String groupe, String isolation) {
        return new KafkaConsumer<>(proprietesConsommateur(groupe, isolation));
    }

    // Sonde le topic jusqu'à avoir lu au moins `minimum` enregistrements ou épuisé le délai. Avec Integer.MAX_VALUE, sonde
    // pendant tout le délai : c'est ainsi qu'on prouve qu'un message n'arrive pas.
    private static List<ConsumerRecord<String, String>> lire(
            KafkaConsumer<String, String> consommateur, String topic, int minimum, Duration delai) {
        consommateur.subscribe(List.of(topic));
        List<ConsumerRecord<String, String>> lus = new ArrayList<>();
        long fin = System.nanoTime() + delai.toNanos();
        while (System.nanoTime() < fin && lus.size() < minimum) {
            consommateur.poll(Duration.ofMillis(200)).forEach(lus::add);
        }
        return lus;
    }

    private static List<String> valeurs(List<ConsumerRecord<String, String>> enregistrements) {
        return enregistrements.stream().map(ConsumerRecord::value).toList();
    }

    private static void envoyerSansTransaction(String topic, String valeur) throws Exception {
        try (KafkaProducer<String, String> producteur = producteur(null)) {
            producteur.send(new ProducerRecord<>(topic, "cle", valeur)).get();
        }
    }

    private static Map<TopicPartition, OffsetAndMetadata> offsetsDuGroupe(String groupe) throws Exception {
        try (Admin admin = Admin.create(Map.<String, Object>of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker().getBrokersAsString()))) {
            return admin.listConsumerGroupOffsets(groupe).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
        }
    }

    private static void fermerSansErreur(KafkaProducer<String, String> producteur) {
        try {
            producteur.close(Duration.ofSeconds(5));
        } catch (RuntimeException ignoree) {
            // Un producteur fencé ne peut plus être fermé proprement : sans importance pour le laboratoire.
        }
    }
}
