package io.github.abdeljalil59.mutuelle.impaye.adapter.in.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.github.abdeljalil59.mutuelle.events.ResultatPrelevement;
import io.github.abdeljalil59.mutuelle.events.TypeResultat;
import io.micrometer.core.instrument.MeterRegistry;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.micrometer.metrics.test.autoconfigure.AutoConfigureMetrics;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

// Le service de bout en bout : Kafka embarqué, registre factice (mock://) partagé avec le producteur de test, et la H2
// en mémoire du service (schema.sql appliqué par Spring Boot).
//
// Les résultats sont publiés dans des transactions Kafka, comme le fait prelevement-service : c'est ce qui donne un sens
// au read_committed du consommateur. Les tests « absent » n'attendent pas un délai : ils publient ensuite une SENTINELLE
// (même clé, donc même partition, donc lue après) et attendent qu'elle soit en base. Quand elle y est, tout ce qui la
// précède sur la partition a déjà été livré ou ne le sera jamais : l'absence est une preuve et non un délai trop court.
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.properties.schema.registry.url=" + ImpayeIntegrationTest.REGISTRE})
@AutoConfigureMetrics
@EmbeddedKafka(topics = "prelevements.resultats", partitions = 3, brokerProperties = {
        "transaction.state.log.replication.factor=1",
        "transaction.state.log.min.isr=1"})
@DirtiesContext
class ImpayeIntegrationTest {

    static final String REGISTRE = "mock://impaye-tests";

    private static final String TOPIC = "prelevements.resultats";
    // Plafond des attentes (rendu dès que la condition est vraie) : le premier message d'un contexte paie le démarrage
    // à froid du consommateur (adhésion au groupe, attribution des partitions).
    private static final Duration ATTENTE = Duration.ofSeconds(60);
    // Précision en microsecondes, comme le timestamp-micros du contrat : elle doit survivre à Avro puis à H2.
    private static final Instant DECIDE_LE = Instant.parse("2026-11-20T08:00:00.123456Z");

    private DefaultKafkaProducerFactory<String, ResultatPrelevement> usine;
    private KafkaTemplate<String, ResultatPrelevement> producteur;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private MeterRegistry meterRegistry;

    @Value("${local.server.port}")
    private int portWeb;

    @BeforeEach
    void ouvre_un_producteur_transactionnel() {
        Map<String, Object> proprietes = KafkaTestUtils.producerProps(embeddedKafka);
        proprietes.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        proprietes.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        proprietes.put("schema.registry.url", REGISTRE);
        usine = new DefaultKafkaProducerFactory<>(proprietes);
        usine.setTransactionIdPrefix("tx-test-impaye-");
        producteur = new KafkaTemplate<>(usine);
    }

    @AfterEach
    void ferme_le_producteur() {
        usine.destroy();
    }

    // PROUVE : un résultat REJETE publié (transaction validée) devient une ligne en base, avec ses valeurs, et l'instant de
    // décision traverse Avro (timestamp-micros) puis H2 sans perdre sa précision.
    @Test
    void un_rejet_est_persiste() {
        UUID idEvenement = UUID.randomUUID();

        publier(resultat(idEvenement, TypeResultat.REJETE, "adh-impaye-rejet", "150.00"));

        attendreQueLImpayeSoitEnBase(idEvenement);
        Ligne ligne = ligne(idEvenement);
        assertThat(ligne.idAdherent()).isEqualTo("adh-impaye-rejet");
        assertThat(ligne.montant()).isEqualByComparingTo("150.00");
        assertThat(ligne.decideLe().toInstant()).isEqualTo(DECIDE_LE);
        assertThat(ligne.enregistreLe()).isNotNull();
    }

    // PROUVE : un résultat ACCEPTE est lu mais ne crée aucun impayé. La sentinelle REJETE de même clé le suit sur la
    // partition : quand elle est en base, l'ACCEPTE a forcément été traité avant elle.
    @Test
    void un_accepte_n_est_pas_persiste() {
        String adherent = "adh-impaye-accepte";
        UUID accepte = UUID.randomUUID();
        UUID sentinelle = UUID.randomUUID();

        publier(resultat(accepte, TypeResultat.ACCEPTE, adherent, "75.00"));
        publier(resultat(sentinelle, TypeResultat.REJETE, adherent, "150.00"));

        attendreQueLImpayeSoitEnBase(sentinelle);
        assertThat(nombreDeLignes(accepte)).isZero();
    }

    // PROUVE (D11) : le même REJETE reçu deux fois (même idEvenement) laisse une seule ligne et gonfle le compteur de
    // doublons exactement d'une unité, sans exception ni réessai.
    @Test
    void un_rejet_rejoue_laisse_une_seule_ligne_et_incremente_le_compteur_de_doublons() {
        UUID idEvenement = UUID.randomUUID();
        ResultatPrelevement rejet = resultat(idEvenement, TypeResultat.REJETE, "adh-impaye-rejoue", "150.00");
        double avant = doublonsIgnores();

        publier(rejet);
        publier(rejet);

        await().atMost(ATTENTE).pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> assertThat(doublonsIgnores()).isEqualTo(avant + 1));
        assertThat(nombreDeLignes(idEvenement)).isEqualTo(1);
    }

    // PROUVE : l'isolation.level=read_committed du consommateur. Un REJETE écrit dans une transaction ANNULÉE est dans le
    // journal du topic mais n'est jamais livré : aucune ligne. Sans read_committed (read_uncommitted par défaut de
    // Kafka), ce test échouerait.
    @Test
    void un_rejet_d_une_transaction_annulee_n_est_pas_persiste() {
        String adherent = "adh-impaye-annule";
        UUID annule = UUID.randomUUID();
        UUID sentinelle = UUID.randomUUID();

        publierPuisAnnuler(resultat(annule, TypeResultat.REJETE, adherent, "150.00"));
        publier(resultat(sentinelle, TypeResultat.REJETE, adherent, "150.00"));

        attendreQueLImpayeSoitEnBase(sentinelle);
        assertThat(nombreDeLignes(annule)).isZero();
    }

    // PROUVE : Actuator expose le compteur de doublons au format Prometheus sur /actuator/prometheus (la cible du job
    // impaye-service de prometheus.yml). La série existe dès le démarrage, à 0.
    @Test
    void expose_le_compteur_de_doublons_au_format_prometheus() throws Exception {
        HttpRequest requete = HttpRequest.newBuilder(URI.create("http://localhost:" + portWeb + "/actuator/prometheus"))
                .GET().build();

        HttpResponse<String> reponse = HttpClient.newHttpClient().send(requete, HttpResponse.BodyHandlers.ofString());

        assertThat(reponse.statusCode()).isEqualTo(200);
        assertThat(reponse.body()).contains("impaye_doublons_ignores_total");
    }

    // ----------------------------------------------------------------------------------------------- outils

    private record Ligne(String idAdherent, BigDecimal montant, OffsetDateTime decideLe, OffsetDateTime enregistreLe) {
    }

    private static ResultatPrelevement resultat(UUID idEvenement, TypeResultat type, String idAdherent, String montant) {
        return ResultatPrelevement.newBuilder()
                .setIdEvenement(idEvenement)
                .setType(type)
                .setIdAdherent(idAdherent)
                .setMontant(new BigDecimal(montant))
                .setDecideLe(DECIDE_LE)
                .build();
    }

    // Une transaction par résultat, validée à la sortie du bloc.
    private void publier(ResultatPrelevement resultat) {
        producteur.executeInTransaction(operations -> {
            operations.send(TOPIC, resultat.getIdAdherent(), resultat);
            return null;
        });
    }

    // Le résultat est envoyé (le broker l'écrit) puis la transaction est annulée par l'exception.
    private void publierPuisAnnuler(ResultatPrelevement resultat) {
        assertThatThrownBy(() -> producteur.executeInTransaction(operations -> {
            operations.send(TOPIC, resultat.getIdAdherent(), resultat);
            throw new IllegalStateException("annulation voulue de la transaction");
        })).isInstanceOf(IllegalStateException.class);
    }

    private void attendreQueLImpayeSoitEnBase(UUID idEvenement) {
        await("impayé " + idEvenement + " en base").atMost(ATTENTE).pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> assertThat(nombreDeLignes(idEvenement)).isEqualTo(1));
    }

    private int nombreDeLignes(UUID idEvenement) {
        return jdbcClient.sql("SELECT COUNT(*) FROM impaye WHERE id_evenement = ?")
                .param(idEvenement.toString())
                .query(Integer.class)
                .single();
    }

    private Ligne ligne(UUID idEvenement) {
        return jdbcClient.sql("SELECT id_adherent, montant, decide_le, enregistre_le FROM impaye WHERE id_evenement = ?")
                .param(idEvenement.toString())
                .query((rs, numero) -> new Ligne(
                        rs.getString("id_adherent"),
                        rs.getBigDecimal("montant"),
                        rs.getObject("decide_le", OffsetDateTime.class),
                        rs.getObject("enregistre_le", OffsetDateTime.class)))
                .single();
    }

    private double doublonsIgnores() {
        return meterRegistry.get("impaye.doublons.ignores").counter().count();
    }
}
