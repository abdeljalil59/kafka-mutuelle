package io.github.abdeljalil59.mutuelle.stats.adapter.in.kafka;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.github.abdeljalil59.mutuelle.events.ResultatPrelevement;
import io.github.abdeljalil59.mutuelle.events.TypeResultat;
import io.github.abdeljalil59.mutuelle.stats.domain.StatsAdherent;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Prouve la logique d'agrégation sans broker ni Spring : TopologyTestDriver et registre Avro factice (mock://). */
class StatsTopologyTest {

    private static final String TOPIC = "prelevements.resultats";
    private static final String STORE = "stats-store";

    private SpecificAvroSerde<ResultatPrelevement> resultatSerde;
    private Topology topology;
    private TopologyTestDriver driver;
    private TestInputTopic<String, ResultatPrelevement> entree;
    private KeyValueStore<String, StatsAdherent> store;

    @BeforeEach
    void demarrer() {
        resultatSerde = new SpecificAvroSerde<>();
        resultatSerde.configure(Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                "mock://stats-" + UUID.randomUUID()), false);

        StreamsBuilder builder = new StreamsBuilder();
        new StatsTopology(TOPIC, STORE, Duration.ofDays(7), resultatSerde, new StatsAdherentSerde()).definir(builder);
        topology = builder.build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stats-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        driver = new TopologyTestDriver(topology, props);
        entree = driver.createInputTopic(TOPIC, new StringSerializer(), resultatSerde.serializer());
        store = driver.getKeyValueStore(STORE);
    }

    @AfterEach
    void arreter() {
        driver.close();
        resultatSerde.close();
    }

    @Test
    void lesAcceptesEtLesRejetesSontComptesSeparementParAdherent() {
        envoyer("ADH-1", TypeResultat.ACCEPTE, "42.50", 1_000L);
        envoyer("ADH-1", TypeResultat.ACCEPTE, "42.50", 2_000L);
        envoyer("ADH-1", TypeResultat.REJETE, "150.00", 3_000L);

        StatsAdherent stats = store.get("ADH-1");
        assertThat(stats.nombreAcceptes()).isEqualTo(2);
        assertThat(stats.nombreRejetes()).isEqualTo(1);
    }

    @Test
    void leMontantTotalCumuleTousLesResultats() {
        envoyer("ADH-1", TypeResultat.ACCEPTE, "42.50", 1_000L);
        envoyer("ADH-1", TypeResultat.REJETE, "150.00", 2_000L);

        assertThat(store.get("ADH-1").montantTotal()).isEqualTo(192.50);
    }

    @Test
    void lesAdherentsSontAgreguesIndependamment() {
        envoyer("ADH-1", TypeResultat.ACCEPTE, "10.00", 1_000L);
        envoyer("ADH-2", TypeResultat.REJETE, "200.00", 2_000L);

        assertThat(store.get("ADH-1")).isEqualTo(new StatsAdherent(1, 0, 10.0, 1_000L));
        assertThat(store.get("ADH-2")).isEqualTo(new StatsAdherent(0, 1, 200.0, 2_000L));
    }

    @Test
    void laCleDuMessageEstRemplaceeParIdAdherent() {
        // Clé d'entrée sans rapport avec l'adhérent : seule la valeur compte pour l'agrégation (selectKey).
        entree.pipeInput("n-importe-quoi", resultat("ADH-9", TypeResultat.ACCEPTE, "5.00", 1_000L));
        entree.pipeInput("autre-cle", resultat("ADH-9", TypeResultat.ACCEPTE, "5.00", 2_000L));

        assertThat(store.get("ADH-9").nombreAcceptes()).isEqualTo(2);
        assertThat(store.get("n-importe-quoi")).isNull();
    }

    @Test
    void ledernierEvenementEstLeResultatLePlusRecent() {
        envoyer("ADH-1", TypeResultat.ACCEPTE, "1.00", 9_000L);
        envoyer("ADH-1", TypeResultat.ACCEPTE, "1.00", 4_000L);

        assertThat(store.get("ADH-1").dernierEvenement()).isEqualTo(9_000L);
    }

    @Test
    void unAdherentInconnuEstAbsentDuStore() {
        assertThat(store.get("ADH-INCONNU")).isNull();
    }

    @Test
    void leChangementDeClePasseParUnTopicDeRepartitionnement() {
        assertThat(topology.describe().toString()).contains("repartition");
    }

    @Test
    void leStateStoreNommeEstDeclareDansLaTopology() {
        assertThat(topology.describe().toString()).contains(STORE);
    }

    @Test
    void unMemeIdEvenementRecuDeuxFoisNEstCompteQuUneFois() {
        UUID idEvenement = UUID.randomUUID();
        entree.pipeInput("ADH-1", resultat(idEvenement, "ADH-1", TypeResultat.ACCEPTE, "42.50", 1_000L));
        entree.pipeInput("ADH-1", resultat(idEvenement, "ADH-1", TypeResultat.ACCEPTE, "42.50", 1_000L));

        assertThat(store.get("ADH-1")).isEqualTo(new StatsAdherent(1, 0, 42.50, 1_000L));
    }

    @Test
    void deuxIdEvenementDistinctsAuContenuIdentiqueSAdditionnent() {
        entree.pipeInput("ADH-1", resultat(UUID.randomUUID(), "ADH-1", TypeResultat.ACCEPTE, "10.00", 1_000L));
        entree.pipeInput("ADH-1", resultat(UUID.randomUUID(), "ADH-1", TypeResultat.ACCEPTE, "10.00", 1_000L));

        assertThat(store.get("ADH-1")).isEqualTo(new StatsAdherent(2, 0, 20.0, 1_000L));
    }

    @Test
    void rejouerUnLotEntierLaisseLesStatsInchangees() {
        List<ResultatPrelevement> lot = List.of(
                resultat(UUID.randomUUID(), "ADH-1", TypeResultat.ACCEPTE, "10.00", 1_000L),
                resultat(UUID.randomUUID(), "ADH-1", TypeResultat.REJETE, "150.00", 2_000L),
                resultat(UUID.randomUUID(), "ADH-2", TypeResultat.ACCEPTE, "42.50", 3_000L));
        Instant horodatage = Instant.ofEpochMilli(5_000L);
        lot.forEach(r -> entree.pipeInput(r.getIdAdherent(), r, horodatage));
        assertThat(store.get("ADH-1")).isEqualTo(new StatsAdherent(1, 1, 160.0, 2_000L));
        assertThat(store.get("ADH-2")).isEqualTo(new StatsAdherent(1, 0, 42.50, 3_000L));

        lot.forEach(r -> entree.pipeInput(r.getIdAdherent(), r, horodatage)); // rejeu complet

        assertThat(store.get("ADH-1")).isEqualTo(new StatsAdherent(1, 1, 160.0, 2_000L));
        assertThat(store.get("ADH-2")).isEqualTo(new StatsAdherent(1, 0, 42.50, 3_000L));
    }

    @Test
    void unIdEvenementRejoueApresLaRetentionEstRecompte() {
        UUID rejoue = UUID.randomUUID();
        Instant debut = Instant.ofEpochMilli(0);
        entree.pipeInput("ADH-1", resultat(rejoue, "ADH-1", TypeResultat.ACCEPTE, "10.00", 1_000L), debut);

        // 6 jours plus tard, sous la rétention de 7 jours : le doublon est ignoré
        entree.pipeInput("ADH-1", resultat(rejoue, "ADH-1", TypeResultat.ACCEPTE, "10.00", 1_000L),
                debut.plus(Duration.ofDays(6)));
        assertThat(store.get("ADH-1").nombreAcceptes()).isEqualTo(1);

        // un événement distinct à 8 jours fait passer le temps du flux au-delà de la rétention
        entree.pipeInput("ADH-1", resultat(UUID.randomUUID(), "ADH-1", TypeResultat.ACCEPTE, "5.00", 2_000L),
                debut.plus(Duration.ofDays(8)));

        // le premier événement revient avec son horodatage d'origine : il n'est plus retrouvé, donc recompté
        entree.pipeInput("ADH-1", resultat(rejoue, "ADH-1", TypeResultat.ACCEPTE, "10.00", 1_000L), debut);

        assertThat(store.get("ADH-1")).isEqualTo(new StatsAdherent(3, 0, 25.0, 2_000L));
    }

    @Test
    void unDoublonNAjouteAucuneLigneAuChangelogDeLAgregat() {
        TestOutputTopic<String, StatsAdherent> changelog = driver.createOutputTopic(
                "stats-test-stats-store-changelog", new StringDeserializer(), new StatsAdherentSerde().deserializer());
        UUID idEvenement = UUID.randomUUID();

        entree.pipeInput("ADH-1", resultat(idEvenement, "ADH-1", TypeResultat.ACCEPTE, "42.50", 1_000L));
        assertThat(changelog.readKeyValuesToList()).hasSize(1); // témoin : un événement nouveau écrit une ligne

        entree.pipeInput("ADH-1", resultat(idEvenement, "ADH-1", TypeResultat.ACCEPTE, "42.50", 1_000L));

        assertThat(changelog.isEmpty()).isTrue();
    }

    private void envoyer(String idAdherent, TypeResultat type, String montant, long decideLeMillis) {
        entree.pipeInput(idAdherent, resultat(idAdherent, type, montant, decideLeMillis));
    }

    private static ResultatPrelevement resultat(String idAdherent, TypeResultat type, String montant,
                                                long decideLeMillis) {
        return resultat(UUID.randomUUID(), idAdherent, type, montant, decideLeMillis);
    }

    private static ResultatPrelevement resultat(UUID idEvenement, String idAdherent, TypeResultat type,
                                                String montant, long decideLeMillis) {
        return ResultatPrelevement.newBuilder()
                .setIdEvenement(idEvenement)
                .setType(type)
                .setIdAdherent(idAdherent)
                .setMontant(new BigDecimal(montant))
                .setDecideLe(Instant.ofEpochMilli(decideLeMillis))
                .build();
    }
}
