package io.github.abdeljalil59.mutuelle.prelevement.adapter.in.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.github.abdeljalil59.mutuelle.events.avro.CotisationEmise;
import io.github.abdeljalil59.mutuelle.prelevement.application.TraiterDemandePrelevement;
import io.github.abdeljalil59.mutuelle.prelevement.application.port.out.JournalDecisions;
import io.github.abdeljalil59.mutuelle.prelevement.domain.DecisionPrelevement;
import io.github.abdeljalil59.mutuelle.prelevement.domain.PlafondPrelevement;
import io.github.abdeljalil59.mutuelle.prelevement.domain.PrelevementInvalideException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.SerializationUtils;

class CotisationEmiseListenerTest {

    private static final String TOPIC = "cotisations.emises";
    private static final String ADHERENT_EN_ECHEC = "adh-en-echec";

    private final Logger logger = (Logger) LoggerFactory.getLogger(CotisationEmiseListener.class);
    private final ListAppender<ILoggingEvent> journal = new ListAppender<>();
    // Journal factice : il garde chaque lot reçu, et échoue pour tout lot contenant ADHERENT_EN_ECHEC.
    private final List<List<DecisionPrelevement>> lotsEcrits = new ArrayList<>();
    private final JournalDecisions journalDecisions = decisions -> {
        if (decisions.stream().anyMatch(decision -> ADHERENT_EN_ECHEC.equals(decision.idAdherent()))) {
            throw new IllegalStateException("base indisponible");
        }
        lotsEcrits.add(decisions);
    };
    private final CotisationEmiseListener listener = new CotisationEmiseListener(
            new TraiterDemandePrelevement(
                    new PlafondPrelevement(new BigDecimal("100.00")),
                    journalDecisions,
                    Clock.fixed(Instant.parse("2026-11-18T09:00:00Z"), ZoneOffset.UTC)));

    @BeforeEach
    void branche_le_journal() {
        logger.setLevel(Level.INFO);
        journal.start();
        logger.addAppender(journal);
    }

    @AfterEach
    void debranche_le_journal() {
        logger.detachAppender(journal);
        journal.stop();
    }

    private static UUID idEvenement(int numero) {
        return UUID.fromString(String.format("44444444-4444-4444-4444-%012d", numero));
    }

    private static ConsumerRecord<String, CotisationEmise> message(
            int numero, int partition, long offset, String idAdherent, String montant) {
        CotisationEmise evenement = CotisationEmise.newBuilder()
                .setIdEvenement(idEvenement(numero))
                .setIdAdherent(idAdherent)
                .setMontant(new BigDecimal(montant))
                .setEmiseLe(Instant.parse("2026-11-16T10:00:00Z"))
                .build();
        return new ConsumerRecord<>(TOPIC, partition, offset, "adh1", evenement);
    }

    private static ConsumerRecord<String, CotisationEmise> message(int partition, long offset, String montant) {
        return message(4, partition, offset, "adh1", montant);
    }

    // Valeur à null, comme laisse ErrorHandlingDeserializer un message illisible.
    private static ConsumerRecord<String, CotisationEmise> messageSansValeur(long offset) {
        return new ConsumerRecord<>(TOPIC, 0, offset, "adh1", null);
    }

    private List<String> lignesDuJournal() {
        return journal.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
    }

    @Test
    void journalise_partition_offset_cle_et_decision_acceptee() {
        listener.ecouter(List.of(message(2, 7L, "100.00")));

        assertThat(lignesDuJournal()).containsExactly(
                "partition=2 offset=7 cle=adh1 idEvenement=44444444-4444-4444-4444-000000000004"
                        + " montant=100.00 decision=ACCEPTE");
    }

    @Test
    void journalise_une_decision_rejetee_au_dela_du_plafond() {
        listener.ecouter(List.of(message(0, 3L, "100.01")));

        assertThat(lignesDuJournal()).containsExactly(
                "partition=0 offset=3 cle=adh1 idEvenement=44444444-4444-4444-4444-000000000004"
                        + " montant=100.01 decision=REJETE");
    }

    @Test
    void conserve_l_echelle_du_montant_dans_le_journal() {
        listener.ecouter(List.of(message(1, 0L, "50.00")));

        assertThat(journal.list).hasSize(1);
        assertThat(journal.list.get(0).getFormattedMessage()).contains("montant=50.00").endsWith("decision=ACCEPTE");
    }

    @Test
    void ecrit_tous_les_messages_du_lot_en_un_seul_appel_et_journalise_chacun_dans_l_ordre() {
        listener.ecouter(List.of(
                message(1, 0, 10L, "adh1", "50.00"),
                message(2, 0, 11L, "adh2", "150.00"),
                message(3, 1, 4L, "adh3", "100.00")));

        assertThat(lotsEcrits).hasSize(1);
        assertThat(lotsEcrits.get(0)).extracting(DecisionPrelevement::idEvenement)
                .containsExactly(idEvenement(1), idEvenement(2), idEvenement(3));
        assertThat(lignesDuJournal()).hasSize(3);
        assertThat(lignesDuJournal().get(0)).startsWith("partition=0 offset=10 ").endsWith("decision=ACCEPTE");
        assertThat(lignesDuJournal().get(1)).startsWith("partition=0 offset=11 ").endsWith("decision=REJETE");
        assertThat(lignesDuJournal().get(2)).startsWith("partition=1 offset=4 ").endsWith("decision=ACCEPTE");
    }

    @Test
    void un_lot_vide_n_ecrit_rien_et_ne_journalise_rien() {
        listener.ecouter(List.of());

        assertThat(lotsEcrits).allSatisfy(lot -> assertThat(lot).isEmpty());
        assertThat(journal.list).isEmpty();
    }

    // L'Avro ne porte aucun invariant (contrairement au record, dont le constructeur refusait ces valeurs à la
    // désérialisation) : c'est le domaine qui rejette, avant tout traitement.
    @Test
    void signale_un_montant_negatif_par_l_index_du_message_sans_rien_journaliser() {
        assertThatExceptionOfType(BatchListenerFailedException.class)
                .isThrownBy(() -> listener.ecouter(List.of(message(0, 1L, "-5.00"))))
                .withMessageContaining("montant")
                .satisfies(exception -> {
                    assertThat(exception.getIndex()).isZero();
                    assertThat(exception).hasCauseInstanceOf(PrelevementInvalideException.class);
                });

        assertThat(journal.list).isEmpty();
    }

    @Test
    void ecrit_et_journalise_les_messages_valides_avant_le_message_invalide_puis_signale_son_index() {
        List<ConsumerRecord<String, CotisationEmise>> messages = List.of(
                message(1, 0, 10L, "adh1", "50.00"),
                message(2, 0, 11L, "adh2", "50.00"),
                message(3, 0, 12L, "adh3", "-5.00"),
                message(4, 0, 13L, "adh4", "50.00"));

        assertThatExceptionOfType(BatchListenerFailedException.class)
                .isThrownBy(() -> listener.ecouter(messages))
                .satisfies(exception -> assertThat(exception.getIndex()).isEqualTo(2));

        // Le préfixe valide est écrit en un seul lot ; le message invalide et ceux qui le suivent ne le sont pas.
        assertThat(lotsEcrits).hasSize(1);
        assertThat(lotsEcrits.get(0)).extracting(DecisionPrelevement::idEvenement)
                .containsExactly(idEvenement(1), idEvenement(2));
        assertThat(lignesDuJournal()).hasSize(2);
    }

    @Test
    void signale_un_message_sans_valeur_par_son_index_apres_avoir_ecrit_ceux_qui_le_precedent() {
        List<ConsumerRecord<String, CotisationEmise>> messages = List.of(
                message(1, 0, 10L, "adh1", "50.00"), messageSansValeur(11L), message(3, 0, 12L, "adh3", "50.00"));

        assertThatExceptionOfType(BatchListenerFailedException.class)
                .isThrownBy(() -> listener.ecouter(messages))
                .satisfies(exception -> assertThat(exception.getIndex()).isEqualTo(1));

        assertThat(lotsEcrits).hasSize(1);
        assertThat(lotsEcrits.get(0)).extracting(DecisionPrelevement::idEvenement).containsExactly(idEvenement(1));
    }

    @Test
    void retransmet_l_exception_de_deserialisation_de_l_en_tete_comme_cause_d_un_message_sans_valeur() {
        ConsumerRecord<String, CotisationEmise> illisible = messageSansValeur(11L);
        SerializationUtils.deserializationException(illisible.headers(),
                "ceci-n-est-pas-de-l-avro".getBytes(StandardCharsets.UTF_8), new IllegalStateException("octets invalides"),
                false);

        assertThatExceptionOfType(BatchListenerFailedException.class)
                .isThrownBy(() -> listener.ecouter(List.of(illisible)))
                .satisfies(exception -> {
                    assertThat(exception.getIndex()).isZero();
                    assertThat(exception).hasCauseInstanceOf(DeserializationException.class);
                });
    }

    @Test
    void signale_l_index_de_la_decision_non_enregistree_quand_l_ecriture_echoue() {
        List<ConsumerRecord<String, CotisationEmise>> messages = List.of(
                message(1, 0, 10L, "adh1", "50.00"),
                message(2, 0, 11L, ADHERENT_EN_ECHEC, "50.00"),
                message(3, 0, 12L, "adh3", "50.00"));

        assertThatExceptionOfType(BatchListenerFailedException.class)
                .isThrownBy(() -> listener.ecouter(messages))
                .satisfies(exception -> {
                    assertThat(exception.getIndex()).isEqualTo(1);
                    assertThat(exception).hasCauseInstanceOf(IllegalStateException.class);
                });

        // Le lot a échoué ; rejoué une à une, seule la décision de l'index 0 a pu être écrite.
        assertThat(lotsEcrits).hasSize(1);
        assertThat(lotsEcrits.get(0)).extracting(DecisionPrelevement::idEvenement).containsExactly(idEvenement(1));
    }
}
