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
import io.github.abdeljalil59.mutuelle.prelevement.domain.Decision;
import io.github.abdeljalil59.mutuelle.prelevement.domain.DecisionPrelevement;
import io.github.abdeljalil59.mutuelle.prelevement.domain.PlafondPrelevement;
import io.github.abdeljalil59.mutuelle.prelevement.domain.PrelevementInvalideException;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

// Listener en mode record, avec le vrai cas d'usage : le journal est un double (il ignore les doublons comme le MERGE) et
// la publication un collecteur.
class CotisationEmiseListenerTest {

    private static final String TOPIC = "cotisations.emises";
    private static final UUID ID_EVENEMENT = UUID.fromString("44444444-4444-4444-4444-444444444444");

    private final Logger logger = (Logger) LoggerFactory.getLogger(CotisationEmiseListener.class);
    private final ListAppender<ILoggingEvent> journal = new ListAppender<>();
    private final Set<UUID> idsDejaEnregistres = new HashSet<>();
    private final List<DecisionPrelevement> enregistrees = new ArrayList<>();
    private final List<DecisionPrelevement> publiees = new ArrayList<>();
    private final JournalDecisions journalDecisions = decisions -> decisions.stream()
            .filter(decision -> idsDejaEnregistres.add(decision.idEvenement()))
            .forEach(enregistrees::add);
    private CotisationEmiseListener listener;

    @BeforeEach
    void branche_le_journal_et_cree_le_listener() {
        logger.setLevel(Level.INFO);
        journal.start();
        logger.addAppender(journal);
        listener = listenerAvec(journalDecisions);
    }

    @AfterEach
    void debranche_le_journal() {
        logger.detachAppender(journal);
        journal.stop();
    }

    private CotisationEmiseListener listenerAvec(JournalDecisions journalUtilise) {
        return new CotisationEmiseListener(new TraiterDemandePrelevement(
                new PlafondPrelevement(new BigDecimal("100.00")), journalUtilise, publiees::add,
                Clock.fixed(Instant.parse("2026-11-18T09:00:00Z"), ZoneOffset.UTC)));
    }

    private static ConsumerRecord<String, CotisationEmise> message(int partition, long offset, String montant) {
        CotisationEmise evenement = CotisationEmise.newBuilder()
                .setIdEvenement(ID_EVENEMENT)
                .setIdAdherent("adh1")
                .setMontant(new BigDecimal(montant))
                .setEmiseLe(Instant.parse("2026-11-16T10:00:00Z"))
                .build();
        return new ConsumerRecord<>(TOPIC, partition, offset, "adh1", evenement);
    }

    private List<String> lignesDuJournal() {
        return journal.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
    }

    @Test
    void journalise_partition_offset_cle_et_decision_acceptee() {
        listener.ecouter(message(2, 7L, "100.00"));

        assertThat(lignesDuJournal()).containsExactly(
                "partition=2 offset=7 cle=adh1 idEvenement=44444444-4444-4444-4444-444444444444"
                        + " montant=100.00 decision=ACCEPTE");
    }

    @Test
    void journalise_une_decision_rejetee_au_dela_du_plafond() {
        listener.ecouter(message(0, 3L, "100.01"));

        assertThat(lignesDuJournal()).containsExactly(
                "partition=0 offset=3 cle=adh1 idEvenement=44444444-4444-4444-4444-444444444444"
                        + " montant=100.01 decision=REJETE");
    }

    @Test
    void conserve_l_echelle_du_montant_dans_le_journal() {
        listener.ecouter(message(1, 0L, "50.00"));

        assertThat(journal.list).hasSize(1);
        assertThat(journal.list.get(0).getFormattedMessage()).contains("montant=50.00").endsWith("decision=ACCEPTE");
    }

    @Test
    void publie_ACCEPTE_sous_le_plafond_et_REJETE_au_dela() {
        listener.ecouter(message(0, 1L, "100.00"));
        listener.ecouter(message(0, 2L, "100.01"));

        assertThat(publiees).extracting(DecisionPrelevement::decision).containsExactly(Decision.ACCEPTE, Decision.REJETE);
        assertThat(publiees).extracting(DecisionPrelevement::idAdherent).containsOnly("adh1");
    }

    // D4 : un message rejoué (même idEvenement) est ignoré par le journal comme le MERGE le fait, mais son résultat est
    // publié à nouveau, avec la même décision : une décision ACCEPTE reste ACCEPTE, elle ne devient pas REJETE.
    @Test
    void republie_la_meme_decision_quand_le_journal_ignore_le_message_rejoue() {
        listener.ecouter(message(0, 1L, "50.00"));
        listener.ecouter(message(0, 1L, "50.00"));

        assertThat(enregistrees).hasSize(1);
        assertThat(publiees).hasSize(2);
        assertThat(publiees).extracting(DecisionPrelevement::decision).containsOnly(Decision.ACCEPTE);
        assertThat(publiees).extracting(DecisionPrelevement::idEvenement).containsOnly(ID_EVENEMENT);
    }

    // L'Avro ne porte aucun invariant : c'est le domaine qui rejette, avant tout traitement. L'exception sort du listener
    // telle quelle : le gestionnaire d'erreurs du container la classe non réessayable (ErreursKafkaConfigTest).
    @Test
    void rejette_un_montant_negatif_par_le_domaine_sans_rien_ecrire_ni_publier() {
        assertThatExceptionOfType(PrelevementInvalideException.class)
                .isThrownBy(() -> listener.ecouter(message(0, 1L, "-5.00")))
                .withMessageContaining("montant");

        assertThat(journal.list).isEmpty();
        assertThat(enregistrees).isEmpty();
        assertThat(publiees).isEmpty();
    }

    @Test
    void laisse_remonter_l_echec_du_journal_sans_publier_ni_journaliser() {
        IllegalStateException echec = new IllegalStateException("base indisponible");
        CotisationEmiseListener listenerEnPanne = listenerAvec(decisions -> {
            throw echec;
        });

        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(() -> listenerEnPanne.ecouter(message(0, 1L, "50.00")))
                .isSameAs(echec);

        assertThat(publiees).isEmpty();
        assertThat(journal.list).isEmpty();
    }
}
