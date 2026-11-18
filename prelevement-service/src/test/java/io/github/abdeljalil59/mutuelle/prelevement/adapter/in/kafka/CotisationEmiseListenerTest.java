package io.github.abdeljalil59.mutuelle.prelevement.adapter.in.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.github.abdeljalil59.mutuelle.events.CotisationEmise;
import io.github.abdeljalil59.mutuelle.prelevement.application.TraiterDemandePrelevement;
import io.github.abdeljalil59.mutuelle.prelevement.domain.PlafondPrelevement;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class CotisationEmiseListenerTest {

    private static final String TOPIC = "cotisations.emises";
    private static final UUID ID_EVENEMENT = UUID.fromString("44444444-4444-4444-4444-444444444444");

    private final Logger logger = (Logger) LoggerFactory.getLogger(CotisationEmiseListener.class);
    private final ListAppender<ILoggingEvent> journal = new ListAppender<>();
    // Journal sans effet : ce test ne vérifie que le journal applicatif du listener.
    private final CotisationEmiseListener listener = new CotisationEmiseListener(
            new TraiterDemandePrelevement(
                    new PlafondPrelevement(new BigDecimal("100.00")),
                    decision -> { },
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

    private static ConsumerRecord<String, CotisationEmise> message(int partition, long offset, String montant) {
        CotisationEmise evenement = new CotisationEmise(
                ID_EVENEMENT, "adh1", new BigDecimal(montant), Instant.parse("2026-11-16T10:00:00Z"));
        return new ConsumerRecord<>(TOPIC, partition, offset, "adh1", evenement);
    }

    @Test
    void journalise_partition_offset_cle_et_decision_acceptee() {
        listener.ecouter(message(2, 7L, "100.00"));

        assertThat(journal.list).extracting(ILoggingEvent::getFormattedMessage).containsExactly(
                "partition=2 offset=7 cle=adh1 idEvenement=44444444-4444-4444-4444-444444444444"
                        + " montant=100.00 decision=ACCEPTE");
    }

    @Test
    void journalise_une_decision_rejetee_au_dela_du_plafond() {
        listener.ecouter(message(0, 3L, "100.01"));

        assertThat(journal.list).extracting(ILoggingEvent::getFormattedMessage).containsExactly(
                "partition=0 offset=3 cle=adh1 idEvenement=44444444-4444-4444-4444-444444444444"
                        + " montant=100.01 decision=REJETE");
    }

    @Test
    void conserve_l_echelle_du_montant_dans_le_journal() {
        listener.ecouter(message(1, 0L, "50.00"));

        assertThat(journal.list).hasSize(1);
        assertThat(journal.list.get(0).getFormattedMessage()).contains("montant=50.00").endsWith("decision=ACCEPTE");
    }
}
