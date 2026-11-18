package io.github.abdeljalil59.mutuelle.prelevement.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.github.abdeljalil59.mutuelle.prelevement.application.port.out.JournalDecisions;
import io.github.abdeljalil59.mutuelle.prelevement.domain.Decision;
import io.github.abdeljalil59.mutuelle.prelevement.domain.DecisionPrelevement;
import io.github.abdeljalil59.mutuelle.prelevement.domain.DemandePrelevement;
import io.github.abdeljalil59.mutuelle.prelevement.domain.PlafondPrelevement;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TraiterDemandePrelevementTest {

    private static final UUID ID_EVENEMENT = UUID.fromString("aaaaaaaa-0000-0000-0000-000000000001");
    private static final Instant DECIDE_LE = Instant.parse("2026-11-18T09:00:00Z");
    private static final Clock HORLOGE = Clock.fixed(DECIDE_LE, ZoneOffset.UTC);

    private final JournalDecisions journal = mock(JournalDecisions.class);

    private static DemandePrelevement demande(String montant) {
        return new DemandePrelevement(ID_EVENEMENT, "adh1", new BigDecimal(montant));
    }

    private TraiterDemandePrelevement avecPlafond(String plafond) {
        return new TraiterDemandePrelevement(new PlafondPrelevement(new BigDecimal(plafond)), journal, HORLOGE);
    }

    @Test
    void retourne_ACCEPTE_a_la_limite_du_plafond() {
        assertThat(avecPlafond("100.00").traiter(demande("100.00"))).isEqualTo(Decision.ACCEPTE);
    }

    @Test
    void retourne_REJETE_au_dela_du_plafond() {
        assertThat(avecPlafond("100.00").traiter(demande("100.01"))).isEqualTo(Decision.REJETE);
    }

    @Test
    void utilise_le_plafond_configure() {
        assertThat(avecPlafond("200.00").traiter(demande("150.00"))).isEqualTo(Decision.ACCEPTE);
        assertThat(avecPlafond("100.00").traiter(demande("150.00"))).isEqualTo(Decision.REJETE);
    }

    @Test
    void enregistre_la_decision_acceptee_dans_le_journal() {
        avecPlafond("100.00").traiter(demande("50.00"));

        verify(journal).enregistrer(new DecisionPrelevement(
                ID_EVENEMENT, "adh1", new BigDecimal("50.00"), Decision.ACCEPTE, DECIDE_LE));
    }

    @Test
    void enregistre_la_decision_rejetee_dans_le_journal() {
        avecPlafond("100.00").traiter(demande("100.01"));

        verify(journal).enregistrer(new DecisionPrelevement(
                ID_EVENEMENT, "adh1", new BigDecimal("100.01"), Decision.REJETE, DECIDE_LE));
    }

    @Test
    void laisse_remonter_une_exception_du_journal() {
        RuntimeException echec = new IllegalStateException("base indisponible");
        doThrow(echec).when(journal).enregistrer(any());

        assertThatThrownBy(() -> avecPlafond("100.00").traiter(demande("50.00"))).isSameAs(echec);
    }
}
