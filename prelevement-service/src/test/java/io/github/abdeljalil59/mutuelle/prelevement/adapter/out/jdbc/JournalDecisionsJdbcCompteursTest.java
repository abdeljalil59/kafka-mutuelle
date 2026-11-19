package io.github.abdeljalil59.mutuelle.prelevement.adapter.out.jdbc;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.github.abdeljalil59.mutuelle.prelevement.domain.Decision;
import io.github.abdeljalil59.mutuelle.prelevement.domain.DecisionPrelevement;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

// Correspondance résultat du MERGE -> compteur, sans base : le JdbcTemplate et le MeterRegistry sont simulés. Que
// Oracle renvoie bien 1 et 0 à travers Spring est prouvé par JournalDecisionsJdbcTest (vrai Oracle, vrai registre).
class JournalDecisionsJdbcCompteursTest {

    private static final Instant DECIDE_LE = Instant.parse("2026-11-19T09:00:00Z");
    // Résultat de batchUpdate quand le driver ne détaille pas (java.sql.Statement.SUCCESS_NO_INFO).
    private static final int SUCCES_SANS_DETAIL = -2;

    private final JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
    private final MeterRegistry registre = mock(MeterRegistry.class);
    private final Counter acceptees = mock(Counter.class);
    private final Counter rejetees = mock(Counter.class);
    private final Counter doublons = mock(Counter.class);
    private JournalDecisionsJdbc journal;

    @BeforeEach
    void cree_l_adaptateur_avec_les_trois_compteurs() {
        // Les noms et les étiquettes attendus sont ceux du contrat Prometheus : prelevement_decisions_total{decision}
        // et prelevement_doublons_ignores_total. Un autre nom ou une autre étiquette renverrait null.
        when(registre.counter("prelevement.decisions", "decision", "ACCEPTE")).thenReturn(acceptees);
        when(registre.counter("prelevement.decisions", "decision", "REJETE")).thenReturn(rejetees);
        when(registre.counter("prelevement.doublons.ignores")).thenReturn(doublons);
        journal = new JournalDecisionsJdbc(jdbcTemplate, registre);
    }

    private static DecisionPrelevement decision(int numero, Decision decision) {
        return new DecisionPrelevement(UUID.fromString(String.format("cccccccc-0000-0000-0000-%012d", numero)),
                "adh" + numero, new BigDecimal("50.00"), decision, DECIDE_LE);
    }

    private void leMergeRenvoie(int... resultats) {
        when(jdbcTemplate.batchUpdate(anyString(), anyList())).thenReturn(resultats);
    }

    @Test
    void incremente_le_compteur_de_chaque_decision_avec_son_etiquette() {
        leMergeRenvoie(1, 1);

        journal.enregistrer(List.of(decision(1, Decision.ACCEPTE), decision(2, Decision.REJETE)));

        verify(acceptees, times(1)).increment();
        verify(rejetees, times(1)).increment();
        verifyNoInteractions(doublons);
    }

    @Test
    void incremente_les_doublons_ignores_quand_batchUpdate_renvoie_0_pour_une_ligne() {
        leMergeRenvoie(1, 0);

        journal.enregistrer(List.of(decision(1, Decision.ACCEPTE), decision(2, Decision.REJETE)));

        verify(acceptees, times(1)).increment();
        verify(doublons, times(1)).increment();
        // La ligne ignorée n'est pas une décision enregistrée : le compteur de sa décision ne bouge pas.
        verify(rejetees, never()).increment();
    }

    @Test
    void compte_chaque_ligne_a_son_propre_resultat() {
        leMergeRenvoie(0, 1, 0, 1);

        journal.enregistrer(List.of(
                decision(1, Decision.ACCEPTE), decision(2, Decision.REJETE),
                decision(3, Decision.REJETE), decision(4, Decision.ACCEPTE)));

        verify(acceptees, times(1)).increment();
        verify(rejetees, times(1)).increment();
        verify(doublons, times(2)).increment();
    }

    @Test
    void ne_compte_pas_un_resultat_que_le_driver_ne_detaille_pas() {
        leMergeRenvoie(SUCCES_SANS_DETAIL, 1);

        journal.enregistrer(List.of(decision(1, Decision.ACCEPTE), decision(2, Decision.REJETE)));

        verify(acceptees, never()).increment();
        verify(rejetees, times(1)).increment();
        verifyNoInteractions(doublons);
    }

    @Test
    void ne_compte_rien_pour_un_lot_vide() {
        leMergeRenvoie();

        journal.enregistrer(List.of());

        verifyNoInteractions(acceptees, rejetees, doublons);
    }

    @Test
    void laisse_remonter_l_erreur_sans_rien_compter_quand_le_lot_echoue() {
        IllegalStateException echec = new IllegalStateException("base indisponible");
        when(jdbcTemplate.batchUpdate(anyString(), anyList())).thenThrow(echec);

        assertThatThrownBy(() -> journal.enregistrer(List.of(decision(1, Decision.ACCEPTE)))).isSameAs(echec);

        verifyNoInteractions(acceptees, rejetees, doublons);
    }
}
