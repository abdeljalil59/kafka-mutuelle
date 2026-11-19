package io.github.abdeljalil59.mutuelle.prelevement.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import io.github.abdeljalil59.mutuelle.prelevement.application.port.out.JournalDecisions;
import io.github.abdeljalil59.mutuelle.prelevement.domain.Decision;
import io.github.abdeljalil59.mutuelle.prelevement.domain.DecisionPrelevement;
import io.github.abdeljalil59.mutuelle.prelevement.domain.DemandePrelevement;
import io.github.abdeljalil59.mutuelle.prelevement.domain.PlafondPrelevement;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

class TraiterDemandePrelevementTest {

    private static final Instant DECIDE_LE = Instant.parse("2026-11-18T09:00:00Z");
    private static final Clock HORLOGE = Clock.fixed(DECIDE_LE, ZoneOffset.UTC);

    private final JournalDecisions journal = mock(JournalDecisions.class);

    private static UUID idEvenement(int numero) {
        return UUID.fromString(String.format("aaaaaaaa-0000-0000-0000-%012d", numero));
    }

    private static DemandePrelevement demande(int numero, String montant) {
        return new DemandePrelevement(idEvenement(numero), "adh" + numero, new BigDecimal(montant));
    }

    private static DecisionPrelevement decision(int numero, String montant, Decision decision) {
        return new DecisionPrelevement(
                idEvenement(numero), "adh" + numero, new BigDecimal(montant), decision, DECIDE_LE);
    }

    private TraiterDemandePrelevement avecPlafond(String plafond) {
        return new TraiterDemandePrelevement(new PlafondPrelevement(new BigDecimal(plafond)), journal, HORLOGE);
    }

    private Decision traiterUne(String plafond, String montant) {
        return avecPlafond(plafond).traiterLot(List.of(demande(1, montant))).get(0);
    }

    @Test
    void retourne_ACCEPTE_a_la_limite_du_plafond() {
        assertThat(traiterUne("100.00", "100.00")).isEqualTo(Decision.ACCEPTE);
    }

    @Test
    void retourne_REJETE_au_dela_du_plafond() {
        assertThat(traiterUne("100.00", "100.01")).isEqualTo(Decision.REJETE);
    }

    @Test
    void utilise_le_plafond_configure() {
        assertThat(traiterUne("200.00", "150.00")).isEqualTo(Decision.ACCEPTE);
        assertThat(traiterUne("100.00", "150.00")).isEqualTo(Decision.REJETE);
    }

    @Test
    void enregistre_la_decision_acceptee_dans_le_journal() {
        avecPlafond("100.00").traiterLot(List.of(demande(1, "50.00")));

        verify(journal).enregistrer(List.of(decision(1, "50.00", Decision.ACCEPTE)));
    }

    @Test
    void enregistre_la_decision_rejetee_dans_le_journal() {
        avecPlafond("100.00").traiterLot(List.of(demande(1, "100.01")));

        verify(journal).enregistrer(List.of(decision(1, "100.01", Decision.REJETE)));
    }

    @Test
    void ecrit_tout_le_lot_en_un_seul_appel_et_retourne_les_decisions_dans_l_ordre() {
        List<DemandePrelevement> demandes = List.of(demande(1, "50.00"), demande(2, "150.00"), demande(3, "100.00"));

        List<Decision> decisions = avecPlafond("100.00").traiterLot(demandes);

        assertThat(decisions).containsExactly(Decision.ACCEPTE, Decision.REJETE, Decision.ACCEPTE);
        verify(journal).enregistrer(List.of(
                decision(1, "50.00", Decision.ACCEPTE),
                decision(2, "150.00", Decision.REJETE),
                decision(3, "100.00", Decision.ACCEPTE)));
        verifyNoMoreInteractions(journal);
    }

    @Test
    void un_lot_vide_ne_donne_aucune_decision_et_aucune_erreur() {
        assertThat(avecPlafond("100.00").traiterLot(List.of())).isEmpty();
    }

    @Test
    void localise_la_premiere_decision_en_echec_en_rejouant_le_lot_une_par_une() {
        RuntimeException echec = new IllegalStateException("valeur trop longue");
        doThrow(new IllegalStateException("lot en échec")).when(journal).enregistrer(anyList());
        doNothing().when(journal).enregistrer(List.of(decision(1, "50.00", Decision.ACCEPTE)));
        doThrow(echec).when(journal).enregistrer(List.of(decision(2, "50.00", Decision.ACCEPTE)));

        List<DemandePrelevement> demandes = List.of(demande(1, "50.00"), demande(2, "50.00"), demande(3, "50.00"));

        assertThatThrownBy(() -> avecPlafond("100.00").traiterLot(demandes))
                .isInstanceOfSatisfying(EchecTraitementLotException.class, exception -> {
                    assertThat(exception.index()).isEqualTo(1);
                    assertThat(exception).hasCause(echec);
                });
        // Le lot, puis les décisions 1 et 2 une à une : la 3 n'est jamais tentée.
        InOrder ordre = inOrder(journal);
        ordre.verify(journal).enregistrer(List.of(
                decision(1, "50.00", Decision.ACCEPTE),
                decision(2, "50.00", Decision.ACCEPTE),
                decision(3, "50.00", Decision.ACCEPTE)));
        ordre.verify(journal).enregistrer(List.of(decision(1, "50.00", Decision.ACCEPTE)));
        ordre.verify(journal).enregistrer(List.of(decision(2, "50.00", Decision.ACCEPTE)));
        verify(journal, never()).enregistrer(List.of(decision(3, "50.00", Decision.ACCEPTE)));
    }

    @Test
    void un_echec_passager_du_lot_n_est_pas_une_erreur_si_chaque_decision_passe_seule() {
        doThrow(new IllegalStateException("lot en échec passager")).when(journal).enregistrer(
                List.of(decision(1, "50.00", Decision.ACCEPTE), decision(2, "50.00", Decision.ACCEPTE)));

        List<Decision> decisions = avecPlafond("100.00").traiterLot(List.of(demande(1, "50.00"), demande(2, "50.00")));

        assertThat(decisions).containsExactly(Decision.ACCEPTE, Decision.ACCEPTE);
        verify(journal).enregistrer(List.of(decision(1, "50.00", Decision.ACCEPTE)));
        verify(journal).enregistrer(List.of(decision(2, "50.00", Decision.ACCEPTE)));
    }

    @Test
    void ne_rejoue_pas_un_lot_d_une_seule_decision_et_signale_l_index_zero() {
        RuntimeException echec = new IllegalStateException("base indisponible");
        doThrow(echec).when(journal).enregistrer(any());

        assertThatThrownBy(() -> avecPlafond("100.00").traiterLot(List.of(demande(1, "50.00"))))
                .isInstanceOfSatisfying(EchecTraitementLotException.class, exception -> {
                    assertThat(exception.index()).isZero();
                    assertThat(exception).hasCause(echec);
                });
        verify(journal, times(1)).enregistrer(any());
    }
}
