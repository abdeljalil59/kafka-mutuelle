package io.github.abdeljalil59.mutuelle.prelevement.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.github.abdeljalil59.mutuelle.prelevement.application.port.out.JournalDecisions;
import io.github.abdeljalil59.mutuelle.prelevement.application.port.out.PublicationResultats;
import io.github.abdeljalil59.mutuelle.prelevement.domain.Decision;
import io.github.abdeljalil59.mutuelle.prelevement.domain.DecisionPrelevement;
import io.github.abdeljalil59.mutuelle.prelevement.domain.DemandePrelevement;
import io.github.abdeljalil59.mutuelle.prelevement.domain.PlafondPrelevement;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

class TraiterDemandePrelevementTest {

    private static final UUID ID_EVENEMENT = UUID.fromString("aaaaaaaa-0000-0000-0000-000000000001");
    private static final Instant DECIDE_LE = Instant.parse("2026-11-18T09:00:00Z");
    private static final Clock HORLOGE = Clock.fixed(DECIDE_LE, ZoneOffset.UTC);

    private final JournalDecisions journal = mock(JournalDecisions.class);
    private final PublicationResultats publication = mock(PublicationResultats.class);

    private static DemandePrelevement demande(String montant) {
        return new DemandePrelevement(ID_EVENEMENT, "adh1", new BigDecimal(montant));
    }

    private static DecisionPrelevement decision(String montant, Decision decision) {
        return new DecisionPrelevement(ID_EVENEMENT, "adh1", new BigDecimal(montant), decision, DECIDE_LE);
    }

    private TraiterDemandePrelevement avecPlafond(String plafond) {
        return avecPlafond(plafond, journal);
    }

    private TraiterDemandePrelevement avecPlafond(String plafond, JournalDecisions journalUtilise) {
        return new TraiterDemandePrelevement(
                new PlafondPrelevement(new BigDecimal(plafond)), journalUtilise, publication, HORLOGE);
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
    void enregistre_la_decision_dans_le_journal() {
        avecPlafond("100.00").traiter(demande("50.00"));

        verify(journal).enregistrer(List.of(decision("50.00", Decision.ACCEPTE)));
    }

    @Test
    void publie_le_resultat_de_la_decision_acceptee_ou_rejetee() {
        avecPlafond("100.00").traiter(demande("50.00"));
        avecPlafond("100.00").traiter(demande("100.01"));

        verify(publication).publier(decision("50.00", Decision.ACCEPTE));
        verify(publication).publier(decision("100.01", Decision.REJETE));
    }

    @Test
    void publie_apres_avoir_enregistre() {
        avecPlafond("100.00").traiter(demande("50.00"));

        InOrder ordre = inOrder(journal, publication);
        ordre.verify(journal).enregistrer(List.of(decision("50.00", Decision.ACCEPTE)));
        ordre.verify(publication).publier(decision("50.00", Decision.ACCEPTE));
    }

    // Le journal ignore un doublon sans le dire (MERGE) : la décision est quand même republiée, avec le même idEvenement.
    // C'est ce qui garantit qu'un résultat dont la publication avait échoué est publié au réessai.
    @Test
    void republie_un_message_deja_enregistre_que_le_journal_ignore() {
        Set<UUID> deja = new HashSet<>();
        List<DecisionPrelevement> ecrites = new ArrayList<>();
        JournalDecisions journalQuiIgnoreLesDoublons = decisions -> decisions.stream()
                .filter(d -> deja.add(d.idEvenement()))
                .forEach(ecrites::add);
        TraiterDemandePrelevement traitement = avecPlafond("100.00", journalQuiIgnoreLesDoublons);

        traitement.traiter(demande("50.00"));
        traitement.traiter(demande("50.00"));

        assertThat(ecrites).hasSize(1);
        verify(publication, times(2)).publier(decision("50.00", Decision.ACCEPTE));
    }

    @Test
    void ne_publie_rien_quand_le_journal_echoue() {
        RuntimeException echec = new IllegalStateException("base indisponible");
        doThrow(echec).when(journal).enregistrer(any());

        assertThatThrownBy(() -> avecPlafond("100.00").traiter(demande("50.00"))).isSameAs(echec);

        verify(publication, never()).publier(any());
    }

    @Test
    void laisse_remonter_l_echec_de_publication_apres_l_ecriture_au_journal() {
        PublicationResultatImpossibleException echec =
                new PublicationResultatImpossibleException("broker injoignable", new IllegalStateException());
        doThrow(echec).when(publication).publier(any());

        assertThatThrownBy(() -> avecPlafond("100.00").traiter(demande("50.00"))).isSameAs(echec);

        verify(journal).enregistrer(List.of(decision("50.00", Decision.ACCEPTE)));
    }
}
