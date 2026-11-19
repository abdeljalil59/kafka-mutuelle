package io.github.abdeljalil59.mutuelle.prelevement.application;

import io.github.abdeljalil59.mutuelle.prelevement.application.port.out.JournalDecisions;
import io.github.abdeljalil59.mutuelle.prelevement.domain.Decision;
import io.github.abdeljalil59.mutuelle.prelevement.domain.DecisionPrelevement;
import io.github.abdeljalil59.mutuelle.prelevement.domain.DemandePrelevement;
import io.github.abdeljalil59.mutuelle.prelevement.domain.PlafondPrelevement;
import java.time.Clock;
import java.util.List;

public class TraiterDemandePrelevement {

    private final PlafondPrelevement plafond;
    private final JournalDecisions journalDecisions;
    private final Clock clock;

    public TraiterDemandePrelevement(PlafondPrelevement plafond, JournalDecisions journalDecisions, Clock clock) {
        this.plafond = plafond;
        this.journalDecisions = journalDecisions;
        this.clock = clock;
    }

    // Décide chaque demande puis écrit toutes les décisions en un seul lot. Retourne les décisions dans l'ordre des
    // demandes. Si l'écriture échoue, EchecTraitementLotException indique la première demande non enregistrée.
    public List<Decision> traiterLot(List<DemandePrelevement> demandes) {
        List<DecisionPrelevement> decisions = demandes.stream().map(this::decider).toList();
        try {
            journalDecisions.enregistrer(decisions);
        } catch (RuntimeException echecDuLot) {
            rejouerUneParUne(decisions, echecDuLot);
        }
        return decisions.stream().map(DecisionPrelevement::decision).toList();
    }

    private DecisionPrelevement decider(DemandePrelevement demande) {
        return new DecisionPrelevement(demande.idEvenement(), demande.idAdherent(), demande.montant(),
                plafond.decider(demande), clock.instant());
    }

    // Le lot a échoué sans dire quelle décision était en cause : on les rejoue une à une pour la trouver. Le journal
    // est idempotent, rejouer celles déjà écrites est sans effet. Si toutes passent une à une, l'échec du lot était
    // passager et tout est écrit. Un lot d'une décision n'a rien à localiser : l'échec est le sien.
    private void rejouerUneParUne(List<DecisionPrelevement> decisions, RuntimeException echecDuLot) {
        if (decisions.size() == 1) {
            throw new EchecTraitementLotException(0, echecDuLot);
        }
        for (int index = 0; index < decisions.size(); index++) {
            try {
                journalDecisions.enregistrer(List.of(decisions.get(index)));
            } catch (RuntimeException echec) {
                throw new EchecTraitementLotException(index, echec);
            }
        }
    }
}
