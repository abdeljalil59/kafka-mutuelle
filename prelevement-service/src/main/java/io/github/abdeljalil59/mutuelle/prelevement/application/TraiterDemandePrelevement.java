package io.github.abdeljalil59.mutuelle.prelevement.application;

import io.github.abdeljalil59.mutuelle.prelevement.application.port.out.JournalDecisions;
import io.github.abdeljalil59.mutuelle.prelevement.domain.Decision;
import io.github.abdeljalil59.mutuelle.prelevement.domain.DecisionPrelevement;
import io.github.abdeljalil59.mutuelle.prelevement.domain.DemandePrelevement;
import io.github.abdeljalil59.mutuelle.prelevement.domain.PlafondPrelevement;
import java.time.Clock;

public class TraiterDemandePrelevement {

    private final PlafondPrelevement plafond;
    private final JournalDecisions journalDecisions;
    private final Clock clock;

    public TraiterDemandePrelevement(PlafondPrelevement plafond, JournalDecisions journalDecisions, Clock clock) {
        this.plafond = plafond;
        this.journalDecisions = journalDecisions;
        this.clock = clock;
    }

    public Decision traiter(DemandePrelevement demande) {
        Decision decision = plafond.decider(demande);
        journalDecisions.enregistrer(new DecisionPrelevement(
                demande.idEvenement(), demande.idAdherent(), demande.montant(), decision, clock.instant()));
        return decision;
    }
}
