package io.github.abdeljalil59.mutuelle.prelevement.application;

import io.github.abdeljalil59.mutuelle.prelevement.application.port.out.JournalDecisions;
import io.github.abdeljalil59.mutuelle.prelevement.application.port.out.PublicationResultats;
import io.github.abdeljalil59.mutuelle.prelevement.domain.Decision;
import io.github.abdeljalil59.mutuelle.prelevement.domain.DecisionPrelevement;
import io.github.abdeljalil59.mutuelle.prelevement.domain.DemandePrelevement;
import io.github.abdeljalil59.mutuelle.prelevement.domain.PlafondPrelevement;
import java.time.Clock;
import java.util.List;

public class TraiterDemandePrelevement {

    private final PlafondPrelevement plafond;
    private final JournalDecisions journalDecisions;
    private final PublicationResultats publicationResultats;
    private final Clock clock;

    public TraiterDemandePrelevement(PlafondPrelevement plafond, JournalDecisions journalDecisions,
            PublicationResultats publicationResultats, Clock clock) {
        this.plafond = plafond;
        this.journalDecisions = journalDecisions;
        this.publicationResultats = publicationResultats;
        this.clock = clock;
    }

    // Décide, enregistre dans le journal, puis publie le résultat. Le résultat publié est la décision métier (plafond),
    // dans tous les cas : le journal ignore un doublon (message rejoué) sans le signaler, et la décision est republiée
    // quand même. Après un échec de la publication, le réessai du message réécrit la ligne (ignorée par le journal) puis
    // republie : le résultat n'est jamais perdu, il peut être publié plusieurs fois avec le même idEvenement.
    public Decision traiter(DemandePrelevement demande) {
        DecisionPrelevement decision = new DecisionPrelevement(
                demande.idEvenement(), demande.idAdherent(), demande.montant(),
                plafond.decider(demande), clock.instant());
        journalDecisions.enregistrer(List.of(decision));
        publicationResultats.publier(decision);
        return decision.decision();
    }
}
