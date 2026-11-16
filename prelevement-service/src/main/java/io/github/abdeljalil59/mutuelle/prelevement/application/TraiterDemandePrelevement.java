package io.github.abdeljalil59.mutuelle.prelevement.application;

import io.github.abdeljalil59.mutuelle.prelevement.domain.Decision;
import io.github.abdeljalil59.mutuelle.prelevement.domain.DemandePrelevement;
import io.github.abdeljalil59.mutuelle.prelevement.domain.PlafondPrelevement;

public class TraiterDemandePrelevement {

    private final PlafondPrelevement plafond;

    public TraiterDemandePrelevement(PlafondPrelevement plafond) {
        this.plafond = plafond;
    }

    public Decision traiter(DemandePrelevement demande) {
        return plafond.decider(demande);
    }
}
