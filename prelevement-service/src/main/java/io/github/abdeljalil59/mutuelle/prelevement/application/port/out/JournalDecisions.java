package io.github.abdeljalil59.mutuelle.prelevement.application.port.out;

import io.github.abdeljalil59.mutuelle.prelevement.domain.DecisionPrelevement;
import java.util.List;

public interface JournalDecisions {

    // Idempotent : une décision dont l'idEvenement est déjà enregistré est ignorée, sans exception.
    // Les décisions sont écrites en un seul lot ; une liste vide n'écrit rien.
    void enregistrer(List<DecisionPrelevement> decisions);
}
