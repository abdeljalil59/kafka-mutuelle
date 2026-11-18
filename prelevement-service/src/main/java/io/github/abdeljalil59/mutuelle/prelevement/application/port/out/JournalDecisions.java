package io.github.abdeljalil59.mutuelle.prelevement.application.port.out;

import io.github.abdeljalil59.mutuelle.prelevement.domain.DecisionPrelevement;

public interface JournalDecisions {

    // Idempotent : enregistrer deux fois une décision de même idEvenement n'a pas d'effet à la seconde fois.
    void enregistrer(DecisionPrelevement decision);
}
