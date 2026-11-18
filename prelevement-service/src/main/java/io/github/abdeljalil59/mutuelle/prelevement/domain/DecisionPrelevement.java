package io.github.abdeljalil59.mutuelle.prelevement.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record DecisionPrelevement(
        UUID idEvenement,
        String idAdherent,
        BigDecimal montant,
        Decision decision,
        Instant decideLe) {
}
