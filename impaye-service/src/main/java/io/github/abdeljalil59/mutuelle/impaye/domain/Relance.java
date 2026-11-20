package io.github.abdeljalil59.mutuelle.impaye.domain;

import java.math.BigDecimal;
import java.time.Instant;

// Relance d'un adhérent dont le nombre de prélèvements rejetés a atteint le seuil. dernierMontant est celui du rejet
// qui a déclenché la relance ; relanceLe est l'instant de la décision de relancer.
public record Relance(String idAdherent, long nombreRejets, BigDecimal dernierMontant, Instant relanceLe) {
}
