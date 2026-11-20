package io.github.abdeljalil59.mutuelle.impaye.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

// Un prélèvement rejeté. idEvenement est celui de la cotisation d'origine : il identifie l'impayé et porte l'idempotence.
// decideLe est l'instant de la décision de rejet, pris dans le résultat reçu (pas l'instant de l'enregistrement).
public record Impaye(UUID idEvenement, String idAdherent, BigDecimal montant, Instant decideLe) {
}
