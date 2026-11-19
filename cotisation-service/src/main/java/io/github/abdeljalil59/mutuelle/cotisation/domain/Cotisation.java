package io.github.abdeljalil59.mutuelle.cotisation.domain;

import java.math.BigDecimal;
import java.time.Instant;

public record Cotisation(String idAdherent, BigDecimal montant, Instant emiseLe) {

    public Cotisation {
        exigerNonNull(idAdherent, "idAdherent");
        exigerNonNull(montant, "montant");
        exigerNonNull(emiseLe, "emiseLe");
        if (idAdherent.isBlank()) {
            throw new CotisationInvalideException("idAdherent ne doit pas être vide");
        }
        if (montant.signum() <= 0) {
            throw new CotisationInvalideException("montant doit être strictement positif");
        }
        // Le contrat Avro encode le montant en decimal(19,2) : refuser ici plutôt que de laisser une ligne
        // inconvertible bloquer l'outbox (le relais s'arrête à la première erreur du lot).
        if (montant.scale() > 2) {
            throw new CotisationInvalideException("montant doit avoir au plus 2 décimales");
        }
    }

    private static void exigerNonNull(Object valeur, String nom) {
        if (valeur == null) {
            throw new CotisationInvalideException(nom + " ne doit pas être null");
        }
    }
}
