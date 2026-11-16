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
    }

    private static void exigerNonNull(Object valeur, String nom) {
        if (valeur == null) {
            throw new CotisationInvalideException(nom + " ne doit pas être null");
        }
    }
}
