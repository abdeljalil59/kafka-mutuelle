package io.github.abdeljalil59.mutuelle.prelevement.domain;

import java.math.BigDecimal;

public record DemandePrelevement(String idAdherent, BigDecimal montant) {

    public DemandePrelevement {
        exigerNonNull(idAdherent, "idAdherent");
        exigerNonNull(montant, "montant");
        if (idAdherent.isBlank()) {
            throw new PrelevementInvalideException("idAdherent ne doit pas être vide");
        }
        if (montant.signum() <= 0) {
            throw new PrelevementInvalideException("montant doit être strictement positif");
        }
    }

    private static void exigerNonNull(Object valeur, String nom) {
        if (valeur == null) {
            throw new PrelevementInvalideException(nom + " ne doit pas être null");
        }
    }
}
