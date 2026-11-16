package io.github.abdeljalil59.mutuelle.prelevement.domain;

import java.math.BigDecimal;

public record PlafondPrelevement(BigDecimal valeur) {

    public PlafondPrelevement {
        // Erreur de configuration (pas une règle du domaine) : l'application échoue au démarrage.
        if (valeur == null || valeur.signum() <= 0) {
            throw new IllegalArgumentException("le plafond doit être strictement positif");
        }
    }

    // Rejeté si le montant est strictement supérieur au plafond.
    // compareTo ignore l'échelle : 100, 100.0 et 100.00 sont égaux.
    public Decision decider(DemandePrelevement demande) {
        return demande.montant().compareTo(valeur) > 0 ? Decision.REJETE : Decision.ACCEPTE;
    }
}
