package io.github.abdeljalil59.mutuelle.events;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Événement publié sur le topic {@code cotisations.emises}.
 * La clé Kafka du message est {@code idAdherent}.
 */
public record CotisationEmise(
        UUID idEvenement,
        String idAdherent,
        BigDecimal montant,
        Instant emiseLe) {

    public CotisationEmise {
        Objects.requireNonNull(idEvenement, "idEvenement ne doit pas être null");
        Objects.requireNonNull(idAdherent, "idAdherent ne doit pas être null");
        Objects.requireNonNull(montant, "montant ne doit pas être null");
        Objects.requireNonNull(emiseLe, "emiseLe ne doit pas être null");
        if (idAdherent.isBlank()) {
            throw new IllegalArgumentException("idAdherent ne doit pas être vide");
        }
        if (montant.signum() <= 0) {
            throw new IllegalArgumentException("montant doit être strictement positif");
        }
    }
}
