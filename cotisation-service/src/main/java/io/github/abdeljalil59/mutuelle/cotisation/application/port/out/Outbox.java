package io.github.abdeljalil59.mutuelle.cotisation.application.port.out;

import io.github.abdeljalil59.mutuelle.cotisation.domain.Cotisation;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface Outbox {

    // Enregistre l'événement à publier pour cette cotisation, avec l'identifiant fixé par l'appelant.
    void enregistrer(Cotisation cotisation, UUID idEvenement);

    // Événements non encore publiés, du plus ancien au plus récent, au plus {@code limite}.
    List<EvenementOutbox> lireNonEnvoyes(int limite);

    void marquerEnvoye(UUID idEvenement, Instant envoyeLe);
}
