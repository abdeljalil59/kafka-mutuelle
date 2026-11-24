package io.github.abdeljalil59.mutuelle.cotisation.application.port.out;

import io.github.abdeljalil59.mutuelle.cotisation.domain.Cotisation;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface Outbox {

    // Enregistre l'événement à publier pour cette cotisation, avec l'identifiant fixé par l'appelant.
    void enregistrer(Cotisation cotisation, UUID idEvenement);

    // Réserve pour le relais jusqu'à {@code limite} événements non publiés : au plus un par clé (le plus ancien non
    // envoyé de la clé), du plus ancien au plus récent. Les lignes réservées sont verrouillées (FOR UPDATE SKIP LOCKED)
    // jusqu'à la fin de la transaction en cours : une autre instance les saute, et ne peut donc pas publier un
    // événement plus récent de la même clé tant que celui-ci n'est pas marqué envoyé.
    // À appeler dans une transaction (IllegalStateException sinon) : c'est elle qui tient les verrous jusqu'au marquage.
    List<EvenementOutbox> reserverNonEnvoyes(int limite);

    void marquerEnvoye(UUID idEvenement, Instant envoyeLe);
}
