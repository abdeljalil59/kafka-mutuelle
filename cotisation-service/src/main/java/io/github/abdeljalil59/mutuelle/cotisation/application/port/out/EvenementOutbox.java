package io.github.abdeljalil59.mutuelle.cotisation.application.port.out;

import java.util.UUID;

// Événement en attente de publication : le payload est le JSON déjà sérialisé.
public record EvenementOutbox(UUID idEvenement, String topic, String cle, String payload) {
}
