package io.github.abdeljalil59.mutuelle.cotisation.application.port.out;

import io.github.abdeljalil59.mutuelle.cotisation.domain.Cotisation;
import java.util.UUID;

public interface PublicationCotisation {

    void publier(Cotisation cotisation, UUID idEvenement);
}
