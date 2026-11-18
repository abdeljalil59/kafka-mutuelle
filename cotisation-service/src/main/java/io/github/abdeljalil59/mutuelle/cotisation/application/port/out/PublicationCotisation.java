package io.github.abdeljalil59.mutuelle.cotisation.application.port.out;

public interface PublicationCotisation {

    // Publie un payload déjà sérialisé (JSON) ; l'échec ou l'absence de confirmation lève PublicationImpossibleException.
    void publier(String topic, String cle, String payload);
}
