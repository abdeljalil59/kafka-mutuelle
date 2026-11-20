package io.github.abdeljalil59.mutuelle.impaye.application.port.out;

import io.github.abdeljalil59.mutuelle.impaye.application.PublicationRelanceImpossibleException;
import io.github.abdeljalil59.mutuelle.impaye.domain.Relance;

public interface PublicationRelance {

    // Rend la main une fois la relance confirmée par le broker. Si elle ne l'est pas, lève
    // PublicationRelanceImpossibleException.
    void publier(Relance relance) throws PublicationRelanceImpossibleException;
}
