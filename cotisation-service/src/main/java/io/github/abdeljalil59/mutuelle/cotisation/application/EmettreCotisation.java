package io.github.abdeljalil59.mutuelle.cotisation.application;

import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.PublicationCotisation;
import io.github.abdeljalil59.mutuelle.cotisation.domain.Cotisation;
import java.math.BigDecimal;
import java.time.Clock;

public class EmettreCotisation {

    private final PublicationCotisation publication;
    private final Clock clock;

    public EmettreCotisation(PublicationCotisation publication, Clock clock) {
        this.publication = publication;
        this.clock = clock;
    }

    public Cotisation emettre(String idAdherent, BigDecimal montant) {
        Cotisation cotisation = new Cotisation(idAdherent, montant, clock.instant());
        publication.publier(cotisation);
        return cotisation;
    }
}
