package io.github.abdeljalil59.mutuelle.cotisation.application;

import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.PublicationCotisation;
import io.github.abdeljalil59.mutuelle.cotisation.domain.Cotisation;
import java.math.BigDecimal;
import java.time.Clock;
import java.util.UUID;
import java.util.function.Supplier;

public class EmettreCotisation {

    private final PublicationCotisation publication;
    private final Clock clock;
    private final Supplier<UUID> generateurIdEvenement;

    public EmettreCotisation(PublicationCotisation publication, Clock clock, Supplier<UUID> generateurIdEvenement) {
        this.publication = publication;
        this.clock = clock;
        this.generateurIdEvenement = generateurIdEvenement;
    }

    public Cotisation emettre(String idAdherent, BigDecimal montant) {
        Cotisation cotisation = new Cotisation(idAdherent, montant, clock.instant());
        publication.publier(cotisation, generateurIdEvenement.get());
        return cotisation;
    }
}
