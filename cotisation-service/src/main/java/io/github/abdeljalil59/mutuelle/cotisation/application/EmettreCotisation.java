package io.github.abdeljalil59.mutuelle.cotisation.application;

import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.Outbox;
import io.github.abdeljalil59.mutuelle.cotisation.domain.Cotisation;
import java.math.BigDecimal;
import java.time.Clock;
import java.util.UUID;
import java.util.function.Supplier;
import org.springframework.transaction.annotation.Transactional;

public class EmettreCotisation {

    private final Outbox outbox;
    private final Clock clock;
    private final Supplier<UUID> generateurIdEvenement;

    public EmettreCotisation(Outbox outbox, Clock clock, Supplier<UUID> generateurIdEvenement) {
        this.outbox = outbox;
        this.clock = clock;
        this.generateurIdEvenement = generateurIdEvenement;
    }

    // Enregistre l'événement dans l'outbox, dans une transaction : Kafka n'est plus dans le chemin de la réponse.
    // La publication est faite plus tard par le relais (RelayerOutbox).
    @Transactional
    public Cotisation emettre(String idAdherent, BigDecimal montant) {
        Cotisation cotisation = new Cotisation(idAdherent, montant, clock.instant());
        outbox.enregistrer(cotisation, generateurIdEvenement.get());
        return cotisation;
    }
}
