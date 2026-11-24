package io.github.abdeljalil59.mutuelle.cotisation.application;

import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.EvenementOutbox;
import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.Outbox;
import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.PublicationCotisation;
import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.PublicationImpossibleException;
import java.time.Clock;
import java.util.List;
import org.springframework.transaction.annotation.Transactional;

public class RelayerOutbox {

    private final Outbox outbox;
    private final PublicationCotisation publication;
    private final Clock clock;
    private final int tailleLot;

    public RelayerOutbox(Outbox outbox, PublicationCotisation publication, Clock clock, int tailleLot) {
        this.outbox = outbox;
        this.publication = publication;
        this.clock = clock;
        this.tailleLot = tailleLot;
    }

    // Publie les événements non envoyés, du plus ancien au plus récent, et retourne leur nombre.
    // Un passage est une transaction : lecture, publications et marquages sont validés ensemble.
    // Un événement est marqué envoyé APRÈS l'accusé du broker. Au premier échec de publication
    // (PublicationImpossibleException) ou de conversion (IllegalArgumentException), la transaction est VALIDÉE
    // (noRollbackFor) avant que l'exception remonte : les événements déjà publiés et marqués le restent, le reste
    // du lot n'est pas tenté (l'ordre par clé est conservé).
    // Toute autre exception (par exemple une erreur SQL au marquage) annule le passage : les événements déjà
    // publiés du lot seront republiés au passage suivant (doublon avec le même idEvenement, absorbé par
    // l'idempotence du consommateur).
    @Transactional(noRollbackFor = {PublicationImpossibleException.class, IllegalArgumentException.class})
    public int relayer() {
        List<EvenementOutbox> evenements = outbox.reserverNonEnvoyes(tailleLot);
        for (EvenementOutbox evenement : evenements) {
            publication.publier(evenement.topic(), evenement.cle(), evenement.payload());
            outbox.marquerEnvoye(evenement.idEvenement(), clock.instant());
        }
        return evenements.size();
    }
}
