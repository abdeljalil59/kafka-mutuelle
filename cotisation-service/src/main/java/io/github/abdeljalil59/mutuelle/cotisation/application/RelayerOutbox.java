package io.github.abdeljalil59.mutuelle.cotisation.application;

import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.EvenementOutbox;
import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.Outbox;
import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.PublicationCotisation;
import java.time.Clock;
import java.util.List;

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
    // Un événement est marqué envoyé APRÈS l'accusé du broker. Au premier échec de publication, l'exception
    // remonte et le reste du lot n'est pas tenté : l'ordre par clé est conservé et les événements déjà
    // marqués le restent. Un plantage entre l'envoi et le marquage laisse l'événement non envoyé :
    // il est republié au passage suivant (doublon possible, absorbé par l'idempotence du consommateur).
    public int relayer() {
        List<EvenementOutbox> evenements = outbox.lireNonEnvoyes(tailleLot);
        for (EvenementOutbox evenement : evenements) {
            publication.publier(evenement.topic(), evenement.cle(), evenement.payload());
            outbox.marquerEnvoye(evenement.idEvenement(), clock.instant());
        }
        return evenements.size();
    }
}
