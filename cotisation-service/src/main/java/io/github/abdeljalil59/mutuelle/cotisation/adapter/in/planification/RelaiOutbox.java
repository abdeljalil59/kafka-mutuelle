package io.github.abdeljalil59.mutuelle.cotisation.adapter.in.planification;

import io.github.abdeljalil59.mutuelle.cotisation.application.RelayerOutbox;
import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.PublicationImpossibleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

public class RelaiOutbox {

    private static final Logger LOGGER = LoggerFactory.getLogger(RelaiOutbox.class);

    private final RelayerOutbox relayerOutbox;

    private final int passagesMax;

    public RelaiOutbox(RelayerOutbox relayerOutbox, int passagesMax) {
        this.relayerOutbox = relayerOutbox;
        this.passagesMax = passagesMax;
    }

    // fixedDelay : le déclenchement suivant démarre après la fin du précédent, jamais en parallèle sur une instance.
    // Un déclenchement enchaîne des passages tant que le précédent a réservé au moins un événement (un passage ne
    // publie qu'une tête par clé : après un passage, d'autres têtes ont pu apparaître), au plus passagesMax. Au plafond,
    // ou à la première exception, la boucle s'arrête ; le déclenchement suivant reprend après le délai fixe.
    // Toujours au moins un passage, même si passagesMax est inférieur à 1.
    @Scheduled(fixedDelayString = "${mutuelle.cotisation.outbox.delai-relais-ms}")
    public void relayer() {
        try {
            int passages = 0;
            int publies;
            do {
                publies = relayerOutbox.relayer();
                passages++;
                if (publies > 0) {
                    LOGGER.info("outbox : {} événement(s) publié(s)", publies);
                }
            } while (publies > 0 && passages < passagesMax);
        } catch (PublicationImpossibleException e) {
            LOGGER.warn("outbox : publication impossible, nouvel essai au prochain passage : {}", e.getMessage());
        } catch (RuntimeException e) {
            LOGGER.error("outbox : échec du relais", e);
        }
    }
}
