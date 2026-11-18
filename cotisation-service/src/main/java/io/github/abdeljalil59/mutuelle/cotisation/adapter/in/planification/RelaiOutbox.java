package io.github.abdeljalil59.mutuelle.cotisation.adapter.in.planification;

import io.github.abdeljalil59.mutuelle.cotisation.application.RelayerOutbox;
import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.PublicationImpossibleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

public class RelaiOutbox {

    private static final Logger LOGGER = LoggerFactory.getLogger(RelaiOutbox.class);

    private final RelayerOutbox relayerOutbox;

    public RelaiOutbox(RelayerOutbox relayerOutbox) {
        this.relayerOutbox = relayerOutbox;
    }

    // fixedDelay : le passage suivant démarre après la fin du précédent, jamais en parallèle (un seul relais).
    @Scheduled(fixedDelayString = "${mutuelle.cotisation.outbox.delai-relais-ms}")
    public void relayer() {
        try {
            int publies = relayerOutbox.relayer();
            if (publies > 0) {
                LOGGER.info("outbox : {} événement(s) publié(s)", publies);
            }
        } catch (PublicationImpossibleException e) {
            LOGGER.warn("outbox : publication impossible, nouvel essai au prochain passage : {}", e.getMessage());
        } catch (RuntimeException e) {
            LOGGER.error("outbox : échec du relais", e);
        }
    }
}
