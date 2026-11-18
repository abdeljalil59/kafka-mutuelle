package io.github.abdeljalil59.mutuelle.prelevement.adapter.in.kafka;

import io.github.abdeljalil59.mutuelle.events.CotisationEmise;
import io.github.abdeljalil59.mutuelle.prelevement.application.TraiterDemandePrelevement;
import io.github.abdeljalil59.mutuelle.prelevement.domain.Decision;
import io.github.abdeljalil59.mutuelle.prelevement.domain.DemandePrelevement;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class CotisationEmiseListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(CotisationEmiseListener.class);

    private final TraiterDemandePrelevement traiterDemandePrelevement;

    public CotisationEmiseListener(TraiterDemandePrelevement traiterDemandePrelevement) {
        this.traiterDemandePrelevement = traiterDemandePrelevement;
    }

    @KafkaListener(topics = "${prelevement.kafka.topic}")
    public void ecouter(ConsumerRecord<String, CotisationEmise> message) {
        CotisationEmise evenement = message.value();
        DemandePrelevement demande = new DemandePrelevement(evenement.idEvenement(), evenement.idAdherent(), evenement.montant());
        Decision decision = traiterDemandePrelevement.traiter(demande);
        LOGGER.info("partition={} offset={} cle={} idEvenement={} montant={} decision={}",
                message.partition(), message.offset(), message.key(),
                evenement.idEvenement(), evenement.montant(), decision);
    }
}
