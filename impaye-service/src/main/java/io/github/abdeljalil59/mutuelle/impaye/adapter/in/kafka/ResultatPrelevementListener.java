package io.github.abdeljalil59.mutuelle.impaye.adapter.in.kafka;

import io.github.abdeljalil59.mutuelle.events.ResultatPrelevement;
import io.github.abdeljalil59.mutuelle.events.TypeResultat;
import io.github.abdeljalil59.mutuelle.impaye.application.usecase.TraiterRejet;
import io.github.abdeljalil59.mutuelle.impaye.domain.Impaye;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

// Le listener ne fait que filtrer et traduire : un rejet est confié au cas d'usage TraiterRejet, une acceptation n'en crée
// pas. Il ne connaît ni la persistance ni la relance.
@Component
public class ResultatPrelevementListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(ResultatPrelevementListener.class);

    private final TraiterRejet traiterRejet;

    public ResultatPrelevementListener(TraiterRejet traiterRejet) {
        this.traiterRejet = traiterRejet;
    }

    // Un enregistrement par appel : une exception isole le message fautif, que le gestionnaire d'erreurs réessaie.
    // Aucune transaction Kafka ici : l'offset est validé par le container après le traitement, et la relance éventuelle
    // est publiée hors transaction (voir TraiterRejet pour la limite qui en découle).
    @KafkaListener(topics = "${impaye.kafka.topic}")
    public void ecouter(ConsumerRecord<String, ResultatPrelevement> message) {
        ResultatPrelevement resultat = message.value();
        if (resultat.getType() != TypeResultat.REJETE) {
            LOGGER.debug("partition={} offset={} cle={} idEvenement={} résultat {} ignoré",
                    message.partition(), message.offset(), message.key(), resultat.getIdEvenement(), resultat.getType());
            return;
        }
        traiterRejet.traiter(new Impaye(
                resultat.getIdEvenement(), resultat.getIdAdherent(), resultat.getMontant(), resultat.getDecideLe()));
        LOGGER.info("partition={} offset={} cle={} idEvenement={} montant={} impayé traité",
                message.partition(), message.offset(), message.key(), resultat.getIdEvenement(), resultat.getMontant());
    }
}
