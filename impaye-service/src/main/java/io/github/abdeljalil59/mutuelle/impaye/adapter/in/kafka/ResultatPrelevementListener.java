package io.github.abdeljalil59.mutuelle.impaye.adapter.in.kafka;

import io.github.abdeljalil59.mutuelle.events.ResultatPrelevement;
import io.github.abdeljalil59.mutuelle.events.TypeResultat;
import io.github.abdeljalil59.mutuelle.impaye.application.port.out.PersistanceImpaye;
import io.github.abdeljalil59.mutuelle.impaye.domain.Impaye;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

// Pas de cas d'usage entre le listener et le port : la seule règle est « un rejet crée un impayé, une acceptation n'en
// crée pas » (D9, première version). Un cas d'usage apparaîtra avec la relance après N rejets.
@Component
public class ResultatPrelevementListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(ResultatPrelevementListener.class);

    private final PersistanceImpaye persistanceImpaye;

    public ResultatPrelevementListener(PersistanceImpaye persistanceImpaye) {
        this.persistanceImpaye = persistanceImpaye;
    }

    // Un enregistrement par appel : une exception isole le message fautif, que le gestionnaire d'erreurs réessaie.
    // Aucune transaction Kafka ici : le service ne publie rien, l'offset est validé par le container après le traitement.
    @KafkaListener(topics = "${impaye.kafka.topic}")
    public void ecouter(ConsumerRecord<String, ResultatPrelevement> message) {
        ResultatPrelevement resultat = message.value();
        if (resultat.getType() != TypeResultat.REJETE) {
            LOGGER.debug("partition={} offset={} cle={} idEvenement={} résultat {} ignoré",
                    message.partition(), message.offset(), message.key(), resultat.getIdEvenement(), resultat.getType());
            return;
        }
        persistanceImpaye.enregistrer(new Impaye(
                resultat.getIdEvenement(), resultat.getIdAdherent(), resultat.getMontant(), resultat.getDecideLe()));
        LOGGER.info("partition={} offset={} cle={} idEvenement={} montant={} impayé traité",
                message.partition(), message.offset(), message.key(), resultat.getIdEvenement(), resultat.getMontant());
    }
}
