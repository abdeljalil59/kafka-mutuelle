package io.github.abdeljalil59.mutuelle.prelevement.adapter.in.kafka;

import io.github.abdeljalil59.mutuelle.events.avro.CotisationEmise;
import io.github.abdeljalil59.mutuelle.prelevement.application.EchecTraitementLotException;
import io.github.abdeljalil59.mutuelle.prelevement.application.TraiterDemandePrelevement;
import io.github.abdeljalil59.mutuelle.prelevement.domain.Decision;
import io.github.abdeljalil59.mutuelle.prelevement.domain.DemandePrelevement;
import io.github.abdeljalil59.mutuelle.prelevement.domain.PrelevementInvalideException;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.support.serializer.SerializationUtils;
import org.springframework.stereotype.Component;

@Component
public class CotisationEmiseListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(CotisationEmiseListener.class);
    private static final LogAccessor LOG_ACCESSOR = new LogAccessor(CotisationEmiseListener.class);

    private final TraiterDemandePrelevement traiterDemandePrelevement;

    public CotisationEmiseListener(TraiterDemandePrelevement traiterDemandePrelevement) {
        this.traiterDemandePrelevement = traiterDemandePrelevement;
    }

    // Reçoit tous les enregistrements d'un poll (au plus max.poll.records). Le préfixe de messages valides est écrit
    // en un seul lot ; au premier message illisible ou invalide, le préfixe est traité puis le listener signale
    // l'index fautif : le conteneur valide les offsets qui précèdent, rejoue ou envoie au topic de rejets ce message,
    // et redélivre les suivants.
    @KafkaListener(topics = "${prelevement.kafka.topic}", batch = "true")
    public void ecouter(List<ConsumerRecord<String, CotisationEmise>> messages) {
        List<DemandePrelevement> demandes = new ArrayList<>();
        BatchListenerFailedException premierRejet = null;
        for (int index = 0; index < messages.size() && premierRejet == null; index++) {
            premierRejet = convertir(messages.get(index), index, demandes);
        }

        List<Decision> decisions = traiter(demandes);
        for (int index = 0; index < decisions.size(); index++) {
            journaliser(messages.get(index), decisions.get(index));
        }
        if (premierRejet != null) {
            throw premierRejet;
        }
    }

    // Ajoute la demande du message à la liste ; retourne l'échec à signaler si le message est inutilisable.
    private static BatchListenerFailedException convertir(
            ConsumerRecord<String, CotisationEmise> message, int index, List<DemandePrelevement> demandes) {
        CotisationEmise evenement = message.value();
        if (evenement == null) {
            // Avec un listener batch, rien ne route automatiquement un message illisible (doc spring-kafka) :
            // ErrorHandlingDeserializer laisse la valeur à null et l'exception dans l'en-tête, qu'on retransmet
            // pour que le gestionnaire d'erreurs la classe (DeserializationException : pas de réessai).
            return new BatchListenerFailedException("message illisible", SerializationUtils.getExceptionFromHeader(
                    message, SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER, LOG_ACCESSOR), index);
        }
        try {
            // L'Avro ne porte aucun invariant : DemandePrelevement les vérifie (PrelevementInvalideException).
            demandes.add(new DemandePrelevement(
                    evenement.getIdEvenement(), evenement.getIdAdherent(), evenement.getMontant()));
            return null;
        } catch (PrelevementInvalideException e) {
            return new BatchListenerFailedException(e.getMessage(), e, index);
        }
    }

    private List<Decision> traiter(List<DemandePrelevement> demandes) {
        try {
            return traiterDemandePrelevement.traiterLot(demandes);
        } catch (EchecTraitementLotException e) {
            throw new BatchListenerFailedException(e.getMessage(), e.getCause(), e.index());
        }
    }

    private static void journaliser(ConsumerRecord<String, CotisationEmise> message, Decision decision) {
        CotisationEmise evenement = message.value();
        LOGGER.info("partition={} offset={} cle={} idEvenement={} montant={} decision={}",
                message.partition(), message.offset(), message.key(),
                evenement.getIdEvenement(), evenement.getMontant(), decision);
    }
}
