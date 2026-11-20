package io.github.abdeljalil59.mutuelle.prelevement.adapter.out.kafka;

import io.github.abdeljalil59.mutuelle.events.ResultatPrelevement;
import io.github.abdeljalil59.mutuelle.events.TypeResultat;
import io.github.abdeljalil59.mutuelle.prelevement.application.PublicationResultatImpossibleException;
import io.github.abdeljalil59.mutuelle.prelevement.application.port.out.PublicationResultats;
import io.github.abdeljalil59.mutuelle.prelevement.domain.Decision;
import io.github.abdeljalil59.mutuelle.prelevement.domain.DecisionPrelevement;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.kafka.core.KafkaOperations;

public class PublicationResultatsKafka implements PublicationResultats {

    private final KafkaOperations<Object, Object> kafka;
    private final String topic;
    private final Duration delaiMaximal;

    public PublicationResultatsKafka(KafkaOperations<Object, Object> kafka, String topic, Duration delaiMaximal) {
        this.kafka = kafka;
        this.topic = topic;
        this.delaiMaximal = delaiMaximal;
    }

    // Clé Kafka = idAdherent (ordre par adhérent, comme cotisations.emises). L'événement reprend les valeurs de la
    // décision, dont son idEvenement et son horodatage : republier la même décision donne le même idEvenement.
    @Override
    public void publier(DecisionPrelevement decision) {
        ResultatPrelevement resultat = ResultatPrelevement.newBuilder()
                .setIdEvenement(decision.idEvenement())
                .setType(type(decision.decision()))
                .setIdAdherent(decision.idAdherent())
                .setMontant(decision.montant())
                .setDecideLe(decision.decideLe())
                .build();
        try {
            kafka.send(topic, decision.idAdherent(), resultat).get(delaiMaximal.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw new PublicationResultatImpossibleException(
                    "Échec de la publication du résultat de " + decision.idEvenement(), e.getCause());
        } catch (TimeoutException e) {
            throw new PublicationResultatImpossibleException(
                    "Publication non confirmée en " + delaiMaximal.toMillis() + " ms pour "
                            + decision.idEvenement() + " : le message a peut-être été écrit", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PublicationResultatImpossibleException(
                    "Publication interrompue pour " + decision.idEvenement(), e);
        } catch (RuntimeException e) {
            // Exception levée de façon synchrone par send() (par exemple métadonnées indisponibles).
            throw new PublicationResultatImpossibleException(
                    "Échec de la publication du résultat de " + decision.idEvenement(), e);
        }
    }

    // Switch exhaustif : un nouveau cas de Decision ne compile plus tant qu'il n'a pas son type de résultat.
    private static TypeResultat type(Decision decision) {
        return switch (decision) {
            case ACCEPTE -> TypeResultat.ACCEPTE;
            case REJETE -> TypeResultat.REJETE;
        };
    }
}
