package io.github.abdeljalil59.mutuelle.cotisation.adapter.out.kafka;

import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.PublicationCotisation;
import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.PublicationImpossibleException;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.kafka.core.KafkaTemplate;

public class PublicationCotisationKafka implements PublicationCotisation {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final Duration delaiMaximal;

    public PublicationCotisationKafka(KafkaTemplate<String, String> kafkaTemplate, Duration delaiMaximal) {
        this.kafkaTemplate = kafkaTemplate;
        this.delaiMaximal = delaiMaximal;
    }

    @Override
    public void publier(String topic, String cle, String payload) {
        try {
            kafkaTemplate.send(topic, cle, payload)
                    .get(delaiMaximal.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw new PublicationImpossibleException(
                    "Échec de la publication de l'événement de " + cle, e.getCause());
        } catch (TimeoutException e) {
            throw new PublicationImpossibleException(
                    "Publication non confirmée en " + delaiMaximal.toMillis() + " ms pour "
                            + cle + " : le message a peut-être été écrit", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PublicationImpossibleException(
                    "Publication interrompue pour " + cle, e);
        } catch (RuntimeException e) {
            // Exception levée de façon synchrone par send() (par exemple métadonnées indisponibles).
            // Le constructeur de InterruptException de kafka-clients remet déjà le statut d'interruption.
            throw new PublicationImpossibleException(
                    "Échec de la publication de l'événement de " + cle, e);
        }
    }
}
