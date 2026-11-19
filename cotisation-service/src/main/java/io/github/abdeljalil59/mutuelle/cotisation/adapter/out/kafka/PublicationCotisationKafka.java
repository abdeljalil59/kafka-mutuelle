package io.github.abdeljalil59.mutuelle.cotisation.adapter.out.kafka;

import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.PublicationCotisation;
import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.PublicationImpossibleException;
import io.github.abdeljalil59.mutuelle.events.avro.CotisationEmise;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.json.JsonMapper;

// Le port reste en JSON (format de l'outbox) ; la conversion en objet Avro se fait ici, juste avant l'envoi.
public class PublicationCotisationKafka implements PublicationCotisation {

    private final KafkaTemplate<String, CotisationEmise> kafkaTemplate;
    private final JsonMapper jsonMapper;
    private final Duration delaiMaximal;

    public PublicationCotisationKafka(
            KafkaTemplate<String, CotisationEmise> kafkaTemplate, JsonMapper jsonMapper, Duration delaiMaximal) {
        this.kafkaTemplate = kafkaTemplate;
        this.jsonMapper = jsonMapper;
        this.delaiMaximal = delaiMaximal;
    }

    @Override
    public void publier(String topic, String cle, String payload) {
        // Hors du try : une donnée inconvertible n'est pas une panne transitoire (pas de PublicationImpossibleException).
        CotisationEmise evenement = ConvertisseurAvro.depuisJson(jsonMapper, payload, cle);
        try {
            kafkaTemplate.send(topic, cle, evenement)
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
