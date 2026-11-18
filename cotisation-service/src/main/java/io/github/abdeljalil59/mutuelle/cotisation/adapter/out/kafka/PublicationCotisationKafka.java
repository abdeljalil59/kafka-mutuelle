package io.github.abdeljalil59.mutuelle.cotisation.adapter.out.kafka;

import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.PublicationCotisation;
import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.PublicationImpossibleException;
import io.github.abdeljalil59.mutuelle.cotisation.domain.Cotisation;
import io.github.abdeljalil59.mutuelle.events.CotisationEmise;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.kafka.core.KafkaTemplate;

public class PublicationCotisationKafka implements PublicationCotisation {

    private final KafkaTemplate<String, CotisationEmise> kafkaTemplate;
    private final String topic;
    private final Duration delaiMaximal;

    public PublicationCotisationKafka(KafkaTemplate<String, CotisationEmise> kafkaTemplate,
                                      String topic,
                                      Duration delaiMaximal) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
        this.delaiMaximal = delaiMaximal;
    }

    @Override
    public void publier(Cotisation cotisation, UUID idEvenement) {
        CotisationEmise evenement = versEvenement(cotisation, idEvenement);
        try {
            kafkaTemplate.send(topic, evenement.idAdherent(), evenement)
                    .get(delaiMaximal.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw new PublicationImpossibleException(
                    "Échec de la publication de la cotisation de " + cotisation.idAdherent(), e.getCause());
        } catch (TimeoutException e) {
            throw new PublicationImpossibleException(
                    "Publication non confirmée en " + delaiMaximal.toMillis() + " ms pour "
                            + cotisation.idAdherent() + " : le message a peut-être été écrit", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PublicationImpossibleException(
                    "Publication interrompue pour " + cotisation.idAdherent(), e);
        } catch (RuntimeException e) {
            // Exception levée de façon synchrone par send() (par exemple métadonnées indisponibles).
            // Le constructeur de InterruptException de kafka-clients remet déjà le statut d'interruption.
            throw new PublicationImpossibleException(
                    "Échec de la publication de la cotisation de " + cotisation.idAdherent(), e);
        }
    }

    private CotisationEmise versEvenement(Cotisation cotisation, UUID idEvenement) {
        return new CotisationEmise(
                idEvenement,
                cotisation.idAdherent(),
                cotisation.montant(),
                cotisation.emiseLe());
    }
}
