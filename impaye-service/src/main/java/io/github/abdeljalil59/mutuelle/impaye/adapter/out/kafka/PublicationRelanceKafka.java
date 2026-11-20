package io.github.abdeljalil59.mutuelle.impaye.adapter.out.kafka;

import io.github.abdeljalil59.mutuelle.impaye.application.PublicationRelanceImpossibleException;
import io.github.abdeljalil59.mutuelle.impaye.application.port.out.PublicationRelance;
import io.github.abdeljalil59.mutuelle.impaye.domain.Relance;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.kafka.core.KafkaOperations;
import tools.jackson.databind.json.JsonMapper;

// Relance en JSON, clé et valeur en chaînes : pas de schéma Avro pour ce topic dans cette version. Le message est un DTO
// propre à l'adaptateur : renommer un champ du domaine ne change pas le format publié.
public class PublicationRelanceKafka implements PublicationRelance {

    private record MessageRelance(String idAdherent, long nombreRejets, BigDecimal dernierMontant, Instant relanceLe) {
    }

    private final KafkaOperations<String, String> kafka;
    private final JsonMapper jsonMapper;
    private final String topic;
    private final Duration delaiMaximal;

    public PublicationRelanceKafka(
            KafkaOperations<String, String> kafka, JsonMapper jsonMapper, String topic, Duration delaiMaximal) {
        this.kafka = kafka;
        this.jsonMapper = jsonMapper;
        this.topic = topic;
        this.delaiMaximal = delaiMaximal;
    }

    // Clé = idAdherent : les relances d'un adhérent restent ordonnées. On attend l'accusé du broker : sans cela, un échec
    // d'envoi passerait inaperçu et le message serait considéré comme traité.
    @Override
    public void publier(Relance relance) {
        String valeur = jsonMapper.writeValueAsString(new MessageRelance(
                relance.idAdherent(), relance.nombreRejets(), relance.dernierMontant(), relance.relanceLe()));
        try {
            kafka.send(topic, relance.idAdherent(), valeur).get(delaiMaximal.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException | TimeoutException echec) {
            throw impossible(relance, echec);
        } catch (InterruptedException interruption) {
            Thread.currentThread().interrupt();
            throw impossible(relance, interruption);
        }
    }

    private PublicationRelanceImpossibleException impossible(Relance relance, Exception cause) {
        return new PublicationRelanceImpossibleException(
                "relance non confirmée par le broker pour l'adhérent " + relance.idAdherent(), cause);
    }
}
