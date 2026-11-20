package io.github.abdeljalil59.mutuelle.prelevement.adapter.in.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.abdeljalil59.mutuelle.events.avro.CotisationEmise;
import io.github.abdeljalil59.mutuelle.prelevement.AvroTestUtil;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.jdbc.core.simple.JdbcClient;

// Outils communs aux intégrations Kafka + Oracle du service : l'événement d'entrée, la lecture de la table des
// décisions et celle d'un en-tête de message. Tout ce qui dépend d'une classe (producteur, compteurs, lecteurs) reste
// dans la classe de test.
final class SupportIntegration {

    private SupportIntegration() {
    }

    static CotisationEmise evenement(UUID idEvenement, String idAdherent, String montant) {
        return AvroTestUtil.evenement(idEvenement, idAdherent, montant, Instant.parse("2026-11-20T08:00:00Z"));
    }

    static int nombreDeLignes(JdbcClient jdbcClient, UUID idEvenement) {
        return jdbcClient.sql("SELECT COUNT(*) FROM decision_prelevement WHERE id_evenement = ?")
                .param(idEvenement.toString())
                .query(Integer.class)
                .single();
    }

    static String decisionEnBase(JdbcClient jdbcClient, UUID idEvenement) {
        return jdbcClient.sql("SELECT decision FROM decision_prelevement WHERE id_evenement = ?")
                .param(idEvenement.toString())
                .query(String.class)
                .single();
    }

    static String texte(ConsumerRecord<?, ?> message, String entete) {
        Header header = message.headers().lastHeader(entete);
        assertThat(header).as("en-tête %s", entete).isNotNull();
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
