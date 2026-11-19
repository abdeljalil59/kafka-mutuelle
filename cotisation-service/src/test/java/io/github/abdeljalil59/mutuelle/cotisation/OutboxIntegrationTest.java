package io.github.abdeljalil59.mutuelle.cotisation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.abdeljalil59.mutuelle.cotisation.application.EmettreCotisation;
import io.github.abdeljalil59.mutuelle.cotisation.domain.Cotisation;
import io.github.abdeljalil59.mutuelle.events.avro.CotisationEmise;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

// Flux complet avec le VRAI relais planifié (@Scheduled, toutes les 200 ms), sans aucun espion :
// cas d'usage -> table outbox (H2 en mémoire, JSON) -> relais -> conversion Avro -> Kafka embarqué -> consommateur
// de test. Les scénarios qui stubbent l'outbox sont dans OutboxTransactionIntegrationTest (relais coupé),
// car un stub posé pendant que le relais interroge le même espion serait une course.
// Registre de schémas factice (mock://) partagé entre le service et le consommateur de test.
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.properties.schema.registry.url=" + OutboxIntegrationTest.REGISTRE,
        "mutuelle.cotisation.outbox.relais-actif=true",
        "mutuelle.cotisation.outbox.delai-relais-ms=200"})
@EmbeddedKafka
@DirtiesContext
class OutboxIntegrationTest {

    static final String REGISTRE = "mock://cotisation-outbox";
    private static final Logger LOGGER = LoggerFactory.getLogger(OutboxIntegrationTest.class);
    private static final String TOPIC = "cotisations.emises";

    @Autowired
    private EmettreCotisation emettreCotisation;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Test
    void flux_complet_de_la_cotisation_au_message_avro_sur_le_topic() {
        try (Consumer<String, CotisationEmise> consumer =
                     LectureTopic.consommateurEnFinDeLog(embeddedKafka, TOPIC, REGISTRE)) {
            Cotisation cotisation = emettreCotisation.emettre("adh-flux", new BigDecimal("50.00"));

            // La ligne est marquée envoyée APRÈS l'accusé du broker : une fois envoye_le renseigné,
            // le message est sur le topic.
            await().atMost(Duration.ofSeconds(15)).until(() -> estEnvoye("adh-flux"));

            ConsumerRecord<String, CotisationEmise> message = LectureTopic.lireLaCle(consumer, "adh-flux", 1).get(0);
            LOGGER.info("PREUVE flux complet : cle={} partition={} offset={} valeur={}",
                    message.key(), message.partition(), message.offset(), message.value());
            CotisationEmise evenement = message.value();
            // L'idEvenement du message est celui de la ligne d'outbox, généré par le cas d'usage.
            assertThat(evenement.getIdEvenement()).isEqualTo(UUID.fromString(idEvenementEnBase("adh-flux")));
            assertThat(evenement.getIdAdherent()).isEqualTo("adh-flux");
            // Échelle 2 conservée par le contrat Avro (decimal(19,2)).
            assertThat(evenement.getMontant()).isEqualTo(new BigDecimal("50.00"));
            // timestamp-micros : l'instant d'émission est tronqué à la microseconde par l'encodage.
            assertThat(evenement.getEmiseLe()).isEqualTo(cotisation.emiseLe().truncatedTo(ChronoUnit.MICROS));
        }
    }

    private boolean estEnvoye(String cle) {
        return jdbcClient.sql("SELECT COUNT(*) FROM outbox WHERE cle = ? AND envoye_le IS NOT NULL")
                .param(cle)
                .query(Integer.class)
                .single() > 0;
    }

    private String idEvenementEnBase(String cle) {
        return jdbcClient.sql("SELECT id_evenement FROM outbox WHERE cle = ?")
                .param(cle)
                .query(String.class)
                .single();
    }
}
