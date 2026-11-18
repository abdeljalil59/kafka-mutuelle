package io.github.abdeljalil59.mutuelle.cotisation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.github.abdeljalil59.mutuelle.cotisation.application.EmettreCotisation;
import io.github.abdeljalil59.mutuelle.cotisation.application.RelayerOutbox;
import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.Outbox;
import io.github.abdeljalil59.mutuelle.cotisation.domain.Cotisation;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.transaction.support.TransactionSynchronizationManager;

// Relais PLANIFIÉ désactivé : le test appelle lui-même relayerOutbox.relayer(), sans attente temporelle,
// et peut donc stubber l'espion de l'outbox sans qu'un autre thread n'appelle l'espion en parallèle.
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "mutuelle.cotisation.outbox.relais-actif=false"})
@EmbeddedKafka
@DirtiesContext
class OutboxTransactionIntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(OutboxTransactionIntegrationTest.class);
    private static final String TOPIC = "cotisations.emises";
    private static final UUID ID_EVENEMENT_1 = UUID.fromString("aaaaaaaa-0000-0000-0000-000000000001");
    private static final UUID ID_EVENEMENT_2 = UUID.fromString("aaaaaaaa-0000-0000-0000-000000000002");
    private static final Instant T1 = Instant.parse("2026-11-16T10:00:00Z");

    @Autowired
    private EmettreCotisation emettreCotisation;

    @Autowired
    private RelayerOutbox relayerOutbox;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @MockitoSpyBean
    private Outbox outbox;

    // Les tests partagent le même contexte (donc la même base) : chacun repart d'une outbox vide.
    @BeforeEach
    void vide_l_outbox() {
        jdbcClient.sql("DELETE FROM outbox").update();
    }

    @Test
    void le_cas_d_usage_ecrit_l_outbox_dans_une_transaction() {
        List<Boolean> transactionActive = new ArrayList<>();
        doAnswer(appel -> {
            transactionActive.add(TransactionSynchronizationManager.isActualTransactionActive());
            return appel.callRealMethod();
        }).when(outbox).enregistrer(any(), any());

        // 1. Par le cas d'usage (annoté @Transactional).
        emettreCotisation.emettre("adh-transaction", new BigDecimal("10.00"));
        // 2. Directement par le port, sans passer par le cas d'usage : aucune transaction n'est ouverte.
        outbox.enregistrer(new Cotisation("adh-sans-transaction", BigDecimal.TEN, T1), ID_EVENEMENT_1);

        LOGGER.info("PREUVE transaction active pendant enregistrer : {} (cas d'usage, appel direct)",
                transactionActive);
        assertThat(transactionActive).containsExactly(true, false);
        // Les deux écritures sont bien validées (commit) : la transaction du cas d'usage ne reste pas ouverte.
        assertThat(nombreDeLignes()).isEqualTo(2);
    }

    @Test
    void republie_l_evenement_quand_le_marquage_echoue_une_fois() {
        String cle = "adh-doublon";
        doThrow(new IllegalStateException("marquage impossible")).doCallRealMethod()
                .when(outbox).marquerEnvoye(eq(ID_EVENEMENT_2), any());

        try (Consumer<String, String> consumer = LectureTopic.consommateurEnFinDeLog(embeddedKafka, TOPIC)) {
            outbox.enregistrer(new Cotisation(cle, new BigDecimal("50.00"), T1), ID_EVENEMENT_2);

            // Premier passage : l'événement est publié (le broker a accusé réception), puis le marquage échoue.
            assertThatThrownBy(() -> relayerOutbox.relayer())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("marquage impossible");
            assertThat(estEnvoye(cle)).isFalse();

            // Second passage : l'événement est toujours "non envoyé", il est republié puis marqué.
            assertThat(relayerOutbox.relayer()).isEqualTo(1);
            assertThat(estEnvoye(cle)).isTrue();

            List<ConsumerRecord<String, String>> messages = LectureTopic.lireLaCle(consumer, cle, 2);
            messages.forEach(message -> LOGGER.info("PREUVE doublon : cle={} partition={} offset={} valeur={}",
                    message.key(), message.partition(), message.offset(), message.value()));
            // Même clé donc même partition ; offsets différents ; même idEvenement et même payload :
            // c'est ce doublon que l'idempotence du consommateur (prelevement-service) absorbe.
            assertThat(messages.get(1).partition()).isEqualTo(messages.get(0).partition());
            assertThat(messages.get(1).offset()).isGreaterThan(messages.get(0).offset());
            assertThat(messages.get(1).value()).isEqualTo(messages.get(0).value());
            assertThat(messages.get(0).value()).contains(ID_EVENEMENT_2.toString());
            verify(outbox, times(2)).marquerEnvoye(eq(ID_EVENEMENT_2), any());
        }
    }

    private int nombreDeLignes() {
        return jdbcClient.sql("SELECT COUNT(*) FROM outbox").query(Integer.class).single();
    }

    private boolean estEnvoye(String cle) {
        return jdbcClient.sql("SELECT COUNT(*) FROM outbox WHERE cle = ? AND envoye_le IS NOT NULL")
                .param(cle)
                .query(Integer.class)
                .single() > 0;
    }
}
