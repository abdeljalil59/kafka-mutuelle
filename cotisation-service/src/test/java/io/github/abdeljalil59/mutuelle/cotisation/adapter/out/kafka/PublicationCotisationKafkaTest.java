package io.github.abdeljalil59.mutuelle.cotisation.adapter.out.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.PublicationImpossibleException;
import io.github.abdeljalil59.mutuelle.events.avro.CotisationEmise;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import tools.jackson.databind.json.JsonMapper;

@ExtendWith(MockitoExtension.class)
class PublicationCotisationKafkaTest {

    private static final String TOPIC = "cotisations.emises";
    private static final UUID ID_EVENEMENT = UUID.fromString("33333333-3333-3333-3333-333333333333");
    // Payload JSON tel que l'outbox le stocke.
    private static final String PAYLOAD =
            "{\"idEvenement\":\"33333333-3333-3333-3333-333333333333\",\"idAdherent\":\"adh1\","
                    + "\"montant\":50.00,\"emiseLe\":\"2026-11-16T10:00:00Z\"}";
    private static final CotisationEmise EVENEMENT_ATTENDU = CotisationEmise.newBuilder()
            .setIdEvenement(ID_EVENEMENT)
            .setIdAdherent("adh1")
            .setMontant(new BigDecimal("50.00"))
            .setEmiseLe(Instant.parse("2026-11-16T10:00:00Z"))
            .build();

    @Mock
    private KafkaTemplate<String, CotisationEmise> kafkaTemplate;

    private PublicationCotisationKafka adaptateur(Duration delai) {
        return new PublicationCotisationKafka(kafkaTemplate, JsonMapper.builder().build(), delai);
    }

    @Test
    void convertit_le_payload_en_avro_et_l_envoie_sur_le_topic_avec_la_cle() {
        when(kafkaTemplate.send(eq(TOPIC), eq("adh1"), any(CotisationEmise.class)))
                .thenReturn(CompletableFuture.<SendResult<String, CotisationEmise>>completedFuture(null));

        adaptateur(Duration.ofSeconds(1)).publier(TOPIC, "adh1", PAYLOAD);

        verify(kafkaTemplate).send(TOPIC, "adh1", EVENEMENT_ATTENDU);
    }

    @Test
    void ne_publie_rien_si_le_payload_est_inconvertible() {
        // Une donnée inconvertible n'est pas une panne transitoire : ce n'est pas une PublicationImpossibleException.
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> adaptateur(Duration.ofSeconds(1)).publier(TOPIC, "adh1", "ceci-n-est-pas-du-json"))
                .withMessageContaining("adh1");

        verifyNoInteractions(kafkaTemplate);
    }

    @Test
    void enveloppe_l_echec_de_l_envoi_dans_une_PublicationImpossibleException() {
        CompletableFuture<SendResult<String, CotisationEmise>> echec = new CompletableFuture<>();
        echec.completeExceptionally(new KafkaException("broker indisponible"));
        when(kafkaTemplate.send(eq(TOPIC), eq("adh1"), any(CotisationEmise.class))).thenReturn(echec);

        assertThatExceptionOfType(PublicationImpossibleException.class)
                .isThrownBy(() -> adaptateur(Duration.ofSeconds(1)).publier(TOPIC, "adh1", PAYLOAD))
                .withMessageContaining("adh1")
                .withCauseInstanceOf(KafkaException.class);
    }

    @Test
    void signale_un_delai_depasse_quand_l_envoi_n_est_jamais_confirme() {
        CompletableFuture<SendResult<String, CotisationEmise>> jamaisComplete = new CompletableFuture<>();
        when(kafkaTemplate.send(eq(TOPIC), eq("adh1"), any(CotisationEmise.class))).thenReturn(jamaisComplete);

        assertThatExceptionOfType(PublicationImpossibleException.class)
                .isThrownBy(() -> adaptateur(Duration.ofMillis(50)).publier(TOPIC, "adh1", PAYLOAD))
                .withMessageContaining("50 ms")
                .withCauseInstanceOf(TimeoutException.class);
    }

    @Test
    void conserve_le_statut_d_interruption_du_thread() {
        CompletableFuture<SendResult<String, CotisationEmise>> jamaisComplete = new CompletableFuture<>();
        when(kafkaTemplate.send(eq(TOPIC), eq("adh1"), any(CotisationEmise.class))).thenReturn(jamaisComplete);

        Thread.currentThread().interrupt();
        try {
            assertThatExceptionOfType(PublicationImpossibleException.class)
                    .isThrownBy(() -> adaptateur(Duration.ofSeconds(1)).publier(TOPIC, "adh1", PAYLOAD))
                    .withCauseInstanceOf(InterruptedException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void enveloppe_une_exception_levee_de_facon_synchrone_par_send() {
        // Exception constatée par le test sans broker (étape 3c) : KafkaException de Spring
        // dont la cause est le TimeoutException de Kafka.
        org.springframework.kafka.KafkaException exceptionDeSpring = new org.springframework.kafka.KafkaException(
                "Send failed",
                new org.apache.kafka.common.errors.TimeoutException("métadonnées indisponibles"));
        when(kafkaTemplate.send(eq(TOPIC), eq("adh1"), any(CotisationEmise.class))).thenThrow(exceptionDeSpring);

        assertThatExceptionOfType(PublicationImpossibleException.class)
                .isThrownBy(() -> adaptateur(Duration.ofSeconds(1)).publier(TOPIC, "adh1", PAYLOAD))
                .withMessageContaining("adh1")
                .withCause(exceptionDeSpring)
                .havingRootCause()
                .isInstanceOf(org.apache.kafka.common.errors.TimeoutException.class);
    }
}
