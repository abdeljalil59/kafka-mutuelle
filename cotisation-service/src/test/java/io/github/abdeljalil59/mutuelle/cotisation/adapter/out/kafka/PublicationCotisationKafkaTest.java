package io.github.abdeljalil59.mutuelle.cotisation.adapter.out.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.PublicationImpossibleException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

@ExtendWith(MockitoExtension.class)
class PublicationCotisationKafkaTest {

    private static final String TOPIC = "cotisations.emises";
    private static final String PAYLOAD =
            "{\"idEvenement\":\"33333333-3333-3333-3333-333333333333\",\"idAdherent\":\"adh1\","
                    + "\"montant\":50.00,\"emiseLe\":\"2026-11-16T10:00:00Z\"}";

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private PublicationCotisationKafka adaptateur(Duration delai) {
        return new PublicationCotisationKafka(kafkaTemplate, delai);
    }

    @Test
    void envoie_le_payload_tel_quel_sur_le_topic_avec_la_cle() {
        when(kafkaTemplate.send(eq(TOPIC), eq("adh1"), any(String.class)))
                .thenReturn(CompletableFuture.<SendResult<String, String>>completedFuture(null));

        adaptateur(Duration.ofSeconds(1)).publier(TOPIC, "adh1", PAYLOAD);

        verify(kafkaTemplate).send(TOPIC, "adh1", PAYLOAD);
    }

    @Test
    void enveloppe_l_echec_de_l_envoi_dans_une_PublicationImpossibleException() {
        CompletableFuture<SendResult<String, String>> echec = new CompletableFuture<>();
        echec.completeExceptionally(new KafkaException("broker indisponible"));
        when(kafkaTemplate.send(eq(TOPIC), eq("adh1"), any(String.class))).thenReturn(echec);

        assertThatExceptionOfType(PublicationImpossibleException.class)
                .isThrownBy(() -> adaptateur(Duration.ofSeconds(1)).publier(TOPIC, "adh1", PAYLOAD))
                .withMessageContaining("adh1")
                .withCauseInstanceOf(KafkaException.class);
    }

    @Test
    void signale_un_delai_depasse_quand_l_envoi_n_est_jamais_confirme() {
        CompletableFuture<SendResult<String, String>> jamaisComplete = new CompletableFuture<>();
        when(kafkaTemplate.send(eq(TOPIC), eq("adh1"), any(String.class))).thenReturn(jamaisComplete);

        assertThatExceptionOfType(PublicationImpossibleException.class)
                .isThrownBy(() -> adaptateur(Duration.ofMillis(50)).publier(TOPIC, "adh1", PAYLOAD))
                .withMessageContaining("50 ms")
                .withCauseInstanceOf(TimeoutException.class);
    }

    @Test
    void conserve_le_statut_d_interruption_du_thread() {
        CompletableFuture<SendResult<String, String>> jamaisComplete = new CompletableFuture<>();
        when(kafkaTemplate.send(eq(TOPIC), eq("adh1"), any(String.class))).thenReturn(jamaisComplete);

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
        when(kafkaTemplate.send(eq(TOPIC), eq("adh1"), any(String.class))).thenThrow(exceptionDeSpring);

        assertThatExceptionOfType(PublicationImpossibleException.class)
                .isThrownBy(() -> adaptateur(Duration.ofSeconds(1)).publier(TOPIC, "adh1", PAYLOAD))
                .withMessageContaining("adh1")
                .withCause(exceptionDeSpring)
                .havingRootCause()
                .isInstanceOf(org.apache.kafka.common.errors.TimeoutException.class);
    }
}
