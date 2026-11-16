package io.github.abdeljalil59.mutuelle.cotisation.adapter.out.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.PublicationImpossibleException;
import io.github.abdeljalil59.mutuelle.cotisation.domain.Cotisation;
import io.github.abdeljalil59.mutuelle.events.CotisationEmise;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

@ExtendWith(MockitoExtension.class)
class PublicationCotisationKafkaTest {

    private static final String TOPIC = "cotisations.emises";
    private static final UUID ID_EVENEMENT = UUID.fromString("33333333-3333-3333-3333-333333333333");
    private static final Instant EMISE_LE = Instant.parse("2026-11-16T10:00:00Z");
    private static final Cotisation COTISATION = new Cotisation("adh1", new BigDecimal("50.00"), EMISE_LE);

    @Mock
    private KafkaTemplate<String, CotisationEmise> kafkaTemplate;

    private PublicationCotisationKafka adaptateur(Duration delai) {
        return new PublicationCotisationKafka(kafkaTemplate, TOPIC, delai, () -> ID_EVENEMENT);
    }

    @Test
    void envoie_l_evenement_sur_le_topic_avec_idAdherent_comme_cle() {
        when(kafkaTemplate.send(eq(TOPIC), eq("adh1"), any(CotisationEmise.class)))
                .thenReturn(CompletableFuture.<SendResult<String, CotisationEmise>>completedFuture(null));

        adaptateur(Duration.ofSeconds(1)).publier(COTISATION);

        ArgumentCaptor<CotisationEmise> evenement = ArgumentCaptor.forClass(CotisationEmise.class);
        verify(kafkaTemplate).send(eq(TOPIC), eq("adh1"), evenement.capture());
        assertThat(evenement.getValue()).isEqualTo(
                new CotisationEmise(ID_EVENEMENT, "adh1", new BigDecimal("50.00"), EMISE_LE));
    }

    @Test
    void enveloppe_l_echec_de_l_envoi_dans_une_PublicationImpossibleException() {
        CompletableFuture<SendResult<String, CotisationEmise>> echec = new CompletableFuture<>();
        echec.completeExceptionally(new KafkaException("broker indisponible"));
        when(kafkaTemplate.send(eq(TOPIC), eq("adh1"), any(CotisationEmise.class))).thenReturn(echec);

        assertThatExceptionOfType(PublicationImpossibleException.class)
                .isThrownBy(() -> adaptateur(Duration.ofSeconds(1)).publier(COTISATION))
                .withMessageContaining("adh1")
                .withCauseInstanceOf(KafkaException.class);
    }

    @Test
    void signale_un_delai_depasse_quand_l_envoi_n_est_jamais_confirme() {
        CompletableFuture<SendResult<String, CotisationEmise>> jamaisComplete = new CompletableFuture<>();
        when(kafkaTemplate.send(eq(TOPIC), eq("adh1"), any(CotisationEmise.class))).thenReturn(jamaisComplete);

        assertThatExceptionOfType(PublicationImpossibleException.class)
                .isThrownBy(() -> adaptateur(Duration.ofMillis(50)).publier(COTISATION))
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
                    .isThrownBy(() -> adaptateur(Duration.ofSeconds(1)).publier(COTISATION))
                    .withCauseInstanceOf(InterruptedException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void enveloppe_une_exception_levee_de_facon_synchrone_par_send() {
        when(kafkaTemplate.send(eq(TOPIC), eq("adh1"), any(CotisationEmise.class)))
                .thenThrow(new KafkaException("métadonnées indisponibles"));

        assertThatExceptionOfType(PublicationImpossibleException.class)
                .isThrownBy(() -> adaptateur(Duration.ofSeconds(1)).publier(COTISATION))
                .withMessageContaining("adh1")
                .withCauseInstanceOf(KafkaException.class);
    }
}
