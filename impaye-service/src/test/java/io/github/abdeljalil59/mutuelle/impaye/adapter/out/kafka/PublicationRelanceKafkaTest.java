package io.github.abdeljalil59.mutuelle.impaye.adapter.out.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.abdeljalil59.mutuelle.impaye.application.PublicationRelanceImpossibleException;
import io.github.abdeljalil59.mutuelle.impaye.domain.Relance;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.support.SendResult;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

// L'adaptateur contre un template simulé : le topic, la clé, le contenu JSON et la conversion des échecs. L'envoi réel
// est éprouvé par ImpayeIntegrationTest.
class PublicationRelanceKafkaTest {

    private static final String TOPIC = "impayes.relances";
    private static final Relance RELANCE = new Relance(
            "adh1", 3, new BigDecimal("150.00"), Instant.parse("2026-11-20T10:00:00.123456Z"));

    @SuppressWarnings("unchecked")
    private final KafkaOperations<String, String> kafka = mock(KafkaOperations.class);
    private final JsonMapper jsonMapper = JsonMapper.builder().build();
    private final PublicationRelanceKafka publication =
            new PublicationRelanceKafka(kafka, jsonMapper, TOPIC, Duration.ofMillis(200));

    @Test
    void publie_la_relance_en_json_sur_le_topic_avec_l_adherent_pour_cle() {
        when(kafka.send(eq(TOPIC), eq("adh1"), any())).thenReturn(CompletableFuture.<SendResult<String, String>>completedFuture(null));

        publication.publier(RELANCE);

        ArgumentCaptor<String> valeur = ArgumentCaptor.forClass(String.class);
        verify(kafka).send(eq(TOPIC), eq("adh1"), valeur.capture());
        JsonNode json = jsonMapper.readTree(valeur.getValue());
        assertThat(json.path("idAdherent").asString()).isEqualTo("adh1");
        assertThat(json.path("nombreRejets").asInt()).isEqualTo(3);
        assertThat(json.path("dernierMontant").decimalValue()).isEqualByComparingTo("150.00");
        // Instant en ISO-8601 (et non en nombre) : lisible, et la précision en microsecondes est conservée.
        assertThat(Instant.parse(json.path("relanceLe").asString())).isEqualTo(RELANCE.relanceLe());
    }

    @Test
    void convertit_un_echec_d_envoi_en_publication_impossible() {
        IllegalStateException cause = new IllegalStateException("broker injoignable");
        when(kafka.send(eq(TOPIC), eq("adh1"), any()))
                .thenReturn(CompletableFuture.<SendResult<String, String>>failedFuture(cause));

        assertThatThrownBy(() -> publication.publier(RELANCE))
                .isInstanceOf(PublicationRelanceImpossibleException.class)
                .hasMessageContaining("adh1")
                .hasRootCause(cause);
    }

    @Test
    void convertit_l_absence_d_accuse_dans_le_delai_en_publication_impossible() {
        when(kafka.send(eq(TOPIC), eq("adh1"), any())).thenReturn(new CompletableFuture<SendResult<String, String>>());

        assertThatThrownBy(() -> publication.publier(RELANCE))
                .isInstanceOf(PublicationRelanceImpossibleException.class)
                .hasCauseInstanceOf(TimeoutException.class);
    }
}
