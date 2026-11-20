package io.github.abdeljalil59.mutuelle.prelevement.adapter.out.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.abdeljalil59.mutuelle.events.ResultatPrelevement;
import io.github.abdeljalil59.mutuelle.events.TypeResultat;
import io.github.abdeljalil59.mutuelle.prelevement.application.PublicationResultatImpossibleException;
import io.github.abdeljalil59.mutuelle.prelevement.domain.Decision;
import io.github.abdeljalil59.mutuelle.prelevement.domain.DecisionPrelevement;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.support.SendResult;

class PublicationResultatsKafkaTest {

    private static final String TOPIC = "prelevements.resultats";
    private static final UUID ID_EVENEMENT = UUID.fromString("77777777-7777-7777-7777-777777777777");
    private static final Instant DECIDE_LE = Instant.parse("2026-11-20T09:00:00Z");

    @SuppressWarnings("unchecked")
    private final KafkaOperations<Object, Object> kafka = mock(KafkaOperations.class);
    private final PublicationResultatsKafka publication =
            new PublicationResultatsKafka(kafka, TOPIC, Duration.ofMillis(200));

    private static DecisionPrelevement decision(Decision decision, String montant) {
        return new DecisionPrelevement(ID_EVENEMENT, "adh1", new BigDecimal(montant), decision, DECIDE_LE);
    }

    private void envoiConfirme() {
        when(kafka.send(eq(TOPIC), eq("adh1"), any())).thenReturn(CompletableFuture.<SendResult<Object, Object>>completedFuture(null));
    }

    private List<Object> valeursEnvoyees(int fois) {
        ArgumentCaptor<Object> valeur = ArgumentCaptor.forClass(Object.class);
        verify(kafka, times(fois)).send(eq(TOPIC), eq("adh1"), valeur.capture());
        return valeur.getAllValues();
    }

    @Test
    void publie_sur_le_topic_des_resultats_avec_l_idAdherent_pour_cle_et_les_valeurs_de_la_decision() {
        envoiConfirme();

        publication.publier(decision(Decision.ACCEPTE, "75.00"));

        ResultatPrelevement envoye = (ResultatPrelevement) valeursEnvoyees(1).get(0);
        assertThat(envoye.getIdEvenement()).isEqualTo(ID_EVENEMENT);
        assertThat(envoye.getType()).isEqualTo(TypeResultat.ACCEPTE);
        assertThat(envoye.getIdAdherent()).isEqualTo("adh1");
        assertThat(envoye.getMontant()).isEqualTo(new BigDecimal("75.00"));
        assertThat(envoye.getDecideLe()).isEqualTo(DECIDE_LE);
    }

    @Test
    void publie_le_type_REJETE_pour_une_decision_rejetee() {
        envoiConfirme();

        publication.publier(decision(Decision.REJETE, "150.00"));

        assertThat(((ResultatPrelevement) valeursEnvoyees(1).get(0)).getType()).isEqualTo(TypeResultat.REJETE);
    }

    // Le consommateur aval dédoublonne sur idEvenement : republier la même décision ne doit pas en changer.
    @Test
    void republier_la_meme_decision_donne_le_meme_idEvenement() {
        envoiConfirme();

        publication.publier(decision(Decision.ACCEPTE, "75.00"));
        publication.publier(decision(Decision.ACCEPTE, "75.00"));

        assertThat(valeursEnvoyees(2)).extracting(valeur -> ((ResultatPrelevement) valeur).getIdEvenement())
                .containsExactly(ID_EVENEMENT, ID_EVENEMENT);
    }

    @Test
    void signale_un_echec_d_envoi_confirme_par_le_broker_avec_sa_cause() {
        IllegalStateException cause = new IllegalStateException("broker injoignable");
        when(kafka.send(eq(TOPIC), eq("adh1"), any()))
                .thenReturn(CompletableFuture.<SendResult<Object, Object>>failedFuture(cause));

        assertThatThrownBy(() -> publication.publier(decision(Decision.ACCEPTE, "75.00")))
                .isInstanceOf(PublicationResultatImpossibleException.class)
                .hasCause(cause)
                .hasMessageContaining(ID_EVENEMENT.toString());
    }

    @Test
    void signale_une_publication_non_confirmee_dans_le_delai() {
        when(kafka.send(eq(TOPIC), eq("adh1"), any())).thenReturn(new CompletableFuture<>());

        assertThatThrownBy(() -> publication.publier(decision(Decision.ACCEPTE, "75.00")))
                .isInstanceOf(PublicationResultatImpossibleException.class)
                .hasMessageContaining("peut-être été écrit");
    }

    @Test
    void signale_une_exception_levee_par_send_lui_meme() {
        IllegalStateException cause = new IllegalStateException("métadonnées indisponibles");
        when(kafka.send(eq(TOPIC), eq("adh1"), any())).thenThrow(cause);

        assertThatThrownBy(() -> publication.publier(decision(Decision.ACCEPTE, "75.00")))
                .isInstanceOf(PublicationResultatImpossibleException.class)
                .hasCause(cause);
    }
}
