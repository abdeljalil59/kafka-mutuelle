package io.github.abdeljalil59.mutuelle.impaye.adapter.in.kafka;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import io.github.abdeljalil59.mutuelle.events.ResultatPrelevement;
import io.github.abdeljalil59.mutuelle.events.TypeResultat;
import io.github.abdeljalil59.mutuelle.impaye.application.port.out.PersistanceImpaye;
import io.github.abdeljalil59.mutuelle.impaye.domain.Impaye;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

// Listener seul : le port de persistance est simulé (Mockito ne sert qu'aux ports). L'adaptateur JDBC a son propre test.
class ResultatPrelevementListenerTest {

    private static final UUID ID = UUID.fromString("44444444-4444-4444-4444-444444444444");
    private static final Instant DECIDE_LE = Instant.parse("2026-11-20T08:00:00.123456Z");

    private final PersistanceImpaye persistance = mock(PersistanceImpaye.class);
    private final ResultatPrelevementListener listener = new ResultatPrelevementListener(persistance);

    private static ConsumerRecord<String, ResultatPrelevement> message(TypeResultat type) {
        ResultatPrelevement resultat = ResultatPrelevement.newBuilder()
                .setIdEvenement(ID)
                .setType(type)
                .setIdAdherent("adh1")
                .setMontant(new BigDecimal("150.00"))
                .setDecideLe(DECIDE_LE)
                .build();
        return new ConsumerRecord<>("prelevements.resultats", 2, 7L, "adh1", resultat);
    }

    @Test
    void enregistre_un_impaye_pour_un_resultat_REJETE() {
        listener.ecouter(message(TypeResultat.REJETE));

        verify(persistance).enregistrer(new Impaye(ID, "adh1", new BigDecimal("150.00"), DECIDE_LE));
    }

    @Test
    void ignore_un_resultat_ACCEPTE_sans_toucher_a_la_persistance() {
        listener.ecouter(message(TypeResultat.ACCEPTE));

        verifyNoInteractions(persistance);
    }

    // L'exception sort du listener telle quelle : c'est le gestionnaire d'erreurs du container qui réessaie.
    @Test
    void laisse_remonter_l_echec_de_la_persistance() {
        IllegalStateException echec = new IllegalStateException("base indisponible");
        doThrow(echec).when(persistance).enregistrer(new Impaye(ID, "adh1", new BigDecimal("150.00"), DECIDE_LE));

        assertThatThrownBy(() -> listener.ecouter(message(TypeResultat.REJETE))).isSameAs(echec);
    }
}
