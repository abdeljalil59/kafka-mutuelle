package io.github.abdeljalil59.mutuelle.prelevement.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class PlafondPrelevementTest {

    private static final PlafondPrelevement PLAFOND = new PlafondPrelevement(new BigDecimal("100.00"));

    private static DemandePrelevement demande(String montant) {
        return new DemandePrelevement("adh1", new BigDecimal(montant));
    }

    @Test
    void accepte_un_montant_inferieur_au_plafond() {
        assertThat(PLAFOND.decider(demande("50.00"))).isEqualTo(Decision.ACCEPTE);
    }

    @Test
    void accepte_un_montant_egal_au_plafond() {
        assertThat(PLAFOND.decider(demande("100.00"))).isEqualTo(Decision.ACCEPTE);
    }

    @Test
    void rejette_un_montant_strictement_superieur_au_plafond() {
        assertThat(PLAFOND.decider(demande("100.01"))).isEqualTo(Decision.REJETE);
    }

    @ParameterizedTest
    @ValueSource(strings = {"100", "100.0", "100.000"})
    void ignore_l_echelle_du_montant_egal_au_plafond(String montant) {
        assertThat(PLAFOND.decider(demande(montant))).isEqualTo(Decision.ACCEPTE);
    }

    @Test
    void rejette_un_montant_a_peine_superieur_au_plafond() {
        assertThat(PLAFOND.decider(demande("100.001"))).isEqualTo(Decision.REJETE);
    }

    @Test
    void refuse_un_plafond_null() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new PlafondPrelevement(null))
                .withMessageContaining("plafond");
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "0.00", "-1"})
    void refuse_un_plafond_non_strictement_positif(String plafond) {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new PlafondPrelevement(new BigDecimal(plafond)))
                .withMessageContaining("plafond");
    }
}
