package io.github.abdeljalil59.mutuelle.prelevement.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class DemandePrelevementTest {

    @Test
    void cree_une_demande_valide() {
        DemandePrelevement demande = new DemandePrelevement("adh1", new BigDecimal("50.00"));

        assertThat(demande.idAdherent()).isEqualTo("adh1");
        assertThat(demande.montant()).isEqualByComparingTo("50.00");
    }

    @Test
    void refuse_un_idAdherent_null() {
        assertThatExceptionOfType(PrelevementInvalideException.class)
                .isThrownBy(() -> new DemandePrelevement(null, BigDecimal.TEN))
                .withMessageContaining("idAdherent");
    }

    @Test
    void refuse_un_montant_null() {
        assertThatExceptionOfType(PrelevementInvalideException.class)
                .isThrownBy(() -> new DemandePrelevement("adh1", null))
                .withMessageContaining("montant");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "   "})
    void refuse_un_idAdherent_vide_ou_blanc(String idAdherent) {
        assertThatExceptionOfType(PrelevementInvalideException.class)
                .isThrownBy(() -> new DemandePrelevement(idAdherent, BigDecimal.TEN))
                .withMessageContaining("idAdherent");
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "0.00", "-0.01", "-50"})
    void refuse_un_montant_nul_ou_negatif(String montant) {
        assertThatExceptionOfType(PrelevementInvalideException.class)
                .isThrownBy(() -> new DemandePrelevement("adh1", new BigDecimal(montant)))
                .withMessageContaining("montant");
    }
}
