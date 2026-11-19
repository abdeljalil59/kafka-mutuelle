package io.github.abdeljalil59.mutuelle.cotisation.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.math.BigDecimal;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CotisationTest {

    private static final Instant EMISE_LE = Instant.parse("2026-11-16T10:00:00Z");

    @Test
    void cree_une_cotisation_valide() {
        Cotisation cotisation = new Cotisation("adh1", new BigDecimal("50.00"), EMISE_LE);

        assertThat(cotisation.idAdherent()).isEqualTo("adh1");
        assertThat(cotisation.montant()).isEqualByComparingTo("50.00");
        assertThat(cotisation.emiseLe()).isEqualTo(EMISE_LE);
    }

    @Test
    void refuse_un_idAdherent_null() {
        assertThatExceptionOfType(CotisationInvalideException.class)
                .isThrownBy(() -> new Cotisation(null, BigDecimal.TEN, EMISE_LE))
                .withMessageContaining("idAdherent");
    }

    @Test
    void refuse_un_montant_null() {
        assertThatExceptionOfType(CotisationInvalideException.class)
                .isThrownBy(() -> new Cotisation("adh1", null, EMISE_LE))
                .withMessageContaining("montant");
    }

    @Test
    void refuse_un_emiseLe_null() {
        assertThatExceptionOfType(CotisationInvalideException.class)
                .isThrownBy(() -> new Cotisation("adh1", BigDecimal.TEN, null))
                .withMessageContaining("emiseLe");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "   "})
    void refuse_un_idAdherent_vide_ou_blanc(String idAdherent) {
        assertThatExceptionOfType(CotisationInvalideException.class)
                .isThrownBy(() -> new Cotisation(idAdherent, BigDecimal.TEN, EMISE_LE))
                .withMessageContaining("idAdherent");
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "0.00", "-0.01", "-50"})
    void refuse_un_montant_nul_ou_negatif(String montant) {
        assertThatExceptionOfType(CotisationInvalideException.class)
                .isThrownBy(() -> new Cotisation("adh1", new BigDecimal(montant), EMISE_LE))
                .withMessageContaining("montant");
    }

    // La règle porte sur l'échelle, pas sur la valeur : 50.000 est refusé comme 50.001.
    @ParameterizedTest
    @ValueSource(strings = {"50.001", "10.123", "50.000"})
    void refuse_un_montant_avec_plus_de_deux_decimales(String montant) {
        assertThatExceptionOfType(CotisationInvalideException.class)
                .isThrownBy(() -> new Cotisation("adh1", new BigDecimal(montant), EMISE_LE))
                .withMessageContaining("2 décimales");
    }

    @ParameterizedTest
    @ValueSource(strings = {"50", "50.5", "50.00", "0.01", "5E+1"})
    void accepte_un_montant_avec_au_plus_deux_decimales(String montant) {
        Cotisation cotisation = new Cotisation("adh1", new BigDecimal(montant), EMISE_LE);

        assertThat(cotisation.montant()).isEqualByComparingTo(montant);
    }
}
