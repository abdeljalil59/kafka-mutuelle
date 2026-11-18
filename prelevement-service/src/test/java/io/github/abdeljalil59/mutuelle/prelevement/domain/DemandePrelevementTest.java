package io.github.abdeljalil59.mutuelle.prelevement.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.math.BigDecimal;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class DemandePrelevementTest {

    private static final UUID ID_EVENEMENT = UUID.fromString("aaaaaaaa-0000-0000-0000-000000000001");

    @Test
    void cree_une_demande_valide() {
        DemandePrelevement demande = new DemandePrelevement(ID_EVENEMENT, "adh1", new BigDecimal("50.00"));

        assertThat(demande.idEvenement()).isEqualTo(ID_EVENEMENT);
        assertThat(demande.idAdherent()).isEqualTo("adh1");
        assertThat(demande.montant()).isEqualByComparingTo("50.00");
    }

    @Test
    void refuse_un_idEvenement_null() {
        assertThatExceptionOfType(PrelevementInvalideException.class)
                .isThrownBy(() -> new DemandePrelevement(null, "adh1", BigDecimal.TEN))
                .withMessageContaining("idEvenement");
    }

    @Test
    void refuse_un_idAdherent_null() {
        assertThatExceptionOfType(PrelevementInvalideException.class)
                .isThrownBy(() -> new DemandePrelevement(ID_EVENEMENT, null, BigDecimal.TEN))
                .withMessageContaining("idAdherent");
    }

    @Test
    void refuse_un_montant_null() {
        assertThatExceptionOfType(PrelevementInvalideException.class)
                .isThrownBy(() -> new DemandePrelevement(ID_EVENEMENT, "adh1", null))
                .withMessageContaining("montant");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "   "})
    void refuse_un_idAdherent_vide_ou_blanc(String idAdherent) {
        assertThatExceptionOfType(PrelevementInvalideException.class)
                .isThrownBy(() -> new DemandePrelevement(ID_EVENEMENT, idAdherent, BigDecimal.TEN))
                .withMessageContaining("idAdherent");
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "0.00", "-0.01", "-50"})
    void refuse_un_montant_nul_ou_negatif(String montant) {
        assertThatExceptionOfType(PrelevementInvalideException.class)
                .isThrownBy(() -> new DemandePrelevement(ID_EVENEMENT, "adh1", new BigDecimal(montant)))
                .withMessageContaining("montant");
    }
}
