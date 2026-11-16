package io.github.abdeljalil59.mutuelle.events;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CotisationEmiseTest {

    private static final UUID ID_EVENEMENT = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final Instant EMISE_LE = Instant.parse("2026-11-13T10:00:00Z");

    @Test
    void cree_un_evenement_valide() {
        CotisationEmise evenement = new CotisationEmise(ID_EVENEMENT, "adh1", new BigDecimal("50.00"), EMISE_LE);

        assertThat(evenement.idEvenement()).isEqualTo(ID_EVENEMENT);
        assertThat(evenement.idAdherent()).isEqualTo("adh1");
        assertThat(evenement.montant()).isEqualByComparingTo("50.00");
        assertThat(evenement.emiseLe()).isEqualTo(EMISE_LE);
    }

    @Test
    void refuse_un_idEvenement_null() {
        assertThatNullPointerException()
                .isThrownBy(() -> new CotisationEmise(null, "adh1", BigDecimal.TEN, EMISE_LE))
                .withMessageContaining("idEvenement");
    }

    @Test
    void refuse_un_idAdherent_null() {
        assertThatNullPointerException()
                .isThrownBy(() -> new CotisationEmise(ID_EVENEMENT, null, BigDecimal.TEN, EMISE_LE))
                .withMessageContaining("idAdherent");
    }

    @Test
    void refuse_un_montant_null() {
        assertThatNullPointerException()
                .isThrownBy(() -> new CotisationEmise(ID_EVENEMENT, "adh1", null, EMISE_LE))
                .withMessageContaining("montant");
    }

    @Test
    void refuse_un_emiseLe_null() {
        assertThatNullPointerException()
                .isThrownBy(() -> new CotisationEmise(ID_EVENEMENT, "adh1", BigDecimal.TEN, null))
                .withMessageContaining("emiseLe");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "   "})
    void refuse_un_idAdherent_vide_ou_blanc(String idAdherent) {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new CotisationEmise(ID_EVENEMENT, idAdherent, BigDecimal.TEN, EMISE_LE))
                .withMessageContaining("idAdherent");
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "0.00", "-0.01", "-50"})
    void refuse_un_montant_nul_ou_negatif(String montant) {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new CotisationEmise(ID_EVENEMENT, "adh1", new BigDecimal(montant), EMISE_LE))
                .withMessageContaining("montant");
    }
}
