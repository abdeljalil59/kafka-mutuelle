package io.github.abdeljalil59.mutuelle.prelevement.application;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.abdeljalil59.mutuelle.prelevement.domain.Decision;
import io.github.abdeljalil59.mutuelle.prelevement.domain.DemandePrelevement;
import io.github.abdeljalil59.mutuelle.prelevement.domain.PlafondPrelevement;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

class TraiterDemandePrelevementTest {

    private static DemandePrelevement demande(String montant) {
        return new DemandePrelevement("adh1", new BigDecimal(montant));
    }

    private static TraiterDemandePrelevement avecPlafond(String plafond) {
        return new TraiterDemandePrelevement(new PlafondPrelevement(new BigDecimal(plafond)));
    }

    @Test
    void retourne_ACCEPTE_a_la_limite_du_plafond() {
        assertThat(avecPlafond("100.00").traiter(demande("100.00"))).isEqualTo(Decision.ACCEPTE);
    }

    @Test
    void retourne_REJETE_au_dela_du_plafond() {
        assertThat(avecPlafond("100.00").traiter(demande("100.01"))).isEqualTo(Decision.REJETE);
    }

    @Test
    void utilise_le_plafond_configure() {
        assertThat(avecPlafond("200.00").traiter(demande("150.00"))).isEqualTo(Decision.ACCEPTE);
        assertThat(avecPlafond("100.00").traiter(demande("150.00"))).isEqualTo(Decision.REJETE);
    }
}
