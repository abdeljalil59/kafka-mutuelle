package io.github.abdeljalil59.mutuelle.cotisation.adapter.out.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.abdeljalil59.mutuelle.events.avro.CotisationEmise;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

class ConvertisseurAvroTest {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final UUID ID_EVENEMENT = UUID.fromString("33333333-3333-3333-3333-333333333333");

    private static String payload(String montant) {
        return "{\"idEvenement\":\"" + ID_EVENEMENT + "\",\"idAdherent\":\"adh1\","
                + "\"montant\":" + montant + ",\"emiseLe\":\"2026-11-16T10:00:00Z\"}";
    }

    @Test
    void convertit_le_json_de_l_outbox_en_objet_avro() {
        CotisationEmise evenement = ConvertisseurAvro.depuisJson(JSON, payload("50.00"), "adh1");

        assertThat(evenement.getIdEvenement()).isEqualTo(ID_EVENEMENT);
        assertThat(evenement.getIdAdherent()).isEqualTo("adh1");
        assertThat(evenement.getMontant()).isEqualTo(new BigDecimal("50.00"));
        assertThat(evenement.getEmiseLe()).isEqualTo(Instant.parse("2026-11-16T10:00:00Z"));
    }

    @Test
    void normalise_l_echelle_zero_a_deux_decimales() {
        assertThat(ConvertisseurAvro.depuisJson(JSON, payload("50"), "adh1").getMontant())
                .isEqualTo(new BigDecimal("50.00"));
    }

    @Test
    void accepte_des_zeros_non_significatifs_au_dela_de_la_deuxieme_decimale() {
        // Aucun arrondi n'est nécessaire : 50.000 vaut 50.00. (La règle de domaine, plus stricte, les refuse déjà.)
        assertThat(ConvertisseurAvro.depuisJson(JSON, payload("50.000"), "adh1").getMontant())
                .isEqualTo(new BigDecimal("50.00"));
    }

    @Test
    void refuse_un_montant_dont_la_conversion_imposerait_un_arrondi() {
        assertThatThrownBy(() -> ConvertisseurAvro.depuisJson(JSON, payload("50.123"), "adh-arrondi"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("adh-arrondi")
                .hasRootCauseInstanceOf(ArithmeticException.class);
    }

    @Test
    void refuse_un_json_illisible() {
        assertThatThrownBy(() -> ConvertisseurAvro.depuisJson(JSON, "ceci-n-est-pas-du-json", "adh1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("adh1");
    }

    @Test
    void refuse_un_json_auquel_il_manque_un_champ() {
        String sansMontant = "{\"idEvenement\":\"" + ID_EVENEMENT + "\",\"idAdherent\":\"adh1\","
                + "\"emiseLe\":\"2026-11-16T10:00:00Z\"}";

        assertThatThrownBy(() -> ConvertisseurAvro.depuisJson(JSON, sansMontant, "adh1"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
