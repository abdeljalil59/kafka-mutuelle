package io.github.abdeljalil59.mutuelle.cotisation.adapter.in.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.github.abdeljalil59.mutuelle.cotisation.application.EmettreCotisation;
import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.PublicationImpossibleException;
import io.github.abdeljalil59.mutuelle.cotisation.domain.Cotisation;
import io.github.abdeljalil59.mutuelle.cotisation.domain.CotisationInvalideException;
import java.math.BigDecimal;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.assertj.MockMvcTester;
import org.springframework.test.web.servlet.assertj.MvcTestResult;

@WebMvcTest(CotisationController.class)
class CotisationControllerTest {

    private static final Instant EMISE_LE = Instant.parse("2026-11-16T10:00:00Z");

    @Autowired
    private MockMvcTester mvc;

    @MockitoBean
    private EmettreCotisation emettreCotisation;

    @Test
    void repond_201_avec_la_cotisation_emise() {
        when(emettreCotisation.emettre("adh1", new BigDecimal("50.00")))
                .thenReturn(new Cotisation("adh1", new BigDecimal("50.00"), EMISE_LE));

        MvcTestResult resultat = mvc.post().uri("/cotisations")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"idAdherent\":\"adh1\",\"montant\":50.00}")
                .exchange();

        assertThat(resultat)
                .hasStatus(HttpStatus.CREATED)
                .bodyJson()
                .isLenientlyEqualTo("{\"idAdherent\":\"adh1\",\"montant\":50.00,\"emiseLe\":\"2026-11-16T10:00:00Z\"}");
        // Texte brut : prouve que l'échelle du montant est conservée dans la réponse.
        assertThat(resultat).bodyText().contains("\"montant\":50.00");
        verify(emettreCotisation).emettre("adh1", new BigDecimal("50.00"));
    }

    @Test
    void repond_400_quand_la_cotisation_est_invalide() {
        when(emettreCotisation.emettre("adh1", BigDecimal.ZERO))
                .thenThrow(new CotisationInvalideException("montant doit être strictement positif"));

        assertThat(mvc.post().uri("/cotisations")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"idAdherent\":\"adh1\",\"montant\":0}"))
                .hasStatus(HttpStatus.BAD_REQUEST)
                .bodyJson()
                .isLenientlyEqualTo("{\"status\":400,\"title\":\"Cotisation invalide\","
                        + "\"detail\":\"montant doit être strictement positif\"}");
    }

    @Test
    void repond_503_quand_la_publication_est_impossible() {
        when(emettreCotisation.emettre("adh1", new BigDecimal("50.00")))
                .thenThrow(new PublicationImpossibleException("Échec de la publication", new RuntimeException("cause")));

        assertThat(mvc.post().uri("/cotisations")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"idAdherent\":\"adh1\",\"montant\":50.00}"))
                .hasStatus(HttpStatus.SERVICE_UNAVAILABLE)
                .bodyJson()
                .isLenientlyEqualTo("{\"status\":503,\"title\":\"Publication impossible\"}");
    }

    @Test
    void repond_400_pour_un_json_mal_forme_sans_appeler_le_cas_d_usage() {
        assertThat(mvc.post().uri("/cotisations")
                .contentType(MediaType.APPLICATION_JSON)
                .content("ceci n'est pas du json"))
                .hasStatus(HttpStatus.BAD_REQUEST);

        verifyNoInteractions(emettreCotisation);
    }
}
