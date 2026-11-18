package io.github.abdeljalil59.mutuelle.cotisation.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.PublicationCotisation;
import io.github.abdeljalil59.mutuelle.cotisation.domain.Cotisation;
import io.github.abdeljalil59.mutuelle.cotisation.domain.CotisationInvalideException;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EmettreCotisationTest {

    private static final Instant MAINTENANT = Instant.parse("2026-11-16T10:00:00Z");
    private static final UUID ID_EVENEMENT = UUID.fromString("aaaaaaaa-0000-0000-0000-000000000001");

    @Mock
    private PublicationCotisation publication;

    private EmettreCotisation emettreCotisation;

    @BeforeEach
    void initialise() {
        emettreCotisation = new EmettreCotisation(
                publication, Clock.fixed(MAINTENANT, ZoneOffset.UTC), () -> ID_EVENEMENT);
    }

    @Test
    void publie_la_cotisation_creee_et_la_retourne() {
        Cotisation resultat = emettreCotisation.emettre("adh1", new BigDecimal("50.00"));

        Cotisation attendue = new Cotisation("adh1", new BigDecimal("50.00"), MAINTENANT);
        assertThat(resultat).isEqualTo(attendue);
        verify(publication).publier(attendue, ID_EVENEMENT);
    }

    @Test
    void ne_publie_rien_si_la_cotisation_est_invalide() {
        assertThatExceptionOfType(CotisationInvalideException.class)
                .isThrownBy(() -> emettreCotisation.emettre("adh1", BigDecimal.ZERO));

        verifyNoInteractions(publication);
    }
}
