package io.github.abdeljalil59.mutuelle.impaye.application.usecase;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.github.abdeljalil59.mutuelle.impaye.application.PublicationRelanceImpossibleException;
import io.github.abdeljalil59.mutuelle.impaye.application.port.out.PersistanceImpaye;
import io.github.abdeljalil59.mutuelle.impaye.application.port.out.PublicationRelance;
import io.github.abdeljalil59.mutuelle.impaye.domain.Impaye;
import io.github.abdeljalil59.mutuelle.impaye.domain.Relance;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

// Les règles de la relance, sans Kafka ni base : seuls les deux ports sont simulés (Mockito ne sert qu'aux ports).
class TraiterRejetTest {

    private static final int SEUIL = 3;
    private static final Instant MAINTENANT = Instant.parse("2026-11-20T10:00:00Z");
    private static final Impaye IMPAYE = new Impaye(
            UUID.fromString("44444444-4444-4444-4444-444444444444"), "adh1", new BigDecimal("150.00"),
            Instant.parse("2026-11-20T08:00:00Z"));
    private static final Impaye IMPAYE_SUIVANT = new Impaye(
            UUID.fromString("55555555-5555-5555-5555-555555555555"), "adh1", new BigDecimal("90.00"),
            Instant.parse("2026-11-20T09:00:00Z"));

    private final PersistanceImpaye persistance = mock(PersistanceImpaye.class);
    private final PublicationRelance publicationRelance = mock(PublicationRelance.class);
    private final TraiterRejet traiterRejet = new TraiterRejet(
            persistance, publicationRelance, Clock.fixed(MAINTENANT, ZoneOffset.UTC), SEUIL);

    // État de la base vu par le cas d'usage après l'enregistrement : décompte de l'adhérent et relance déjà émise ou non.
    private void base(long decompte, boolean relanceDejaEmise) {
        when(persistance.compterParAdherent("adh1")).thenReturn(decompte);
        when(persistance.relanceEmise("adh1")).thenReturn(relanceDejaEmise);
    }

    @Test
    void un_rejet_sous_le_seuil_est_enregistre_sans_relance() {
        base(SEUIL - 1, false);

        traiterRejet.traiter(IMPAYE);

        verify(persistance).enregistrer(IMPAYE);
        verifyNoInteractions(publicationRelance);
        verify(persistance, never()).marquerRelanceEmise(any());
    }

    // L'ordre est la garantie : publier PUIS marquer. Marquer d'abord perdrait la relance si la publication échouait.
    @Test
    void le_rejet_qui_atteint_le_seuil_publie_la_relance_puis_marque_l_adherent() {
        base(SEUIL, false);

        traiterRejet.traiter(IMPAYE);

        InOrder ordre = inOrder(publicationRelance, persistance);
        ordre.verify(publicationRelance).publier(new Relance("adh1", SEUIL, new BigDecimal("150.00"), MAINTENANT));
        ordre.verify(persistance).marquerRelanceEmise("adh1");
    }

    // Une seule relance par adhérent : le drapeau, et non l'égalité du décompte au seuil, empêche la seconde.
    @Test
    void au_dela_du_seuil_une_relance_deja_emise_n_est_pas_republiee() {
        base(SEUIL + 1, true);

        traiterRejet.traiter(IMPAYE_SUIVANT);

        verifyNoInteractions(publicationRelance);
        verify(persistance, never()).marquerRelanceEmise(any());
    }

    // IDEMPOTENCE : le N-ième rejet rejoué retrouve un décompte égal au seuil, mais sa relance est déjà émise : rien ne part.
    @Test
    void un_doublon_dont_la_relance_est_deja_emise_ne_relance_pas() {
        when(persistance.enregistrer(IMPAYE)).thenReturn(false);
        base(SEUIL, true);

        traiterRejet.traiter(IMPAYE);

        verifyNoInteractions(publicationRelance);
        verify(persistance, never()).marquerRelanceEmise(any());
    }

    // Si la publication échoue, rien n'est marqué : la relance reste à faire, et l'exception remonte pour les réessais.
    @Test
    void un_echec_de_publication_ne_marque_pas_l_adherent_et_remonte() {
        base(SEUIL, false);
        PublicationRelanceImpossibleException echec =
                new PublicationRelanceImpossibleException("broker injoignable", new IllegalStateException("cause"));
        doThrow(echec).when(publicationRelance).publier(any());

        assertThatThrownBy(() -> traiterRejet.traiter(IMPAYE)).isSameAs(echec);

        verify(persistance, never()).marquerRelanceEmise(any());
    }

    // REPRISE (c'était la limite connue) : la publication échoue au N-ième rejet, puis ce MÊME message est rejoué (réessai du
    // gestionnaire d'erreurs) : l'impayé est un doublon, mais la relance n'est pas marquée, donc elle part.
    @Test
    void apres_un_echec_de_publication_le_rejeu_du_message_emet_la_relance() {
        base(SEUIL, false);
        doThrow(new PublicationRelanceImpossibleException("broker injoignable", new IllegalStateException("cause")))
                .doNothing().when(publicationRelance).publier(any());
        assertThatThrownBy(() -> traiterRejet.traiter(IMPAYE)).isInstanceOf(PublicationRelanceImpossibleException.class);

        when(persistance.enregistrer(IMPAYE)).thenReturn(false);
        traiterRejet.traiter(IMPAYE);

        verify(publicationRelance, times(2)).publier(new Relance("adh1", SEUIL, new BigDecimal("150.00"), MAINTENANT));
        verify(persistance, times(1)).marquerRelanceEmise("adh1");
    }

    // REPRISE au rejet suivant : le message abandonné par le gestionnaire d'erreurs n'est pas rejoué, mais le rejet suivant de
    // l'adhérent (décompte au-delà du seuil, relance non marquée) émet la relance, avec son décompte et son montant à lui.
    @Test
    void apres_un_echec_de_publication_le_rejet_suivant_emet_la_relance() {
        base(SEUIL, false);
        doThrow(new PublicationRelanceImpossibleException("broker injoignable", new IllegalStateException("cause")))
                .when(publicationRelance).publier(any());
        assertThatThrownBy(() -> traiterRejet.traiter(IMPAYE)).isInstanceOf(PublicationRelanceImpossibleException.class);

        base(SEUIL + 1, false);
        doNothing().when(publicationRelance).publier(any());
        traiterRejet.traiter(IMPAYE_SUIVANT);

        verify(publicationRelance).publier(new Relance("adh1", SEUIL + 1, new BigDecimal("90.00"), MAINTENANT));
        verify(persistance).marquerRelanceEmise("adh1");
    }

    // LIMITE ASSUMÉE : si le marquage échoue après une publication réussie, le réessai republie. Une relance en double, jamais
    // une relance perdue : le consommateur de impayes.relances doit être idempotent.
    @Test
    void un_echec_du_marquage_apres_publication_fait_republier_au_rejeu() {
        base(SEUIL, false);
        doThrow(new IllegalStateException("base indisponible")).doNothing().when(persistance).marquerRelanceEmise("adh1");
        assertThatThrownBy(() -> traiterRejet.traiter(IMPAYE)).isInstanceOf(IllegalStateException.class);

        when(persistance.enregistrer(IMPAYE)).thenReturn(false);
        traiterRejet.traiter(IMPAYE);

        verify(publicationRelance, times(2)).publier(new Relance("adh1", SEUIL, new BigDecimal("150.00"), MAINTENANT));
        verify(persistance, times(2)).marquerRelanceEmise("adh1");
    }

    @Test
    void avec_un_seuil_de_un_le_premier_rejet_relance() {
        TraiterRejet premierRejet = new TraiterRejet(
                persistance, publicationRelance, Clock.fixed(MAINTENANT, ZoneOffset.UTC), 1);
        base(1, false);

        premierRejet.traiter(IMPAYE);

        verify(publicationRelance).publier(new Relance("adh1", 1, new BigDecimal("150.00"), MAINTENANT));
    }

    @Test
    void refuse_un_seuil_inferieur_a_un() {
        Clock horloge = Clock.fixed(MAINTENANT, ZoneOffset.UTC);

        assertThatThrownBy(() -> new TraiterRejet(persistance, publicationRelance, horloge, 0))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("seuil");
        assertThatThrownBy(() -> new TraiterRejet(persistance, publicationRelance, horloge, -3))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
