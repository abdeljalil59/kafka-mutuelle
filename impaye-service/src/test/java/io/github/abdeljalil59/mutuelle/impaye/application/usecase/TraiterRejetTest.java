package io.github.abdeljalil59.mutuelle.impaye.application.usecase;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
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

// Les règles de la relance, sans Kafka ni base : seuls les deux ports sont simulés (Mockito ne sert qu'aux ports).
class TraiterRejetTest {

    private static final int SEUIL = 3;
    private static final Instant MAINTENANT = Instant.parse("2026-11-20T10:00:00Z");
    private static final Impaye IMPAYE = new Impaye(
            UUID.fromString("44444444-4444-4444-4444-444444444444"), "adh1", new BigDecimal("150.00"),
            Instant.parse("2026-11-20T08:00:00Z"));

    private final PersistanceImpaye persistance = mock(PersistanceImpaye.class);
    private final PublicationRelance publicationRelance = mock(PublicationRelance.class);
    private final TraiterRejet traiterRejet = new TraiterRejet(
            persistance, publicationRelance, Clock.fixed(MAINTENANT, ZoneOffset.UTC), SEUIL);

    // Un nouvel impayé dont le décompte, une fois enregistré, vaut `nombre`.
    private void rejetNouveauAvecDecompte(long nombre) {
        when(persistance.enregistrer(IMPAYE)).thenReturn(true);
        when(persistance.compterParAdherent("adh1")).thenReturn(nombre);
    }

    @Test
    void un_rejet_sous_le_seuil_est_enregistre_sans_relance() {
        rejetNouveauAvecDecompte(SEUIL - 1);

        traiterRejet.traiter(IMPAYE);

        verify(persistance).enregistrer(IMPAYE);
        verifyNoInteractions(publicationRelance);
    }

    @Test
    void le_rejet_qui_atteint_le_seuil_publie_une_relance_avec_le_decompte_et_le_dernier_montant() {
        rejetNouveauAvecDecompte(SEUIL);

        traiterRejet.traiter(IMPAYE);

        verify(publicationRelance).publier(new Relance("adh1", SEUIL, new BigDecimal("150.00"), MAINTENANT));
    }

    // « Atteint » : une seule relance par adhérent, au N-ième rejet. Les suivants n'en émettent pas.
    @Test
    void un_rejet_au_dela_du_seuil_ne_relance_pas_a_nouveau() {
        rejetNouveauAvecDecompte(SEUIL + 1);

        traiterRejet.traiter(IMPAYE);

        verifyNoInteractions(publicationRelance);
    }

    // IDEMPOTENCE : le N-ième rejet rejoué retrouverait un décompte égal au seuil (il est déjà en base). Seul le résultat
    // de l'enregistrement le distingue d'un vrai N-ième rejet : le doublon ne relance pas, et le décompte n'est même pas lu.
    @Test
    void un_doublon_ne_relance_pas_meme_au_seuil() {
        when(persistance.enregistrer(IMPAYE)).thenReturn(false);
        when(persistance.compterParAdherent("adh1")).thenReturn((long) SEUIL);

        traiterRejet.traiter(IMPAYE);

        verify(persistance, never()).compterParAdherent(any());
        verifyNoInteractions(publicationRelance);
    }

    @Test
    void avec_un_seuil_de_un_le_premier_rejet_relance() {
        TraiterRejet premierRejet = new TraiterRejet(
                persistance, publicationRelance, Clock.fixed(MAINTENANT, ZoneOffset.UTC), 1);
        rejetNouveauAvecDecompte(1);

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

    // L'échec remonte tel quel : c'est le gestionnaire d'erreurs du consommateur qui réessaie le message.
    @Test
    void laisse_remonter_l_echec_de_la_publication() {
        rejetNouveauAvecDecompte(SEUIL);
        PublicationRelanceImpossibleException echec =
                new PublicationRelanceImpossibleException("broker injoignable", new IllegalStateException("cause"));
        doThrow(echec).when(publicationRelance).publier(any());

        assertThatThrownBy(() -> traiterRejet.traiter(IMPAYE)).isSameAs(echec);
    }

    // LIMITE CONNUE, documentée par ce test : l'enregistrement et la publication ne sont pas atomiques. Après un échec de
    // publication, l'impayé est déjà en base : la livraison suivante du même message le trouve en doublon et ne relance
    // pas. La relance est perdue. Corriger demande un état « relance émise » en base ou un outbox (hors de cette version).
    @Test
    void apres_un_echec_de_publication_le_rejeu_du_message_ne_relance_pas() {
        rejetNouveauAvecDecompte(SEUIL);
        doThrow(new PublicationRelanceImpossibleException("broker injoignable", new IllegalStateException("cause")))
                .when(publicationRelance).publier(any());
        assertThatThrownBy(() -> traiterRejet.traiter(IMPAYE)).isInstanceOf(PublicationRelanceImpossibleException.class);

        when(persistance.enregistrer(IMPAYE)).thenReturn(false);
        traiterRejet.traiter(IMPAYE);

        // Une seule tentative de publication en tout : celle qui a échoué.
        verify(publicationRelance, times(1)).publier(any());
    }
}
