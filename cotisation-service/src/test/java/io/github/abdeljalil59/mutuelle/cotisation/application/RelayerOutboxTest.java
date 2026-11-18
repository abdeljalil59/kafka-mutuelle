package io.github.abdeljalil59.mutuelle.cotisation.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.EvenementOutbox;
import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.Outbox;
import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.PublicationCotisation;
import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.PublicationImpossibleException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RelayerOutboxTest {

    private static final String TOPIC = "cotisations.emises";
    private static final int TAILLE_LOT = 50;
    private static final Instant MAINTENANT = Instant.parse("2026-11-16T10:00:00Z");
    private static final UUID ID_EVENEMENT_1 = UUID.fromString("aaaaaaaa-0000-0000-0000-000000000001");
    private static final UUID ID_EVENEMENT_2 = UUID.fromString("aaaaaaaa-0000-0000-0000-000000000002");
    private static final UUID ID_EVENEMENT_3 = UUID.fromString("aaaaaaaa-0000-0000-0000-000000000003");

    @Mock
    private Outbox outbox;

    @Mock
    private PublicationCotisation publication;

    private RelayerOutbox relais() {
        return new RelayerOutbox(outbox, publication, Clock.fixed(MAINTENANT, ZoneOffset.UTC), TAILLE_LOT);
    }

    private static EvenementOutbox evenement(UUID idEvenement, String cle) {
        return new EvenementOutbox(idEvenement, TOPIC, cle, "{\"idAdherent\":\"" + cle + "\"}");
    }

    @Test
    void relaie_les_evenements_non_envoyes_dans_l_ordre() {
        EvenementOutbox premier = evenement(ID_EVENEMENT_1, "adh1");
        EvenementOutbox second = evenement(ID_EVENEMENT_2, "adh2");
        when(outbox.lireNonEnvoyes(TAILLE_LOT)).thenReturn(List.of(premier, second));

        int relayes = relais().relayer();

        assertThat(relayes).isEqualTo(2);
        InOrder ordre = inOrder(publication, outbox);
        ordre.verify(publication).publier(TOPIC, "adh1", premier.payload());
        // Le marquage vient APRÈS la publication de l'événement.
        ordre.verify(outbox).marquerEnvoye(ID_EVENEMENT_1, MAINTENANT);
        ordre.verify(publication).publier(TOPIC, "adh2", second.payload());
        ordre.verify(outbox).marquerEnvoye(ID_EVENEMENT_2, MAINTENANT);
    }

    @Test
    void arrete_le_lot_au_premier_echec_de_publication() {
        EvenementOutbox premier = evenement(ID_EVENEMENT_1, "adh1");
        EvenementOutbox second = evenement(ID_EVENEMENT_2, "adh2");
        EvenementOutbox troisieme = evenement(ID_EVENEMENT_3, "adh3");
        when(outbox.lireNonEnvoyes(TAILLE_LOT)).thenReturn(List.of(premier, second, troisieme));
        // Mockito en mode strict : l'appel réussi pour adh1 doit être déclaré, sinon il est signalé
        // comme un appel aux arguments différents de ceux du stub d'échec.
        doNothing().when(publication).publier(TOPIC, "adh1", premier.payload());
        doThrow(new PublicationImpossibleException("broker injoignable", new RuntimeException()))
                .when(publication).publier(TOPIC, "adh2", second.payload());

        assertThatExceptionOfType(PublicationImpossibleException.class).isThrownBy(() -> relais().relayer());

        // Le premier était publié : il est marqué. Le second a échoué : il ne l'est pas.
        verify(outbox).marquerEnvoye(ID_EVENEMENT_1, MAINTENANT);
        verify(outbox, never()).marquerEnvoye(eq(ID_EVENEMENT_2), any());
        // Le reste du lot n'est pas tenté : l'ordre par clé est conservé.
        verify(publication, never()).publier(TOPIC, "adh3", troisieme.payload());
        verify(outbox, never()).marquerEnvoye(eq(ID_EVENEMENT_3), any());
    }

    @Test
    void retourne_zero_quand_il_n_y_a_rien_a_relayer() {
        when(outbox.lireNonEnvoyes(TAILLE_LOT)).thenReturn(List.of());

        int relayes = relais().relayer();

        assertThat(relayes).isZero();
        verifyNoInteractions(publication);
        verify(outbox, never()).marquerEnvoye(any(), any());
    }
}
