package io.github.abdeljalil59.mutuelle.cotisation.adapter.in.planification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.github.abdeljalil59.mutuelle.cotisation.application.RelayerOutbox;
import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.PublicationImpossibleException;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.LoggerFactory;

@ExtendWith(MockitoExtension.class)
class RelaiOutboxTest {

    private static final int PLAFOND_PAR_DEFAUT = 100;

    @Mock
    private RelayerOutbox relayerOutbox;

    private Logger logger;
    private ListAppender<ILoggingEvent> journal;

    @BeforeEach
    void capture_le_journal() {
        logger = (Logger) LoggerFactory.getLogger(RelaiOutbox.class);
        logger.setLevel(Level.INFO);
        journal = new ListAppender<>();
        journal.start();
        logger.addAppender(journal);
    }

    @AfterEach
    void detache_le_journal() {
        logger.detachAppender(journal);
    }

    @Test
    void un_seul_passage_quand_le_premier_ne_reserve_rien() {
        when(relayerOutbox.relayer()).thenReturn(0);

        new RelaiOutbox(relayerOutbox, PLAFOND_PAR_DEFAUT).relayer();

        verify(relayerOutbox, times(1)).relayer();
        assertThat(journal.list).isEmpty();
    }

    @Test
    void enchaine_les_passages_jusqu_a_un_passage_vide() {
        when(relayerOutbox.relayer()).thenReturn(3, 2, 0);

        new RelaiOutbox(relayerOutbox, PLAFOND_PAR_DEFAUT).relayer();

        verify(relayerOutbox, times(3)).relayer();
        assertThat(messages()).containsExactly(
                "INFO outbox : 3 événement(s) publié(s)",
                "INFO outbox : 2 événement(s) publié(s)");
    }

    // Le stub se termine par 0 : sans plafond, la boucle s'arrêterait au 11e passage et le test échouerait proprement.
    @Test
    void s_arrete_au_plafond() {
        when(relayerOutbox.relayer()).thenReturn(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0);

        new RelaiOutbox(relayerOutbox, 5).relayer();

        verify(relayerOutbox, times(5)).relayer();
    }

    @ParameterizedTest
    @MethodSource("echecs")
    void s_arrete_a_la_premiere_exception(RuntimeException echec, String niveauEtMessage, String messageDeLException) {
        when(relayerOutbox.relayer()).thenReturn(2).thenThrow(echec).thenReturn(5, 0);

        new RelaiOutbox(relayerOutbox, PLAFOND_PAR_DEFAUT).relayer();

        verify(relayerOutbox, times(2)).relayer();
        assertThat(messages()).containsExactly("INFO outbox : 2 événement(s) publié(s)", niveauEtMessage);
        ILoggingEvent dernier = journal.list.get(1);
        if (messageDeLException != null) {
            assertThat(dernier.getThrowableProxy().getMessage()).isEqualTo(messageDeLException);
        }
    }

    static Stream<Arguments> echecs() {
        return Stream.of(
                Arguments.of(new PublicationImpossibleException("broker injoignable", new RuntimeException()),
                        "WARN outbox : publication impossible, nouvel essai au prochain passage : broker injoignable",
                        null),
                Arguments.of(new IllegalArgumentException("payload non convertible"),
                        "ERROR outbox : échec du relais",
                        "payload non convertible"),
                Arguments.of(new IllegalStateException("erreur SQL"),
                        "ERROR outbox : échec du relais",
                        null));
    }

    @Test
    void fait_au_moins_un_passage_si_le_plafond_est_nul() {
        when(relayerOutbox.relayer()).thenReturn(0);

        new RelaiOutbox(relayerOutbox, 0).relayer();

        verify(relayerOutbox, times(1)).relayer();
    }

    private List<String> messages() {
        return journal.list.stream()
                .map(evenement -> evenement.getLevel() + " " + evenement.getFormattedMessage())
                .toList();
    }
}
