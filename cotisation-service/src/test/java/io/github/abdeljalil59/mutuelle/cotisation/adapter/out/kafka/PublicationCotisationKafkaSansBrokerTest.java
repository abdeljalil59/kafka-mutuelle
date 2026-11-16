package io.github.abdeljalil59.mutuelle.cotisation.adapter.out.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.PublicationImpossibleException;
import io.github.abdeljalil59.mutuelle.cotisation.domain.Cotisation;
import io.github.abdeljalil59.mutuelle.events.CotisationEmise;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.ServerSocket;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

/**
 * Sans broker : aucun processus n'écoute sur le port utilisé (port libre obtenu puis fermé).
 * Constat du 16/11/2026 : quand les métadonnées sont indisponibles, send() lève une exception
 * de façon synchrone après max.block.ms, et la chaîne de causes contient le TimeoutException
 * de Kafka. Les messages des bibliothèques ne sont volontairement pas figés.
 */
class PublicationCotisationKafkaSansBrokerTest {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(PublicationCotisationKafkaSansBrokerTest.class);
    private static final String TOPIC = "cotisations.emises";
    private static final Instant EMISE_LE = Instant.parse("2026-11-16T10:00:00Z");
    private static final long MAX_BLOCK_MS = 1000;

    private DefaultKafkaProducerFactory<String, CotisationEmise> usine;
    private KafkaTemplate<String, CotisationEmise> template;

    @BeforeEach
    void creeUnProducteurSansBroker() throws IOException {
        Map<String, Object> proprietes = new HashMap<>();
        proprietes.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + obtenirUnPortLibre());
        proprietes.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        proprietes.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
        proprietes.put(JacksonJsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        proprietes.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, (int) MAX_BLOCK_MS);
        proprietes.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000);
        proprietes.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 2000);
        usine = new DefaultKafkaProducerFactory<>(proprietes);
        template = new KafkaTemplate<>(usine);
    }

    @AfterEach
    void fermeLeProducteur() {
        usine.destroy();
    }

    @Test
    void send_leve_de_facon_synchrone_une_exception_dont_la_cause_est_le_TimeoutException_de_kafka() {
        CotisationEmise evenement = new CotisationEmise(UUID.randomUUID(), "adh1", new BigDecimal("50.00"), EMISE_LE);

        long debut = System.nanoTime();
        Throwable erreur = catchThrowable(() -> template.send(TOPIC, "adh1", evenement));
        long dureeMs = ecouleMs(debut);

        LOGGER.info("CONSTAT send direct : exception synchrone après {} ms : {}", dureeMs, decrire(erreur));
        assertThat(erreur).isInstanceOf(RuntimeException.class);
        assertThat(chaineDeCauses(erreur)).anyMatch(TimeoutException.class::isInstance);
        assertThat(dureeMs).isBetween(900L, 5000L);
    }

    @Test
    void l_adaptateur_enveloppe_le_TimeoutException_de_kafka_dans_une_PublicationImpossibleException() {
        PublicationCotisationKafka adaptateur =
                new PublicationCotisationKafka(template, TOPIC, Duration.ofSeconds(5), UUID::randomUUID);
        Cotisation cotisation = new Cotisation("adh1", new BigDecimal("50.00"), EMISE_LE);

        long debut = System.nanoTime();
        Throwable erreur = catchThrowable(() -> adaptateur.publier(cotisation));
        long dureeMs = ecouleMs(debut);

        LOGGER.info("CONSTAT adaptateur : exception après {} ms : {}", dureeMs, decrire(erreur));
        assertThat(erreur).isInstanceOf(PublicationImpossibleException.class);
        assertThat(chaineDeCauses(erreur)).anyMatch(TimeoutException.class::isInstance);
        assertThat(dureeMs).isBetween(900L, 5000L);
    }

    private static int obtenirUnPortLibre() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static long ecouleMs(long debutNanos) {
        return Duration.ofNanos(System.nanoTime() - debutNanos).toMillis();
    }

    private static List<Throwable> chaineDeCauses(Throwable erreur) {
        List<Throwable> chaine = new ArrayList<>();
        for (Throwable courante = erreur; courante != null; courante = courante.getCause()) {
            chaine.add(courante);
        }
        return chaine;
    }

    private static String decrire(Throwable erreur) {
        StringBuilder description = new StringBuilder();
        for (Throwable courante : chaineDeCauses(erreur)) {
            if (!description.isEmpty()) {
                description.append(" <- cause : ");
            }
            description.append(courante.getClass().getName()).append(" (").append(courante.getMessage()).append(')');
        }
        return description.toString();
    }
}
