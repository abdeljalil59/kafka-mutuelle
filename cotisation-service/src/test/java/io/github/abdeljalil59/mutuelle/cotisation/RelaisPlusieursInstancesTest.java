package io.github.abdeljalil59.mutuelle.cotisation;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.abdeljalil59.mutuelle.cotisation.adapter.out.jdbc.OutboxJdbc;
import io.github.abdeljalil59.mutuelle.cotisation.application.RelayerOutbox;
import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.EvenementOutbox;
import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.PublicationCotisation;
import io.github.abdeljalil59.mutuelle.cotisation.domain.Cotisation;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

// Deux instances du relais sur le même Oracle (conteneur partagé) : chaque transaction a sa propre connexion
// (DriverManagerDataSource en fournit une par transaction, un thread par transaction). Déterministe : des verrous
// de synchronisation (CountDownLatch) à délai borné, aucune attente par durée.
class RelaisPlusieursInstancesTest extends OracleIntegrationTest {

    private static final String TOPIC = "cotisations.emises";
    private static final Instant T0 = Instant.parse("2026-11-24T10:00:00Z");
    private static final Instant ENVOI = Instant.parse("2026-11-24T11:00:00Z");
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final long DELAI_SECONDES = 30;
    private static final int PASSAGES_MAX = 10;

    private static JdbcClient jdbcClient;
    private static TransactionTemplate transaction;
    private OutboxJdbc outbox;
    private ExecutorService executeur;

    private record Reservation(String session, List<Integer> numeros) {
    }

    @BeforeAll
    static void cree_le_schema() {
        DataSource base = new DriverManagerDataSource(ORACLE.getJdbcUrl(), ORACLE.getUsername(), ORACLE.getPassword());
        new ResourceDatabasePopulator(new ClassPathResource("schema.sql")).execute(base);
        jdbcClient = JdbcClient.create(base);
        transaction = new TransactionTemplate(new DataSourceTransactionManager(base));
    }

    @BeforeEach
    void repart_d_une_table_vide() {
        jdbcClient.sql("DELETE FROM outbox").update();
        outbox = new OutboxJdbc(jdbcClient, JSON, TOPIC);
        executeur = Executors.newSingleThreadExecutor();
    }

    // Le thread de l'instance A est toujours arrêté (interrompu s'il attend encore) : sa transaction se termine et
    // ses verrous sont libérés, même si une assertion a échoué ou si un délai a expiré.
    @AfterEach
    void arrete_l_instance_a() throws InterruptedException {
        executeur.shutdownNow();
        assertThat(executeur.awaitTermination(DELAI_SECONDES, TimeUnit.SECONDS)).isTrue();
    }

    // (a) Deux réservations en même temps, dans deux transactions ouvertes sur deux connexions : A tient ses verrous
    // pendant que B lit. Puis des passages successifs jusqu'à 0 : chaque événement est réservé exactement une fois.
    @Test
    void deux_reservations_concurrentes_sont_disjointes_puis_chaque_evenement_est_reserve_une_fois() throws Exception {
        String[] cles = {"A", "B", "C", "D", "E", "F"};
        for (int n = 1; n <= 12; n++) {
            enregistrer(n, cles[(n - 1) % 6]);
        }
        CountDownLatch reserveeParA = new CountDownLatch(1);
        CountDownLatch libereA = new CountDownLatch(1);
        Future<Reservation> a = executeur.submit(() -> transaction.execute(statut -> {
            Reservation reservation = reserver(3);
            reserveeParA.countDown();
            attendre(libereA);
            marquer(reservation.numeros());
            return reservation;
        }));
        try {
            attendre(reserveeParA);
            Reservation b = transaction.execute(statut -> {
                Reservation reservation = reserver(3);
                marquer(reservation.numeros());
                return reservation;
            });
            libereA.countDown();
            Reservation reservationA = a.get(DELAI_SECONDES, TimeUnit.SECONDS);

            assertThat(reservationA.session()).as("deux connexions distinctes").isNotEqualTo(b.session());
            assertThat(reservationA.numeros()).containsExactly(1, 2, 3);
            assertThat(b.numeros()).containsExactly(4, 5, 6);

            List<Integer> reserves = new ArrayList<>(reservationA.numeros());
            reserves.addAll(b.numeros());
            List<List<Integer>> passages = new ArrayList<>();
            boolean vide = false;
            for (int i = 0; i < PASSAGES_MAX && !vide; i++) {
                List<Integer> passage = transaction.execute(statut -> {
                    List<Integer> numeros = numeros(outbox.reserverNonEnvoyes(3));
                    marquer(numeros);
                    return numeros;
                });
                if (passage.isEmpty()) {
                    vide = true;
                } else {
                    passages.add(passage);
                    reserves.addAll(passage);
                }
            }
            assertThat(vide).as("le dernier passage doit réserver 0 événement").isTrue();
            assertThat(passages).containsExactly(List.of(7, 8, 9), List.of(10, 11, 12));
            assertThat(reserves).containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
        } finally {
            libereA.countDown();
        }
    }

    // (b) Ordre des appels de publication, clé par clé. A tient les verrous de 1 et 2 sans avoir encore publié :
    // B ne peut ni publier 3 ni 5 (même clé X que 1) ; il publie 4. Puis des passages alternés des deux instances.
    @Test
    void l_ordre_de_publication_par_cle_est_celui_de_cree_le_entre_deux_instances() throws Exception {
        String[] cles = {"X", "Y", "X", "Z", "X"};
        for (int n = 1; n <= 5; n++) {
            enregistrer(n, cles[n - 1]);
        }
        List<Integer> ordre = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch aEnPublication = new CountDownLatch(1);
        CountDownLatch libereA = new CountDownLatch(1);
        Clock horloge = Clock.fixed(ENVOI, ZoneOffset.UTC);
        RelayerOutbox relaisA = new RelayerOutbox(
                outbox, new Enregistreur(ordre, aEnPublication, libereA), horloge, 2);
        RelayerOutbox relaisB = new RelayerOutbox(outbox, new Enregistreur(ordre, null, null), horloge, 2);
        Future<Integer> a = executeur.submit(() -> transaction.execute(statut -> relaisA.relayer()));
        try {
            attendre(aEnPublication);
            Integer publiesParB = transaction.execute(statut -> relaisB.relayer());
            assertThat(ordre).containsExactly(4);
            assertThat(publiesParB).isEqualTo(1);

            libereA.countDown();
            assertThat(a.get(DELAI_SECONDES, TimeUnit.SECONDS)).isEqualTo(2);
            assertThat(ordre).containsExactly(4, 1, 2);

            int zerosConsecutifs = 0;
            for (int i = 0; i < PASSAGES_MAX && zerosConsecutifs < 2; i++) {
                RelayerOutbox relais = i % 2 == 0 ? relaisB : relaisA;
                Integer publies = transaction.execute(statut -> relais.relayer());
                zerosConsecutifs = publies == 0 ? zerosConsecutifs + 1 : 0;
            }
            assertThat(zerosConsecutifs).as("deux passages consécutifs à 0 événement").isEqualTo(2);
            assertThat(ordre).containsExactly(4, 1, 2, 3, 5);
            assertThat(ordre.stream().filter(n -> cles[n - 1].equals("X")).toList()).containsExactly(1, 3, 5);
        } finally {
            libereA.countDown();
        }
    }

    // Publication factice : enregistre l'ordre des appels. Avec un verrou de synchronisation, elle signale qu'elle est
    // atteinte (les verrous de la réservation sont alors tenus) puis attend avant d'enregistrer.
    private static final class Enregistreur implements PublicationCotisation {

        private final List<Integer> ordre;
        private final CountDownLatch signalEnPublication;
        private final CountDownLatch attente;

        private Enregistreur(List<Integer> ordre, CountDownLatch signalEnPublication, CountDownLatch attente) {
            this.ordre = ordre;
            this.signalEnPublication = signalEnPublication;
            this.attente = attente;
        }

        @Override
        public void publier(String topic, String cle, String payload) {
            if (attente != null) {
                signalEnPublication.countDown();
                attendre(attente);
            }
            for (int n = 1; n <= 12; n++) {
                if (payload.contains(ev(n).toString())) {
                    ordre.add(n);
                    return;
                }
            }
            throw new IllegalStateException("événement inconnu : " + payload);
        }
    }

    private void enregistrer(int numero, String cle) {
        outbox.enregistrer(new Cotisation(cle, new BigDecimal("10.00"), T0.plusSeconds(numero)), ev(numero));
    }

    // À appeler dans une transaction : retourne aussi la session Oracle qui porte les verrous.
    private Reservation reserver(int limite) {
        String session = jdbcClient.sql("SELECT SYS_CONTEXT('USERENV', 'SID') FROM dual")
                .query(String.class).single();
        return new Reservation(session, numeros(outbox.reserverNonEnvoyes(limite)));
    }

    private void marquer(List<Integer> numeros) {
        numeros.forEach(numero -> outbox.marquerEnvoye(ev(numero), ENVOI));
    }

    private static List<Integer> numeros(List<EvenementOutbox> evenements) {
        return evenements.stream()
                .map(evenement -> Integer.parseInt(evenement.idEvenement().toString().substring(24)))
                .toList();
    }

    private static UUID ev(int numero) {
        return UUID.fromString(String.format("aaaaaaaa-0000-0000-0000-%012d", numero));
    }

    private static void attendre(CountDownLatch verrou) {
        try {
            if (!verrou.await(DELAI_SECONDES, TimeUnit.SECONDS)) {
                throw new IllegalStateException("délai de synchronisation dépassé");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrompu pendant la synchronisation", e);
        }
    }
}
