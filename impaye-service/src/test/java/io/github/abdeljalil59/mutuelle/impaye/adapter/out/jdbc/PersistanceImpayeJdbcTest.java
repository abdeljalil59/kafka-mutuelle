package io.github.abdeljalil59.mutuelle.impaye.adapter.out.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.github.abdeljalil59.mutuelle.impaye.domain.Impaye;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

// L'adaptateur contre un vrai H2 en mémoire, avec le schema.sql du service : ce sont la contrainte d'unicité et le
// traducteur d'exceptions de Spring qui sont éprouvés, un faux JdbcClient ne prouverait rien de cela.
class PersistanceImpayeJdbcTest {

    private static final UUID ID = UUID.fromString("44444444-4444-4444-4444-444444444444");
    // Précision en microsecondes, comme le timestamp-micros du contrat Avro.
    private static final Instant DECIDE_LE = Instant.parse("2026-11-20T08:00:00.123456Z");
    private static final Instant ENREGISTRE_LE = Instant.parse("2026-11-20T09:30:00Z");

    private record Ligne(String idEvenement, String idAdherent, BigDecimal montant,
            OffsetDateTime decideLe, OffsetDateTime enregistreLe) {
    }

    private EmbeddedDatabase base;
    private JdbcClient jdbcClient;
    private SimpleMeterRegistry meterRegistry;
    private PersistanceImpayeJdbc persistance;

    @BeforeEach
    void ouvre_une_base_h2_en_memoire_avec_le_schema_du_service() {
        base = new EmbeddedDatabaseBuilder()
                .generateUniqueName(true)
                .setType(EmbeddedDatabaseType.H2)
                .addScript("schema.sql")
                .build();
        jdbcClient = JdbcClient.create(base);
        meterRegistry = new SimpleMeterRegistry();
        persistance = new PersistanceImpayeJdbc(
                jdbcClient, Clock.fixed(ENREGISTRE_LE, ZoneOffset.UTC), meterRegistry);
    }

    @AfterEach
    void ferme_la_base() {
        base.shutdown();
    }

    private List<Ligne> lignes() {
        return jdbcClient.sql("SELECT id_evenement, id_adherent, montant, decide_le, enregistre_le FROM impaye ORDER BY id")
                .query((rs, numero) -> new Ligne(
                        rs.getString("id_evenement"),
                        rs.getString("id_adherent"),
                        rs.getBigDecimal("montant"),
                        rs.getObject("decide_le", OffsetDateTime.class),
                        rs.getObject("enregistre_le", OffsetDateTime.class)))
                .list();
    }

    private double doublonsIgnores() {
        return meterRegistry.get("impaye.doublons.ignores").counter().count();
    }

    @Test
    void enregistre_l_impaye_avec_ses_valeurs_et_l_instant_d_enregistrement() {
        boolean enregistre = persistance.enregistrer(new Impaye(ID, "adh1", new BigDecimal("150.00"), DECIDE_LE));

        assertThat(enregistre).isTrue();

        assertThat(lignes()).singleElement().satisfies(ligne -> {
            assertThat(ligne.idEvenement()).isEqualTo(ID.toString());
            assertThat(ligne.idAdherent()).isEqualTo("adh1");
            assertThat(ligne.montant()).isEqualByComparingTo("150.00");
            assertThat(ligne.decideLe().toInstant()).isEqualTo(DECIDE_LE);
            assertThat(ligne.enregistreLe().toInstant()).isEqualTo(ENREGISTRE_LE);
        });
    }

    @Test
    void un_premier_impaye_n_est_pas_compte_comme_un_doublon() {
        persistance.enregistrer(new Impaye(ID, "adh1", new BigDecimal("150.00"), DECIDE_LE));

        // La série existe dès la construction de l'adaptateur, à 0 : elle est lisible avant tout doublon.
        assertThat(doublonsIgnores()).isZero();
    }

    // D11 : un résultat rejoué ne crée pas de seconde ligne, ne lève pas d'exception, et gonfle le compteur. La ligne
    // d'origine reste intacte : le doublon n'écrase rien (un « MERGE ... KEY » de H2 l'aurait écrasée).
    @Test
    void ignore_un_doublon_sans_exception_le_compte_et_conserve_la_premiere_ligne() {
        persistance.enregistrer(new Impaye(ID, "adh1", new BigDecimal("150.00"), DECIDE_LE));

        assertThatCode(() -> assertThat(persistance.enregistrer(
                new Impaye(ID, "adh-autre", new BigDecimal("999.99"), DECIDE_LE.plusSeconds(60)))).isFalse())
                .doesNotThrowAnyException();

        assertThat(lignes()).singleElement().satisfies(ligne -> {
            assertThat(ligne.idAdherent()).isEqualTo("adh1");
            assertThat(ligne.montant()).isEqualByComparingTo("150.00");
        });
        assertThat(doublonsIgnores()).isEqualTo(1.0);
    }

    // Le décompte de la relance : une ligne par impayé, donc un doublon ne le fait pas bouger.
    @Test
    void compte_les_impayes_par_adherent_sans_compter_les_doublons() {
        persistance.enregistrer(new Impaye(ID, "adh1", new BigDecimal("150.00"), DECIDE_LE));
        persistance.enregistrer(new Impaye(ID, "adh1", new BigDecimal("150.00"), DECIDE_LE));
        persistance.enregistrer(new Impaye(UUID.randomUUID(), "adh1", new BigDecimal("120.00"), DECIDE_LE));
        persistance.enregistrer(new Impaye(UUID.randomUUID(), "adh2", new BigDecimal("110.00"), DECIDE_LE));

        assertThat(persistance.compterParAdherent("adh1")).isEqualTo(2);
        assertThat(persistance.compterParAdherent("adh2")).isEqualTo(1);
        assertThat(persistance.compterParAdherent("adh-inconnu")).isZero();
    }

    @Test
    void des_evenements_differents_ne_sont_pas_des_doublons() {
        persistance.enregistrer(new Impaye(ID, "adh1", new BigDecimal("150.00"), DECIDE_LE));
        persistance.enregistrer(new Impaye(UUID.randomUUID(), "adh1", new BigDecimal("120.00"), DECIDE_LE));

        assertThat(lignes()).hasSize(2);
        assertThat(doublonsIgnores()).isZero();
    }
}
