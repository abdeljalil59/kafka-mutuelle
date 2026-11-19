package io.github.abdeljalil59.mutuelle.prelevement.adapter.out.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.tuple;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.github.abdeljalil59.mutuelle.prelevement.OracleIntegrationTest;
import io.github.abdeljalil59.mutuelle.prelevement.domain.Decision;
import io.github.abdeljalil59.mutuelle.prelevement.domain.DecisionPrelevement;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

// Vrai adaptateur contre un vrai Oracle Free (conteneur partagé), créé avec le schema.sql de production.
class JournalDecisionsJdbcTest extends OracleIntegrationTest {

    private static final Instant DECIDE_LE = Instant.parse("2026-11-18T09:00:00.123Z");

    private record Ligne(String idEvenement, String idAdherent, BigDecimal montant, String decision, Instant decideLe) {
    }

    private static JdbcClient jdbcClient;
    private static JdbcTemplate jdbcTemplate;
    // Vrai registre en mémoire (pas de simulation) : il prouve les noms, les étiquettes et les valeurs des compteurs.
    private SimpleMeterRegistry registre;
    private JournalDecisionsJdbc journal;

    @BeforeAll
    static void cree_le_schema() {
        DataSource base = new DriverManagerDataSource(ORACLE.getJdbcUrl(), ORACLE.getUsername(), ORACLE.getPassword());
        new ResourceDatabasePopulator(new ClassPathResource("schema.sql")).execute(base);
        jdbcClient = JdbcClient.create(base);
        jdbcTemplate = new JdbcTemplate(base);
    }

    @BeforeEach
    void repart_d_une_table_vide() {
        // La base est partagée avec les autres classes de test (même conteneur) : on repart d'une table vide.
        jdbcClient.sql("DELETE FROM decision_prelevement").update();
        registre = new SimpleMeterRegistry();
        journal = new JournalDecisionsJdbc(jdbcTemplate, registre);
    }

    private double decisionsEnregistrees(Decision decision) {
        return registre.get("prelevement.decisions").tag("decision", decision.name()).counter().count();
    }

    private double doublonsIgnores() {
        return registre.get("prelevement.doublons.ignores").counter().count();
    }

    private static UUID idEvenement(int numero) {
        return UUID.fromString(String.format("bbbbbbbb-0000-0000-0000-%012d", numero));
    }

    private static DecisionPrelevement decision(int numero, String idAdherent, String montant, Decision decision) {
        return new DecisionPrelevement(idEvenement(numero), idAdherent, new BigDecimal(montant), decision, DECIDE_LE);
    }

    private List<Ligne> lignes() {
        return jdbcClient
                .sql("SELECT id_evenement, id_adherent, montant, decision, decide_le "
                        + "FROM decision_prelevement ORDER BY id_evenement")
                .query((rs, numeroLigne) -> new Ligne(
                        rs.getString("id_evenement"),
                        rs.getString("id_adherent"),
                        rs.getBigDecimal("montant"),
                        rs.getString("decision"),
                        rs.getObject("decide_le", OffsetDateTime.class).toInstant()))
                .list();
    }

    @Test
    void un_enregistrement_donne_une_ligne_avec_toutes_les_colonnes() {
        journal.enregistrer(List.of(decision(1, "adh1", "50.00", Decision.ACCEPTE)));

        assertThat(lignes()).singleElement().satisfies(ligne -> {
            assertThat(ligne.idEvenement()).isEqualTo(idEvenement(1).toString());
            assertThat(ligne.idAdherent()).isEqualTo("adh1");
            assertThat(ligne.montant()).isEqualByComparingTo("50.00");
            assertThat(ligne.decision()).isEqualTo("ACCEPTE");
            assertThat(ligne.decideLe()).isEqualTo(DECIDE_LE);
        });
    }

    @Test
    void un_doublon_est_ignore_sans_exception_ni_avertissement_et_conserve_la_premiere_valeur() {
        journal.enregistrer(List.of(decision(1, "adh1", "50.00", Decision.ACCEPTE)));
        Logger racine = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        ListAppender<ILoggingEvent> traces = new ListAppender<>();
        traces.start();
        racine.addAppender(traces);
        try {
            // Même idEvenement, autres valeurs : MERGE n'a pas de WHEN MATCHED, la ligne existante n'est pas touchée.
            assertThatCode(() -> journal.enregistrer(List.of(decision(1, "adh2", "150.00", Decision.REJETE))))
                    .doesNotThrowAnyException();
        } finally {
            racine.detachAppender(traces);
            traces.stop();
        }

        assertThat(traces.list).extracting(ILoggingEvent::getLevel).noneMatch(niveau -> niveau.isGreaterOrEqual(Level.WARN));
        assertThat(lignes()).singleElement().satisfies(ligne -> {
            assertThat(ligne.idAdherent()).isEqualTo("adh1");
            assertThat(ligne.montant()).isEqualByComparingTo("50.00");
            assertThat(ligne.decision()).isEqualTo("ACCEPTE");
        });
    }

    @Test
    void un_lot_de_deux_idEvenement_differents_donne_deux_lignes() {
        journal.enregistrer(List.of(
                decision(1, "adh1", "50.00", Decision.ACCEPTE),
                decision(2, "adh1", "150.00", Decision.REJETE)));

        assertThat(lignes()).extracting(Ligne::idEvenement)
                .containsExactly(idEvenement(1).toString(), idEvenement(2).toString());
    }

    @Test
    void un_lot_de_500_decisions_donne_500_lignes() {
        List<DecisionPrelevement> decisions = new ArrayList<>();
        for (int numero = 1; numero <= 500; numero++) {
            decisions.add(decision(numero, "adh" + numero, "50.00", Decision.ACCEPTE));
        }

        journal.enregistrer(decisions);

        assertThat(lignes()).hasSize(500);
    }

    @Test
    void un_lot_avec_un_doublon_en_base_et_un_doublon_dans_le_lot_ecrit_chaque_idEvenement_une_fois() {
        journal.enregistrer(List.of(decision(1, "adh-en-base", "50.00", Decision.ACCEPTE)));

        journal.enregistrer(List.of(
                decision(2, "adh-premier", "50.00", Decision.ACCEPTE),
                decision(1, "adh-doublon-en-base", "150.00", Decision.REJETE),
                decision(3, "adh3", "50.00", Decision.ACCEPTE),
                decision(2, "adh-doublon-dans-le-lot", "150.00", Decision.REJETE)));

        assertThat(lignes()).extracting(Ligne::idEvenement, Ligne::idAdherent, Ligne::decision).containsExactly(
                tuple(idEvenement(1).toString(), "adh-en-base", "ACCEPTE"),
                tuple(idEvenement(2).toString(), "adh-premier", "ACCEPTE"),
                tuple(idEvenement(3).toString(), "adh3", "ACCEPTE"));
    }

    // Prouve sur Oracle, à travers JdbcTemplate.batchUpdate, que le MERGE renvoie 1 (inséré) ou 0 (doublon ignoré).
    @Test
    void compte_les_decisions_inserees_par_type_et_les_doublons_ignores_d_apres_les_resultats_d_oracle() {
        journal.enregistrer(List.of(
                decision(1, "adh1", "50.00", Decision.ACCEPTE),
                decision(2, "adh2", "150.00", Decision.REJETE),
                decision(1, "adh-doublon-dans-le-lot", "50.00", Decision.ACCEPTE),
                decision(3, "adh3", "50.00", Decision.ACCEPTE)));
        journal.enregistrer(List.of(
                decision(2, "adh2", "150.00", Decision.REJETE),
                decision(4, "adh4", "50.00", Decision.ACCEPTE)));

        // 4 lignes insérées : 1, 3 et 4 acceptées, 2 rejetée ; 2 doublons ignorés : 1 dans le lot, 2 déjà en base.
        assertThat(decisionsEnregistrees(Decision.ACCEPTE)).isEqualTo(3.0);
        assertThat(decisionsEnregistrees(Decision.REJETE)).isEqualTo(1.0);
        assertThat(doublonsIgnores()).isEqualTo(2.0);
        // Les compteurs disent la même chose que la table.
        assertThat(lignes()).hasSize(4);
    }

    @Test
    void un_lot_vide_n_ecrit_rien_et_ne_leve_pas_d_erreur() {
        assertThatCode(() -> journal.enregistrer(List.of())).doesNotThrowAnyException();

        assertThat(lignes()).isEmpty();
    }

    @Test
    void un_idAdherent_de_plus_de_64_caracteres_leve_une_erreur_d_acces_aux_donnees() {
        DecisionPrelevement tropLongue = decision(1, "x".repeat(65), "50.00", Decision.ACCEPTE);

        // Spring ne classe pas ORA-12899 (valeur trop longue) parmi les violations d'intégrité d'Oracle (codes
        // 1400, 1722, 2291, 2292) : l'erreur reste une UncategorizedSQLException. Sous H2, le même cas donnait une
        // DataIntegrityViolationException.
        assertThatExceptionOfType(UncategorizedSQLException.class)
                .isThrownBy(() -> journal.enregistrer(List.of(tropLongue)))
                .withMessageContaining("ORA-12899");
        assertThat(lignes()).isEmpty();
    }

    // Constaté sur Oracle (essai hors dépôt, puis ce test) : le driver s'arrête à la ligne en échec. Les lignes qui la
    // précèdent restent écrites (validation automatique), celles qui la suivent ne sont pas exécutées. La réécriture
    // du lot étant idempotente, cet état partiel est sans conséquence pour le rejeu.
    @Test
    void un_echec_au_milieu_d_un_lot_laisse_les_lignes_precedentes_ecrites_et_les_suivantes_non() {
        List<DecisionPrelevement> lot = List.of(
                decision(1, "adh1", "50.00", Decision.ACCEPTE),
                decision(2, "x".repeat(65), "50.00", Decision.ACCEPTE),
                decision(3, "adh3", "50.00", Decision.ACCEPTE));

        assertThatExceptionOfType(UncategorizedSQLException.class)
                .isThrownBy(() -> journal.enregistrer(lot))
                .withMessageContaining("ORA-12899");

        assertThat(lignes()).extracting(Ligne::idEvenement).containsExactly(idEvenement(1).toString());
    }
}
