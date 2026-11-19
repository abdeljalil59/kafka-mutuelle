package io.github.abdeljalil59.mutuelle.prelevement.adapter.out.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import io.github.abdeljalil59.mutuelle.prelevement.OracleIntegrationTest;
import io.github.abdeljalil59.mutuelle.prelevement.domain.Decision;
import io.github.abdeljalil59.mutuelle.prelevement.domain.DecisionPrelevement;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

// Vrai adaptateur contre un vrai Oracle Free (conteneur partagé), créé avec le schema.sql de production.
class JournalDecisionsJdbcTest extends OracleIntegrationTest {

    private static final UUID ID_EVENEMENT_1 = UUID.fromString("bbbbbbbb-0000-0000-0000-000000000001");
    private static final UUID ID_EVENEMENT_2 = UUID.fromString("bbbbbbbb-0000-0000-0000-000000000002");
    private static final Instant DECIDE_LE = Instant.parse("2026-11-18T09:00:00.123Z");

    private record Ligne(String idEvenement, String idAdherent, BigDecimal montant, String decision, Instant decideLe) {
    }

    private static JdbcClient jdbcClient;
    private JournalDecisionsJdbc journal;

    @BeforeAll
    static void cree_le_schema() {
        DataSource base = new DriverManagerDataSource(ORACLE.getJdbcUrl(), ORACLE.getUsername(), ORACLE.getPassword());
        new ResourceDatabasePopulator(new ClassPathResource("schema.sql")).execute(base);
        jdbcClient = JdbcClient.create(base);
    }

    @BeforeEach
    void repart_d_une_table_vide() {
        // La base est partagée avec les autres classes de test (même conteneur) : on repart d'une table vide.
        jdbcClient.sql("DELETE FROM decision_prelevement").update();
        journal = new JournalDecisionsJdbc(jdbcClient);
    }

    private static DecisionPrelevement decision(UUID idEvenement, String idAdherent, String montant, Decision decision) {
        return new DecisionPrelevement(idEvenement, idAdherent, new BigDecimal(montant), decision, DECIDE_LE);
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
        journal.enregistrer(decision(ID_EVENEMENT_1, "adh1", "50.00", Decision.ACCEPTE));

        assertThat(lignes()).singleElement().satisfies(ligne -> {
            assertThat(ligne.idEvenement()).isEqualTo(ID_EVENEMENT_1.toString());
            assertThat(ligne.idAdherent()).isEqualTo("adh1");
            assertThat(ligne.montant()).isEqualByComparingTo("50.00");
            assertThat(ligne.decision()).isEqualTo("ACCEPTE");
            assertThat(ligne.decideLe()).isEqualTo(DECIDE_LE);
        });
    }

    @Test
    void un_doublon_donne_une_seule_ligne_et_conserve_la_premiere_valeur() {
        journal.enregistrer(decision(ID_EVENEMENT_1, "adh1", "50.00", Decision.ACCEPTE));

        // Même idEvenement, autres valeurs : l'écriture est ignorée sans exception (ORA-00001 traduit en doublon).
        journal.enregistrer(decision(ID_EVENEMENT_1, "adh2", "150.00", Decision.REJETE));

        assertThat(lignes()).singleElement().satisfies(ligne -> {
            assertThat(ligne.idAdherent()).isEqualTo("adh1");
            assertThat(ligne.montant()).isEqualByComparingTo("50.00");
            assertThat(ligne.decision()).isEqualTo("ACCEPTE");
        });
    }

    @Test
    void deux_idEvenement_differents_donnent_deux_lignes() {
        journal.enregistrer(decision(ID_EVENEMENT_1, "adh1", "50.00", Decision.ACCEPTE));
        journal.enregistrer(decision(ID_EVENEMENT_2, "adh1", "150.00", Decision.REJETE));

        assertThat(lignes()).extracting(Ligne::idEvenement)
                .containsExactly(ID_EVENEMENT_1.toString(), ID_EVENEMENT_2.toString());
    }

    @Test
    void un_idAdherent_de_plus_de_64_caracteres_leve_une_erreur_d_acces_aux_donnees() {
        DecisionPrelevement tropLongue = decision(ID_EVENEMENT_1, "x".repeat(65), "50.00", Decision.ACCEPTE);

        // Spring ne classe pas ORA-12899 (valeur trop longue) parmi les violations d'intégrité d'Oracle (codes
        // 1400, 1722, 2291, 2292) : l'erreur reste une UncategorizedSQLException. Sous H2, le même cas donnait une
        // DataIntegrityViolationException.
        assertThatExceptionOfType(UncategorizedSQLException.class)
                .isThrownBy(() -> journal.enregistrer(tropLongue))
                .withMessageContaining("ORA-12899");
        assertThat(lignes()).isEmpty();
    }
}
