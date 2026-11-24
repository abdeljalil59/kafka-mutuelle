package io.github.abdeljalil59.mutuelle.cotisation.adapter.out.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import io.github.abdeljalil59.mutuelle.cotisation.OracleIntegrationTest;
import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.EvenementOutbox;
import io.github.abdeljalil59.mutuelle.cotisation.domain.Cotisation;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

// Vrai adaptateur contre un vrai Oracle Free (conteneur partagé), créé avec le schéma Oracle de production.
// Le JsonMapper est un mapper Jackson 3 "nu" : le format du JsonMapper du contexte Spring
// (celui de la production) est constaté par OutboxIntegrationTest.
class OutboxJdbcTest extends OracleIntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(OutboxJdbcTest.class);
    private static final String TOPIC = "cotisations.emises";
    private static final UUID ID_EVENEMENT_1 = UUID.fromString("aaaaaaaa-0000-0000-0000-000000000001");
    private static final UUID ID_EVENEMENT_2 = UUID.fromString("aaaaaaaa-0000-0000-0000-000000000002");
    private static final UUID ID_EVENEMENT_3 = UUID.fromString("aaaaaaaa-0000-0000-0000-000000000003");
    private static final Instant T1 = Instant.parse("2026-11-16T10:00:00Z");
    private static final Instant T2 = Instant.parse("2026-11-16T10:00:01Z");
    private static final Instant T3 = Instant.parse("2026-11-16T10:00:02Z");
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private record Ligne(String idEvenement, String topic, String cle, String payload,
                         Instant creeLe, Instant envoyeLe) {
    }

    private static JdbcClient jdbcClient;
    private OutboxJdbc outbox;

    @BeforeAll
    static void cree_le_schema() {
        DataSource base = new DriverManagerDataSource(ORACLE.getJdbcUrl(), ORACLE.getUsername(), ORACLE.getPassword());
        new ResourceDatabasePopulator(new ClassPathResource("schema.sql")).execute(base);
        jdbcClient = JdbcClient.create(base);
    }

    @BeforeEach
    void repart_d_une_table_vide() {
        // La base est partagée entre les tests de la classe (même conteneur) : on repart d'une table vide.
        jdbcClient.sql("DELETE FROM outbox").update();
        outbox = new OutboxJdbc(jdbcClient, JSON, TOPIC);
    }

    private static Cotisation cotisation(String idAdherent, String montant, Instant emiseLe) {
        return new Cotisation(idAdherent, new BigDecimal(montant), emiseLe);
    }

    private List<Ligne> lignes() {
        return jdbcClient
                .sql("SELECT id_evenement, topic, cle, payload, cree_le, envoye_le FROM outbox ORDER BY id_evenement")
                .query((rs, numeroLigne) -> new Ligne(
                        rs.getString("id_evenement"),
                        rs.getString("topic"),
                        rs.getString("cle"),
                        rs.getString("payload"),
                        rs.getObject("cree_le", OffsetDateTime.class).toInstant(),
                        enInstant(rs.getObject("envoye_le", OffsetDateTime.class))))
                .list();
    }

    private static Instant enInstant(OffsetDateTime date) {
        return date == null ? null : date.toInstant();
    }

    @Test
    void enregistre_les_colonnes_attendues() {
        Instant emiseLe = Instant.parse("2026-11-16T10:00:00.123Z");

        outbox.enregistrer(cotisation("adh1", "50.00", emiseLe), ID_EVENEMENT_1);

        assertThat(lignes()).singleElement().satisfies(ligne -> {
            assertThat(ligne.idEvenement()).isEqualTo(ID_EVENEMENT_1.toString());
            assertThat(ligne.topic()).isEqualTo(TOPIC);
            assertThat(ligne.cle()).isEqualTo("adh1");
            assertThat(ligne.creeLe()).isEqualTo(emiseLe);
            assertThat(ligne.envoyeLe()).isNull();
            assertThat(ligne.payload()).isNotBlank();
        });
    }

    @Test
    void serialise_le_payload_au_format_du_contrat() {
        outbox.enregistrer(cotisation("adh1", "50.00", T1), ID_EVENEMENT_1);

        String payload = lignes().get(0).payload();
        LOGGER.info("PREUVE format du payload (JsonMapper nu) : {}", payload);
        JsonNode json = JSON.readTree(payload);
        assertThat(json.propertyNames()).containsExactlyInAnyOrder("idEvenement", "idAdherent", "montant", "emiseLe");
        assertThat(json.get("idEvenement").asString()).isEqualTo(ID_EVENEMENT_1.toString());
        assertThat(json.get("idAdherent").asString()).isEqualTo("adh1");
        assertThat(json.get("montant").isNumber()).isTrue();
        // 50.00 et non 50 : l'échelle du BigDecimal est conservée.
        assertThat(payload).contains("\"montant\":50.00");
        assertThat(json.get("emiseLe").isString()).isTrue();
        assertThat(json.get("emiseLe").asString()).isEqualTo("2026-11-16T10:00:00Z");
    }

    @Test
    void lit_uniquement_les_evenements_non_envoyes_par_ordre_de_creation() {
        // L'ordre de lecture ne suit ni l'ordre d'insertion (1, 2, 3) ni l'ordre des identifiants :
        // seul cree_le le produit.
        outbox.enregistrer(cotisation("adh1", "50.00", T3), ID_EVENEMENT_1);
        outbox.enregistrer(cotisation("adh2", "60.00", T1), ID_EVENEMENT_2);
        outbox.enregistrer(cotisation("adh3", "70.00", T2), ID_EVENEMENT_3);
        outbox.marquerEnvoye(ID_EVENEMENT_2, T3);

        List<EvenementOutbox> evenements = outbox.lireNonEnvoyes(10);

        assertThat(evenements).extracting(EvenementOutbox::idEvenement)
                .containsExactly(ID_EVENEMENT_3, ID_EVENEMENT_1);
        assertThat(evenements).first().satisfies(evenement -> {
            assertThat(evenement.topic()).isEqualTo(TOPIC);
            assertThat(evenement.cle()).isEqualTo("adh3");
            assertThat(evenement.payload()).contains(ID_EVENEMENT_3.toString());
        });
    }

    @Test
    void departage_les_dates_egales_par_idEvenement() {
        // Insérés dans l'ordre 2, 1 à la même date : seul le départage par id_evenement donne 1, 2.
        outbox.enregistrer(cotisation("adh2", "60.00", T1), ID_EVENEMENT_2);
        outbox.enregistrer(cotisation("adh1", "50.00", T1), ID_EVENEMENT_1);

        assertThat(outbox.lireNonEnvoyes(10)).extracting(EvenementOutbox::idEvenement)
                .containsExactly(ID_EVENEMENT_1, ID_EVENEMENT_2);
    }

    @Test
    void respecte_la_limite() {
        outbox.enregistrer(cotisation("adh1", "50.00", T1), ID_EVENEMENT_1);
        outbox.enregistrer(cotisation("adh2", "60.00", T2), ID_EVENEMENT_2);
        outbox.enregistrer(cotisation("adh3", "70.00", T3), ID_EVENEMENT_3);

        assertThat(outbox.lireNonEnvoyes(1)).extracting(EvenementOutbox::idEvenement)
                .containsExactly(ID_EVENEMENT_1);
        assertThat(outbox.lireNonEnvoyes(2)).extracting(EvenementOutbox::idEvenement)
                .containsExactly(ID_EVENEMENT_1, ID_EVENEMENT_2);
        assertThat(outbox.lireNonEnvoyes(10)).hasSize(3);
    }

    @Test
    void marquer_envoye_renseigne_la_date_et_retire_la_ligne_de_la_lecture() {
        Instant envoyeLe = Instant.parse("2026-11-16T10:05:00.456Z");
        outbox.enregistrer(cotisation("adh1", "50.00", T1), ID_EVENEMENT_1);

        outbox.marquerEnvoye(ID_EVENEMENT_1, envoyeLe);

        assertThat(lignes()).singleElement().satisfies(ligne -> assertThat(ligne.envoyeLe()).isEqualTo(envoyeLe));
        assertThat(outbox.lireNonEnvoyes(10)).isEmpty();
    }

    @Test
    void refuse_un_idEvenement_deja_present() {
        outbox.enregistrer(cotisation("adh1", "50.00", T1), ID_EVENEMENT_1);

        assertThatExceptionOfType(DuplicateKeyException.class)
                .isThrownBy(() -> outbox.enregistrer(cotisation("adh2", "60.00", T2), ID_EVENEMENT_1));

        assertThat(lignes()).singleElement().satisfies(ligne -> assertThat(ligne.cle()).isEqualTo("adh1"));
    }
}
