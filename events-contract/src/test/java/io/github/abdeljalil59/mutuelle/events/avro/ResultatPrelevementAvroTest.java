package io.github.abdeljalil59.mutuelle.events.avro;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.abdeljalil59.mutuelle.events.ResultatPrelevement;
import io.github.abdeljalil59.mutuelle.events.TypeResultat;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

// Contrat Avro seul (encodage binaire "single object" d'Avro), sans Schema Registry ni Kafka :
// ce que le schéma garantit, indépendamment des services.
class ResultatPrelevementAvroTest {

    private static final UUID ID_EVENEMENT = UUID.fromString("66666666-6666-6666-6666-666666666666");
    private static final Instant DECIDE_LE = Instant.parse("2026-11-20T09:00:00Z");

    private static ResultatPrelevement resultat(TypeResultat type, BigDecimal montant, Instant decideLe) {
        return ResultatPrelevement.newBuilder()
                .setIdEvenement(ID_EVENEMENT)
                .setType(type)
                .setIdAdherent("adh1")
                .setMontant(montant)
                .setDecideLe(decideLe)
                .build();
    }

    private static ResultatPrelevement allerRetour(ResultatPrelevement resultat) throws IOException {
        return ResultatPrelevement.fromByteBuffer(resultat.toByteBuffer());
    }

    @Test
    void l_aller_retour_en_octets_avro_redonne_un_resultat_egal() throws IOException {
        ResultatPrelevement ecrit = resultat(TypeResultat.ACCEPTE, new BigDecimal("75.00"), DECIDE_LE);

        ResultatPrelevement lu = allerRetour(ecrit);

        assertThat(lu).isEqualTo(ecrit);
        assertThat(lu.getIdEvenement()).isEqualTo(ID_EVENEMENT);
        assertThat(lu.getType()).isEqualTo(TypeResultat.ACCEPTE);
        assertThat(lu.getIdAdherent()).isEqualTo("adh1");
        // isEqualTo (et non ByComparingTo) : l'échelle 2 fait partie du contrat.
        assertThat(lu.getMontant()).isEqualTo(new BigDecimal("75.00"));
        assertThat(lu.getDecideLe()).isEqualTo(DECIDE_LE);
    }

    @Test
    void le_montant_est_un_BigDecimal_et_non_un_ByteBuffer() throws Exception {
        // Le plugin Avro génère un ByteBuffer pour un décimal tant que enableDecimalLogicalType n'est pas activé.
        assertThat(ResultatPrelevement.class.getMethod("getMontant").getReturnType()).isEqualTo(BigDecimal.class);

        ResultatPrelevement lu = allerRetour(resultat(TypeResultat.ACCEPTE, new BigDecimal("75.00"), DECIDE_LE));

        assertThat(lu.getMontant()).isInstanceOf(BigDecimal.class);
    }

    @Test
    void decideLe_est_un_Instant_tronque_a_la_microseconde() throws Exception {
        assertThat(ResultatPrelevement.class.getMethod("getDecideLe").getReturnType()).isEqualTo(Instant.class);
        // Horloge de production (Windows) : 100 ns de résolution, 9 décimales.
        Instant precis = Instant.parse("2026-11-20T09:00:00.803705900Z");

        ResultatPrelevement lu = allerRetour(resultat(TypeResultat.REJETE, new BigDecimal("150.00"), precis));

        assertThat(lu.getDecideLe()).isInstanceOf(Instant.class).isEqualTo(Instant.parse("2026-11-20T09:00:00.803705Z"));
    }

    @Test
    void le_type_est_l_enum_TypeResultat_avec_exactement_ACCEPTE_et_REJETE() throws IOException {
        assertThat(TypeResultat.values()).containsExactly(TypeResultat.ACCEPTE, TypeResultat.REJETE);

        for (TypeResultat attendu : TypeResultat.values()) {
            ResultatPrelevement lu = allerRetour(resultat(attendu, new BigDecimal("10.00"), DECIDE_LE));

            assertThat(lu.getType()).isEqualTo(attendu);
        }
    }

    // Le schéma dit ce que la décision D5 dit : types logiques uuid, decimal(19,2) et timestamp-micros.
    @Test
    void le_schema_porte_les_types_logiques_decides() {
        Schema schema = ResultatPrelevement.getClassSchema();

        assertThat(schema.getFullName()).isEqualTo("io.github.abdeljalil59.mutuelle.events.ResultatPrelevement");
        assertThat(schema.getField("idEvenement").schema().getLogicalType().getName()).isEqualTo("uuid");
        assertThat(schema.getField("decideLe").schema().getLogicalType().getName()).isEqualTo("timestamp-micros");
        LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) schema.getField("montant").schema().getLogicalType();
        assertThat(decimal.getPrecision()).isEqualTo(19);
        assertThat(decimal.getScale()).isEqualTo(2);
        assertThat(schema.getField("type").schema().getEnumSymbols()).containsExactly("ACCEPTE", "REJETE");
    }
}
