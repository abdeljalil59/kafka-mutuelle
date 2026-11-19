package io.github.abdeljalil59.mutuelle.events.avro;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.UUID;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.BinaryMessageEncoder;
import org.junit.jupiter.api.Test;

// Contrat Avro seul (encodage binaire "single object" d'Avro), sans Schema Registry ni Kafka :
// ce que le schéma garantit, indépendamment des services.
class CotisationEmiseAvroTest {

    private static final UUID ID_EVENEMENT = UUID.fromString("55555555-5555-5555-5555-555555555555");
    private static final Instant EMISE_LE = Instant.parse("2026-11-16T10:00:00Z");

    private static CotisationEmise evenement(BigDecimal montant, Instant emiseLe) {
        return CotisationEmise.newBuilder()
                .setIdEvenement(ID_EVENEMENT)
                .setIdAdherent("adh1")
                .setMontant(montant)
                .setEmiseLe(emiseLe)
                .build();
    }

    private static CotisationEmise allerRetour(CotisationEmise evenement) throws IOException {
        return CotisationEmise.fromByteBuffer(evenement.toByteBuffer());
    }

    @Test
    void l_aller_retour_conserve_les_quatre_champs_avec_leurs_types_logiques() throws IOException {
        CotisationEmise lu = allerRetour(evenement(new BigDecimal("50.00"), EMISE_LE));

        assertThat(lu.getIdEvenement()).isEqualTo(ID_EVENEMENT);
        assertThat(lu.getIdAdherent()).isEqualTo("adh1");
        // isEqualTo (et non ByComparingTo) : l'échelle 2 fait partie du contrat.
        assertThat(lu.getMontant()).isEqualTo(new BigDecimal("50.00"));
        assertThat(lu.getEmiseLe()).isEqualTo(EMISE_LE);
    }

    @Test
    void tronque_l_instant_a_la_microseconde() throws IOException {
        // Horloge de production (Windows) : 100 ns de résolution, 9 décimales.
        Instant precis = Instant.parse("2026-11-18T11:32:20.803705900Z");

        CotisationEmise lu = allerRetour(evenement(new BigDecimal("50.00"), precis));

        assertThat(lu.getEmiseLe()).isEqualTo(Instant.parse("2026-11-18T11:32:20.803705Z"));
    }

    @Test
    void normalise_lui_meme_une_echelle_inferieure_a_celle_du_schema() throws IOException {
        // Constaté : Avro 1.11.5 applique setScale(2, UNNECESSARY) à l'encodage, sans erreur : 50 devient 50.00.
        CotisationEmise lu = allerRetour(evenement(new BigDecimal("50"), EMISE_LE));

        assertThat(lu.getMontant()).isEqualTo(new BigDecimal("50.00"));
    }

    @Test
    void refuse_d_encoder_un_montant_qui_imposerait_un_arrondi() {
        // 50.123 ne tient pas dans decimal(19,2) sans arrondi : l'encodage échoue (et non un arrondi silencieux).
        // Le relais convertit lui-même (ConvertisseurAvro) pour classer cette erreur en donnée inconvertible,
        // et non en échec de publication transitoire : à l'envoi, elle serait enveloppée par le producteur.
        CotisationEmise troisDecimales = evenement(new BigDecimal("50.123"), EMISE_LE);

        assertThatThrownBy(troisDecimales::toByteBuffer)
                .isInstanceOf(AvroTypeException.class)
                .hasMessageContaining("scale");
    }

    @Test
    void un_champ_absent_du_message_prend_la_valeur_par_defaut_du_schema() throws IOException {
        // Un ancien producteur qui n'écrivait que idAdherent : les autres champs viennent des défauts du schéma.
        Schema ancien = SchemaBuilder.record("CotisationEmise").namespace("io.github.abdeljalil59.mutuelle.events.avro")
                .fields().requiredString("idAdherent").endRecord();
        GenericRecord ancienEvenement = new GenericData.Record(ancien);
        ancienEvenement.put("idAdherent", "adh-ancien");
        ByteBuffer octets = new BinaryMessageEncoder<GenericRecord>(GenericData.get(), ancien).encode(ancienEvenement);

        BinaryMessageDecoder<CotisationEmise> decodeur = CotisationEmise.getDecoder();
        decodeur.addSchema(ancien);
        CotisationEmise lu = decodeur.decode(octets);

        assertThat(lu.getIdAdherent()).isEqualTo("adh-ancien");
        assertThat(lu.getIdEvenement()).isEqualTo(new UUID(0L, 0L));
        assertThat(lu.getMontant()).isEqualByComparingTo("0");
        assertThat(lu.getEmiseLe()).isEqualTo(Instant.EPOCH);
    }
}
