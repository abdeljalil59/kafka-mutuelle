package io.github.abdeljalil59.mutuelle.cotisation.adapter.out.jdbc;

import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.EvenementOutbox;
import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.Outbox;
import io.github.abdeljalil59.mutuelle.cotisation.domain.Cotisation;
import io.github.abdeljalil59.mutuelle.events.CotisationEmise;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import tools.jackson.databind.json.JsonMapper;

public class OutboxJdbc implements Outbox {

    private final JdbcClient jdbcClient;
    private final JsonMapper jsonMapper;
    private final String topic;

    public OutboxJdbc(JdbcClient jdbcClient, JsonMapper jsonMapper, String topic) {
        this.jdbcClient = jdbcClient;
        this.jsonMapper = jsonMapper;
        this.topic = topic;
    }

    // Participe à la transaction en cours (celle du cas d'usage) : même connexion, même validation.
    @Override
    public void enregistrer(Cotisation cotisation, UUID idEvenement) {
        CotisationEmise evenement = new CotisationEmise(
                idEvenement, cotisation.idAdherent(), cotisation.montant(), cotisation.emiseLe());
        jdbcClient.sql("INSERT INTO outbox (id_evenement, topic, cle, payload, cree_le) VALUES (?, ?, ?, ?, ?)")
                .param(idEvenement.toString())
                .param(topic)
                .param(cotisation.idAdherent())
                .param(jsonMapper.writeValueAsString(evenement))
                // ojdbc17 refuse un Instant en paramètre (ORA-17004, constaté) : on lie un OffsetDateTime UTC.
                .param(OffsetDateTime.ofInstant(cotisation.emiseLe(), ZoneOffset.UTC))
                .update();
    }

    // Nombre de candidats lus par réservation : FACTEUR_CANDIDATS x limite. Les candidats sont les têtes de clé, lues
    // sans verrou ; chaque instance en verrouille jusqu'à « limite », les lignes déjà verrouillées par une autre sont
    // sautées. 3 couvre jusqu'à 3 instances ; au-delà, seul le débit baisse (une instance peut ne rien réserver),
    // jamais la correction.
    static final int FACTEUR_CANDIDATS = 3;

    // Temps 1, sans verrou : le plus ancien non envoyé de chaque clé, du plus ancien au plus récent
    // (cree_le, puis id_evenement pour un ordre déterministe en cas d'égalité).
    private static final String CANDIDATS = "SELECT id_evenement FROM (SELECT id_evenement, cree_le, "
            + "ROW_NUMBER() OVER (PARTITION BY cle ORDER BY cree_le, id_evenement) AS rang "
            + "FROM outbox WHERE envoye_le IS NULL) WHERE rang = 1 "
            + "ORDER BY cree_le, id_evenement FETCH FIRST ? ROWS ONLY";

    // Temps 2, par candidat : le verrou, sans attente (SKIP LOCKED). Le FETCH FIRST n'est pas dans cette instruction :
    // Oracle 23.4 le refuse avec FOR UPDATE (ORA-02014).
    private static final String RESERVATION = "SELECT id_evenement, topic, cle, payload FROM outbox "
            + "WHERE id_evenement = ? AND envoye_le IS NULL FOR UPDATE SKIP LOCKED";

    private static final String HORS_TRANSACTION = "reserverNonEnvoyes doit être appelé dans une transaction : "
            + "sans elle, les verrous seraient relâchés avant le marquage";

    @Override
    public List<EvenementOutbox> reserverNonEnvoyes(int limite) {
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalStateException(HORS_TRANSACTION);
        }
        List<String> candidats = jdbcClient.sql(CANDIDATS)
                .param(limite * FACTEUR_CANDIDATS)
                .query(String.class)
                .list();
        List<EvenementOutbox> reserves = new ArrayList<>();
        for (String candidat : candidats) {
            if (reserves.size() == limite) {
                break;
            }
            jdbcClient.sql(RESERVATION)
                    .param(candidat)
                    .query((rs, numeroLigne) -> new EvenementOutbox(
                            UUID.fromString(rs.getString("id_evenement")),
                            rs.getString("topic"),
                            rs.getString("cle"),
                            rs.getString("payload")))
                    .optional()
                    .ifPresent(reserves::add);
        }
        return reserves;
    }

    @Override
    public void marquerEnvoye(UUID idEvenement, Instant envoyeLe) {
        jdbcClient.sql("UPDATE outbox SET envoye_le = ? WHERE id_evenement = ?")
                .param(OffsetDateTime.ofInstant(envoyeLe, ZoneOffset.UTC))
                .param(idEvenement.toString())
                .update();
    }
}
