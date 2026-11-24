package io.github.abdeljalil59.mutuelle.cotisation.adapter.out.jdbc;

import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.EvenementOutbox;
import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.Outbox;
import io.github.abdeljalil59.mutuelle.cotisation.domain.Cotisation;
import io.github.abdeljalil59.mutuelle.events.CotisationEmise;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
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

    // Ordre : cree_le, puis id_evenement pour un ordre déterministe en cas d'égalité.
    @Override
    public List<EvenementOutbox> lireNonEnvoyes(int limite) {
        return jdbcClient
                .sql("SELECT id_evenement, topic, cle, payload FROM outbox WHERE envoye_le IS NULL "
                        + "ORDER BY cree_le, id_evenement FETCH FIRST ? ROWS ONLY")
                .param(limite)
                .query((rs, numeroLigne) -> new EvenementOutbox(
                        UUID.fromString(rs.getString("id_evenement")),
                        rs.getString("topic"),
                        rs.getString("cle"),
                        rs.getString("payload")))
                .list();
    }

    @Override
    public void marquerEnvoye(UUID idEvenement, Instant envoyeLe) {
        jdbcClient.sql("UPDATE outbox SET envoye_le = ? WHERE id_evenement = ?")
                .param(OffsetDateTime.ofInstant(envoyeLe, ZoneOffset.UTC))
                .param(idEvenement.toString())
                .update();
    }
}
