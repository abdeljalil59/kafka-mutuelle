package io.github.abdeljalil59.mutuelle.prelevement.adapter.out.jdbc;

import io.github.abdeljalil59.mutuelle.prelevement.application.port.out.JournalDecisions;
import io.github.abdeljalil59.mutuelle.prelevement.domain.DecisionPrelevement;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;

public class JournalDecisionsJdbc implements JournalDecisions {

    // Sans WHEN MATCHED : une ligne déjà présente n'est pas touchée (le doublon est ignoré, sans exception).
    private static final String FUSION = "MERGE INTO decision_prelevement cible "
            + "USING (SELECT ? AS id_evenement, ? AS id_adherent, ? AS montant, ? AS decision, ? AS decide_le "
            + "FROM DUAL) source "
            + "ON (cible.id_evenement = source.id_evenement) "
            + "WHEN NOT MATCHED THEN INSERT (id_evenement, id_adherent, montant, decision, decide_le) "
            + "VALUES (source.id_evenement, source.id_adherent, source.montant, source.decision, source.decide_le)";

    // JdbcClient n'a pas d'API de lot (constaté dans spring-jdbc 7.0.10) : l'adaptateur utilise JdbcTemplate.batchUpdate.
    private final JdbcTemplate jdbcTemplate;

    public JournalDecisionsJdbc(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    // Un MERGE par décision, exécuté en un seul lot JDBC : le doublon est tranché ligne par ligne par Oracle,
    // y compris un doublon à l'intérieur du lot. Toute autre erreur d'accès aux données remonte : elle relève des
    // réessais du gestionnaire d'erreurs. Un lot en échec peut avoir écrit les lignes précédant l'erreur
    // (la connexion est en validation automatique) : sans conséquence, la réécriture est idempotente.
    @Override
    public void enregistrer(List<DecisionPrelevement> decisions) {
        jdbcTemplate.batchUpdate(FUSION, decisions.stream().map(JournalDecisionsJdbc::parametres).toList());
    }

    private static Object[] parametres(DecisionPrelevement decision) {
        return new Object[] {
                decision.idEvenement().toString(),
                decision.idAdherent(),
                decision.montant(),
                decision.decision().name(),
                // Le driver Oracle (ojdbc17) refuse un Instant en paramètre (ORA-17004, constaté) :
                // on lie un OffsetDateTime en UTC, que TIMESTAMP WITH TIME ZONE accepte.
                OffsetDateTime.ofInstant(decision.decideLe(), ZoneOffset.UTC)
        };
    }
}
