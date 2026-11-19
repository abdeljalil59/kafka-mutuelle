package io.github.abdeljalil59.mutuelle.prelevement.adapter.out.jdbc;

import io.github.abdeljalil59.mutuelle.prelevement.application.port.out.JournalDecisions;
import io.github.abdeljalil59.mutuelle.prelevement.domain.DecisionPrelevement;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.simple.JdbcClient;

public class JournalDecisionsJdbc implements JournalDecisions {

    private static final Logger LOGGER = LoggerFactory.getLogger(JournalDecisionsJdbc.class);

    private static final String INSERTION = "INSERT INTO decision_prelevement "
            + "(id_evenement, id_adherent, montant, decision, decide_le) VALUES (?, ?, ?, ?, ?)";

    private final JdbcClient jdbcClient;

    public JournalDecisionsJdbc(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    // La contrainte d'unicité sur id_evenement tranche, y compris entre deux consommateurs concurrents.
    // Toute autre erreur d'accès aux données remonte : elle relève des réessais du gestionnaire d'erreurs.
    @Override
    public void enregistrer(DecisionPrelevement decision) {
        try {
            jdbcClient.sql(INSERTION)
                    .param(decision.idEvenement().toString())
                    .param(decision.idAdherent())
                    .param(decision.montant())
                    .param(decision.decision().name())
                    // Le driver Oracle (ojdbc17) refuse un Instant en paramètre (ORA-17004, constaté) :
                    // on lie un OffsetDateTime en UTC, que TIMESTAMP WITH TIME ZONE accepte.
                    .param(OffsetDateTime.ofInstant(decision.decideLe(), ZoneOffset.UTC))
                    .update();
        } catch (DuplicateKeyException e) {
            LOGGER.info("doublon ignoré idEvenement={}", decision.idEvenement());
        }
    }
}
