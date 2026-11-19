package io.github.abdeljalil59.mutuelle.prelevement.adapter.out.jdbc;

import io.github.abdeljalil59.mutuelle.prelevement.application.port.out.JournalDecisions;
import io.github.abdeljalil59.mutuelle.prelevement.domain.Decision;
import io.github.abdeljalil59.mutuelle.prelevement.domain.DecisionPrelevement;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
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
    private final Map<Decision, Counter> decisionsInserees = new EnumMap<>(Decision.class);
    private final Counter doublonsIgnores;

    public JournalDecisionsJdbc(JdbcTemplate jdbcTemplate, MeterRegistry meterRegistry) {
        this.jdbcTemplate = jdbcTemplate;
        // Les séries sont créées dès le démarrage, à 0 : sans cela, le passage de « série absente » à 1 échappe à
        // rate() et increase() de Prometheus. Forme counter(nom, étiquettes…) : c'est celle que les tests unitaires
        // peuvent simuler (Counter.builder(…).register(registre) passe par une méthode non publique de MeterRegistry).
        for (Decision decision : Decision.values()) {
            decisionsInserees.put(decision, meterRegistry.counter("prelevement.decisions", "decision", decision.name()));
        }
        doublonsIgnores = meterRegistry.counter("prelevement.doublons.ignores");
    }

    // Un MERGE par décision, exécuté en un seul lot JDBC : le doublon est tranché ligne par ligne par Oracle,
    // y compris un doublon à l'intérieur du lot. Toute autre erreur d'accès aux données remonte : elle relève des
    // réessais du gestionnaire d'erreurs. Un lot en échec peut avoir écrit les lignes précédant l'erreur
    // (la connexion est en validation automatique) : sans conséquence, la réécriture est idempotente.
    @Override
    public void enregistrer(List<DecisionPrelevement> decisions) {
        int[] resultats = jdbcTemplate.batchUpdate(FUSION, decisions.stream().map(JournalDecisionsJdbc::parametres).toList());
        compter(decisions, resultats);
    }

    // Un compteur par ligne selon le résultat du MERGE : 1 = décision insérée, 0 = doublon ignoré (constaté sur
    // Oracle 23.26.4 par JDBC seul, puis par JournalDecisionsJdbcTest à travers Spring). Une autre valeur (par exemple
    // SUCCESS_NO_INFO, -2, si le driver ne détaille pas) n'est pas comptée : mieux vaut ne rien compter que mal compter.
    // Limite : un lot qui échoue en cours de route ne renvoie aucun résultat, ses lignes déjà écrites ne sont pas
    // comptées, et leur rejeu (une par une, voir TraiterDemandePrelevement) les compte comme doublons ignorés.
    private void compter(List<DecisionPrelevement> decisions, int[] resultats) {
        for (int index = 0; index < resultats.length; index++) {
            if (resultats[index] == 0) {
                doublonsIgnores.increment();
            } else if (resultats[index] > 0) {
                decisionsInserees.get(decisions.get(index).decision()).increment();
            }
        }
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
