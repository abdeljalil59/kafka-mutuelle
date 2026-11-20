package io.github.abdeljalil59.mutuelle.impaye.adapter.out.jdbc;

import io.github.abdeljalil59.mutuelle.impaye.application.port.out.PersistanceImpaye;
import io.github.abdeljalil59.mutuelle.impaye.domain.Impaye;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.simple.JdbcClient;

public class PersistanceImpayeJdbc implements PersistanceImpaye {

    private static final String INSERTION = "INSERT INTO impaye (id_evenement, id_adherent, montant, decide_le, enregistre_le) "
            + "VALUES (?, ?, ?, ?, ?)";

    private final JdbcClient jdbcClient;
    private final Clock clock;
    private final Counter doublonsIgnores;

    public PersistanceImpayeJdbc(JdbcClient jdbcClient, Clock clock, MeterRegistry meterRegistry) {
        this.jdbcClient = jdbcClient;
        this.clock = clock;
        // Créé dès le démarrage, à 0 : sans cela, le passage de « série absente » à 1 échappe à rate() et increase().
        this.doublonsIgnores = meterRegistry.counter("impaye.doublons.ignores");
    }

    // INSERT simple, la contrainte d'unicité de id_evenement tranche : un doublon lève DuplicateKeyException, que
    // Spring déduit du code d'erreur de H2 (23505). Pas de SELECT préalable, qui laisserait une fenêtre entre la lecture
    // et l'écriture. La connexion est en validation automatique : l'échec de l'INSERT ne laisse rien à annuler. Dans une
    // base où une erreur annule la transaction en cours (PostgreSQL), il faudrait un MERGE ou ON CONFLICT à la place.
    @Override
    public void enregistrer(Impaye impaye) {
        try {
            jdbcClient.sql(INSERTION)
                    .params(impaye.idEvenement().toString(),
                            impaye.idAdherent(),
                            impaye.montant(),
                            OffsetDateTime.ofInstant(impaye.decideLe(), ZoneOffset.UTC),
                            OffsetDateTime.ofInstant(clock.instant(), ZoneOffset.UTC))
                    .update();
        } catch (DuplicateKeyException doublon) {
            doublonsIgnores.increment();
        }
    }
}
