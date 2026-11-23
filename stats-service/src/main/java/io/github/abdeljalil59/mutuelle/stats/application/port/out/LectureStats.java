package io.github.abdeljalil59.mutuelle.stats.application.port.out;

import io.github.abdeljalil59.mutuelle.stats.domain.StatsAdherent;

import java.util.Map;
import java.util.Optional;

/** Lecture seule des statistiques agrégées. */
public interface LectureStats {

    Optional<StatsAdherent> parAdherent(String idAdherent);

    /** Toutes les statistiques, triées par identifiant d'adhérent. */
    Map<String, StatsAdherent> toutes();
}
