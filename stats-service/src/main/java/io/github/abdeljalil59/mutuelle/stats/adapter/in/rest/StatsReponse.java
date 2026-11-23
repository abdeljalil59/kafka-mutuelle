package io.github.abdeljalil59.mutuelle.stats.adapter.in.rest;

import io.github.abdeljalil59.mutuelle.stats.domain.StatsAdherent;

/** Corps JSON de l'API, distinct de l'objet du domaine. */
public record StatsReponse(int nombreAcceptes, int nombreRejetes, double montantTotal, long dernierEvenement) {

    static StatsReponse de(StatsAdherent stats) {
        return new StatsReponse(stats.nombreAcceptes(), stats.nombreRejetes(), stats.montantTotal(),
                stats.dernierEvenement());
    }
}
