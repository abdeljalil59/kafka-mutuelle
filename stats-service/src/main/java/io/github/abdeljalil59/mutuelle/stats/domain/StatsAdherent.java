package io.github.abdeljalil59.mutuelle.stats.domain;

import java.math.BigDecimal;

/**
 * Statistiques de prélèvement d'un adhérent. Immuable : chaque résultat produit un nouvel agrégat.
 *
 * @param montantTotal     somme des montants de tous les résultats (acceptés et rejetés), en double pour simplifier le JSON
 * @param dernierEvenement date du résultat le plus récent, en millisecondes depuis l'epoch (0 si aucun)
 */
public record StatsAdherent(int nombreAcceptes, int nombreRejetes, double montantTotal, long dernierEvenement) {

    public static StatsAdherent vide() {
        return new StatsAdherent(0, 0, 0.0, 0L);
    }

    public StatsAdherent avecAccepte(BigDecimal montant, long decideLeMillis) {
        return new StatsAdherent(nombreAcceptes + 1, nombreRejetes, montantTotal + montant.doubleValue(),
                Math.max(dernierEvenement, decideLeMillis));
    }

    public StatsAdherent avecRejete(BigDecimal montant, long decideLeMillis) {
        return new StatsAdherent(nombreAcceptes, nombreRejetes + 1, montantTotal + montant.doubleValue(),
                Math.max(dernierEvenement, decideLeMillis));
    }
}
