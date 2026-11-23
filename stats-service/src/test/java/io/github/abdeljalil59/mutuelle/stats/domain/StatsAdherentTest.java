package io.github.abdeljalil59.mutuelle.stats.domain;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class StatsAdherentTest {

    @Test
    void unAgregatVideEstAZero() {
        assertThat(StatsAdherent.vide()).isEqualTo(new StatsAdherent(0, 0, 0.0, 0L));
    }

    @Test
    void unAcceptePlusUnAccepteIncrementeLeCompteurEtCumuleLeMontant() {
        StatsAdherent stats = StatsAdherent.vide()
                .avecAccepte(new BigDecimal("42.50"), 1_000L)
                .avecAccepte(new BigDecimal("10.00"), 2_000L);

        assertThat(stats.nombreAcceptes()).isEqualTo(2);
        assertThat(stats.nombreRejetes()).isZero();
        assertThat(stats.montantTotal()).isEqualTo(52.50);
    }

    @Test
    void unRejeteIncrementeSonCompteurSansToucherAuxAcceptes() {
        StatsAdherent stats = StatsAdherent.vide().avecRejete(new BigDecimal("150.00"), 1_000L);

        assertThat(stats.nombreRejetes()).isEqualTo(1);
        assertThat(stats.nombreAcceptes()).isZero();
        assertThat(stats.montantTotal()).isEqualTo(150.0);
    }

    @Test
    void ledernierEvenementResteLePlusRecentMemeSiUnResultatAncienArriveEnRetard() {
        StatsAdherent stats = StatsAdherent.vide()
                .avecAccepte(BigDecimal.TEN, 5_000L)
                .avecRejete(BigDecimal.TEN, 3_000L);

        assertThat(stats.dernierEvenement()).isEqualTo(5_000L);
    }

    @Test
    void l_agregatEstImmuable() {
        StatsAdherent depart = StatsAdherent.vide();

        depart.avecAccepte(BigDecimal.ONE, 1L);

        assertThat(depart).isEqualTo(StatsAdherent.vide());
    }
}
