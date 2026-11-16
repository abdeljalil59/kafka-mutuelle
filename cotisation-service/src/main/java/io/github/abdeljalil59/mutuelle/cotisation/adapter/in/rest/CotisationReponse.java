package io.github.abdeljalil59.mutuelle.cotisation.adapter.in.rest;

import io.github.abdeljalil59.mutuelle.cotisation.domain.Cotisation;
import java.math.BigDecimal;
import java.time.Instant;

public record CotisationReponse(String idAdherent, BigDecimal montant, Instant emiseLe) {

    static CotisationReponse depuis(Cotisation cotisation) {
        return new CotisationReponse(cotisation.idAdherent(), cotisation.montant(), cotisation.emiseLe());
    }
}
