package io.github.abdeljalil59.mutuelle.cotisation.adapter.in.rest;

import java.math.BigDecimal;

public record EmettreCotisationRequete(String idAdherent, BigDecimal montant) {
}
