package io.github.abdeljalil59.mutuelle.cotisation.adapter.out.kafka;

import io.github.abdeljalil59.mutuelle.events.avro.CotisationEmise;
import java.math.RoundingMode;
import tools.jackson.databind.json.JsonMapper;

// Convertit le JSON de l'outbox (format du record events.CotisationEmise) en objet Avro du contrat.
final class ConvertisseurAvro {

    private static final int ECHELLE_MONTANT = 2;   // decimal(19,2) dans le schéma

    private ConvertisseurAvro() {
    }

    // Toute erreur est une donnée inconvertible (et non une panne transitoire) : IllegalArgumentException,
    // journalisée en ERROR avec sa pile par le relais.
    static CotisationEmise depuisJson(JsonMapper jsonMapper, String payload, String cle) {
        try {
            io.github.abdeljalil59.mutuelle.events.CotisationEmise source = jsonMapper.readValue(
                    payload, io.github.abdeljalil59.mutuelle.events.CotisationEmise.class);
            return CotisationEmise.newBuilder()
                    .setIdEvenement(source.idEvenement())
                    .setIdAdherent(source.idAdherent())
                    // 50 donne 50.00 ; 50.123 lève une ArithmeticException : aucun arrondi silencieux. Avro appliquerait
                    // la même normalisation à l'encodage (constaté, CotisationEmiseAvroTest), mais l'erreur surgirait
                    // dans send() et serait prise pour une panne transitoire : on convertit donc ici pour la classer.
                    .setMontant(source.montant().setScale(ECHELLE_MONTANT, RoundingMode.UNNECESSARY))
                    // Conservé en Instant ; l'encodage timestamp-micros tronque à la microseconde.
                    .setEmiseLe(source.emiseLe())
                    .build();
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Payload de l'outbox non convertible en Avro pour " + cle, e);
        }
    }
}
