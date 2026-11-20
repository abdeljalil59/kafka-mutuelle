package io.github.abdeljalil59.mutuelle.impaye.application.port.out;

import io.github.abdeljalil59.mutuelle.impaye.domain.Impaye;

public interface PersistanceImpaye {

    // Idempotent : un impayé dont l'idEvenement est déjà enregistré est ignoré, sans exception (D11). Toute autre
    // erreur d'accès aux données remonte : elle relève des réessais du gestionnaire d'erreurs du consommateur.
    void enregistrer(Impaye impaye);
}
