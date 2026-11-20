package io.github.abdeljalil59.mutuelle.impaye.application.port.out;

import io.github.abdeljalil59.mutuelle.impaye.domain.Impaye;

public interface PersistanceImpaye {

    // Idempotent : un impayé dont l'idEvenement est déjà enregistré est ignoré, sans exception (D11). Rend true si
    // l'impayé vient d'être enregistré, false si c'était un doublon : le cas d'usage s'en sert pour ne pas relancer deux
    // fois sur un résultat rejoué. Toute autre erreur d'accès aux données remonte : elle relève des réessais du
    // gestionnaire d'erreurs du consommateur.
    boolean enregistrer(Impaye impaye);

    // Nombre d'impayés enregistrés pour cet adhérent. Un doublon n'ajoute pas de ligne : il ne change pas ce nombre.
    long compterParAdherent(String idAdherent);
}
