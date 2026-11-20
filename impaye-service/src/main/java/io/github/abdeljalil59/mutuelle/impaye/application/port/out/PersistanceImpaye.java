package io.github.abdeljalil59.mutuelle.impaye.application.port.out;

import io.github.abdeljalil59.mutuelle.impaye.domain.Impaye;

public interface PersistanceImpaye {

    // Idempotent : un impayé dont l'idEvenement est déjà enregistré est ignoré, sans exception (D11). Rend true si
    // l'impayé vient d'être enregistré, false si c'était un doublon. Le cas d'usage ne lit pas ce résultat : la relance se
    // décide sur l'état en base, y compris pour un message rejoué (voir TraiterRejet). Toute autre erreur d'accès aux
    // données remonte : elle relève des réessais du gestionnaire d'erreurs du consommateur.
    boolean enregistrer(Impaye impaye);

    // Nombre d'impayés enregistrés pour cet adhérent. Un doublon n'ajoute pas de ligne : il ne change pas ce nombre.
    long compterParAdherent(String idAdherent);

    // Vrai si une relance a déjà été émise pour cet adhérent (au moins un de ses impayés est marqué).
    boolean relanceEmise(String idAdherent);

    // Marque tous les impayés enregistrés de cet adhérent : sa relance est émise. À appeler APRÈS la publication.
    void marquerRelanceEmise(String idAdherent);
}
