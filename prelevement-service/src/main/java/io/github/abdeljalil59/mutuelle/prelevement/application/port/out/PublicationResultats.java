package io.github.abdeljalil59.mutuelle.prelevement.application.port.out;

import io.github.abdeljalil59.mutuelle.prelevement.application.PublicationResultatImpossibleException;
import io.github.abdeljalil59.mutuelle.prelevement.domain.DecisionPrelevement;

public interface PublicationResultats {

    // Publie le résultat d'une décision et n'en rend la main qu'une fois la publication confirmée. L'identifiant de
    // l'événement publié est celui de la décision : republier la même décision (message rejoué, réessai) donne le même
    // idEvenement, que le consommateur aval utilise pour dédoublonner.
    void publier(DecisionPrelevement decision) throws PublicationResultatImpossibleException;
}
