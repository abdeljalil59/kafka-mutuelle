package io.github.abdeljalil59.mutuelle.prelevement.application;

// Échec technique de la publication d'un résultat (broker injoignable, publication non confirmée à temps...). Non métier :
// elle remonte au gestionnaire d'erreurs du listener, qui réessaie puis envoie le message au topic de rejets.
public class PublicationResultatImpossibleException extends RuntimeException {

    public PublicationResultatImpossibleException(String message, Throwable cause) {
        super(message, cause);
    }
}
