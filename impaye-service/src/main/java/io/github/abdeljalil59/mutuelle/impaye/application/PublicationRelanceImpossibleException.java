package io.github.abdeljalil59.mutuelle.impaye.application;

// Erreur technique : le broker n'a pas confirmé la relance. Elle remonte au gestionnaire d'erreurs du consommateur.
public class PublicationRelanceImpossibleException extends RuntimeException {

    public PublicationRelanceImpossibleException(String message, Throwable cause) {
        super(message, cause);
    }
}
