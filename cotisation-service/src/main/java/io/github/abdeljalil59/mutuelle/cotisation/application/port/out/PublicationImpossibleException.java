package io.github.abdeljalil59.mutuelle.cotisation.application.port.out;

public class PublicationImpossibleException extends RuntimeException {

    public PublicationImpossibleException(String message, Throwable cause) {
        super(message, cause);
    }
}
