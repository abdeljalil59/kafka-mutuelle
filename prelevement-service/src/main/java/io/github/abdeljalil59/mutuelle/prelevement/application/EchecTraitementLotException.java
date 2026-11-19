package io.github.abdeljalil59.mutuelle.prelevement.application;

// Échec technique de l'écriture d'un lot : index de la première demande qui n'a pas pu être enregistrée.
// Les demandes qui la précèdent l'ont été. L'adaptateur d'entrée en tire l'index à signaler au broker.
public class EchecTraitementLotException extends RuntimeException {

    private final int index;

    public EchecTraitementLotException(int index, Throwable cause) {
        super("échec de l'enregistrement de la demande d'index " + index, cause);
        this.index = index;
    }

    public int index() {
        return index;
    }
}
