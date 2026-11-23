package io.github.abdeljalil59.mutuelle.stats.application;

/** Les statistiques ne peuvent pas être lues maintenant (Kafka Streams démarre, se rééquilibre ou est arrêté). */
public class StatsIndisponibleException extends RuntimeException {

    public StatsIndisponibleException(String message, Throwable cause) {
        super(message, cause);
    }
}
