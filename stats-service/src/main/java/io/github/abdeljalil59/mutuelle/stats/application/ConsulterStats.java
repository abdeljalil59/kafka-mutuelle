package io.github.abdeljalil59.mutuelle.stats.application;

import io.github.abdeljalil59.mutuelle.stats.application.port.out.LectureStats;
import io.github.abdeljalil59.mutuelle.stats.domain.StatsAdherent;

import java.util.Map;
import java.util.Optional;

/** Cas d'usage : consulter les statistiques par adhérent. */
public class ConsulterStats {

    private final LectureStats lecture;

    public ConsulterStats(LectureStats lecture) {
        this.lecture = lecture;
    }

    public Optional<StatsAdherent> parAdherent(String idAdherent) {
        return lecture.parAdherent(idAdherent);
    }

    public Map<String, StatsAdherent> toutes() {
        return lecture.toutes();
    }
}
