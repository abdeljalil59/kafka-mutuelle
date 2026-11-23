package io.github.abdeljalil59.mutuelle.stats.application;

import io.github.abdeljalil59.mutuelle.stats.application.port.out.LectureStats;
import io.github.abdeljalil59.mutuelle.stats.domain.StatsAdherent;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ConsulterStatsTest {

    private final LectureStats lecture = mock(LectureStats.class);
    private final ConsulterStats consulterStats = new ConsulterStats(lecture);

    @Test
    void renvoieLesStatsDeLAdherentLuesDansLePort() {
        StatsAdherent stats = new StatsAdherent(1, 0, 10.0, 5L);
        when(lecture.parAdherent("ADH-1")).thenReturn(Optional.of(stats));

        assertThat(consulterStats.parAdherent("ADH-1")).contains(stats);
    }

    @Test
    void renvoieVideQuandLAdherentEstInconnu() {
        when(lecture.parAdherent("ADH-X")).thenReturn(Optional.empty());

        assertThat(consulterStats.parAdherent("ADH-X")).isEmpty();
    }

    @Test
    void listeToutesLesStatsDuPort() {
        Map<String, StatsAdherent> toutes = Map.of("ADH-1", StatsAdherent.vide());
        when(lecture.toutes()).thenReturn(toutes);

        assertThat(consulterStats.toutes()).isEqualTo(toutes);
    }
}
