package io.github.abdeljalil59.mutuelle.stats.adapter.in.kafka;

import io.github.abdeljalil59.mutuelle.stats.domain.StatsAdherent;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class StatsAdherentSerdeTest {

    private final StatsAdherentSerde serde = new StatsAdherentSerde();

    @Test
    void allerRetourJsonConserveToutesLesValeurs() {
        StatsAdherent stats = new StatsAdherent(2, 1, 235.0, 1_700_000_000_000L);

        byte[] octets = serde.serializer().serialize("t", stats);

        assertThat(serde.deserializer().deserialize("t", octets)).isEqualTo(stats);
    }

    @Test
    void leJsonExposeLesQuatreChampsDeLaConsigne() {
        byte[] octets = serde.serializer().serialize("t", new StatsAdherent(1, 2, 3.5, 4L));

        assertThat(new String(octets, StandardCharsets.UTF_8))
                .contains("\"nombreAcceptes\":1", "\"nombreRejetes\":2", "\"montantTotal\":3.5",
                        "\"dernierEvenement\":4");
    }

    @Test
    void nullResteNull() {
        assertThat(serde.serializer().serialize("t", null)).isNull();
        assertThat(serde.deserializer().deserialize("t", null)).isNull();
    }
}
