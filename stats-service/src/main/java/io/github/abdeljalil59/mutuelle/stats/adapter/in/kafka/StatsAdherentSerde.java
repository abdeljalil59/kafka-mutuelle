package io.github.abdeljalil59.mutuelle.stats.adapter.in.kafka;

import io.github.abdeljalil59.mutuelle.stats.domain.StatsAdherent;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import tools.jackson.databind.json.JsonMapper;

/** Serde JSON (Jackson) de l'agrégat, utilisé par le State Store et son topic de changelog. */
public final class StatsAdherentSerde implements Serde<StatsAdherent> {

    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    @Override
    public Serializer<StatsAdherent> serializer() {
        return (topic, stats) -> stats == null ? null : MAPPER.writeValueAsBytes(stats);
    }

    @Override
    public Deserializer<StatsAdherent> deserializer() {
        return (topic, octets) -> octets == null ? null : MAPPER.readValue(octets, StatsAdherent.class);
    }
}
