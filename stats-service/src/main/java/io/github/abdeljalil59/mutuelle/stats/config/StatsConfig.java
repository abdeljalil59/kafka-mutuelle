package io.github.abdeljalil59.mutuelle.stats.config;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.github.abdeljalil59.mutuelle.events.ResultatPrelevement;
import io.github.abdeljalil59.mutuelle.stats.adapter.in.kafka.StatsAdherentSerde;
import io.github.abdeljalil59.mutuelle.stats.adapter.in.kafka.StatsTopology;
import io.github.abdeljalil59.mutuelle.stats.adapter.out.streams.LectureStatsStateStore;
import io.github.abdeljalil59.mutuelle.stats.application.ConsulterStats;
import io.github.abdeljalil59.mutuelle.stats.application.port.out.LectureStats;
import io.github.abdeljalil59.mutuelle.stats.domain.StatsAdherent;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import java.util.Map;

@Configuration
public class StatsConfig {

    @Bean
    Serde<ResultatPrelevement> resultatSerde(
            @Value("${spring.kafka.streams.properties.schema.registry.url}") String urlRegistre) {
        SpecificAvroSerde<ResultatPrelevement> serde = new SpecificAvroSerde<>();
        serde.configure(Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, urlRegistre), false);
        return serde;
    }

    @Bean
    StatsTopology statsTopology(@Value("${stats.topic}") String topic, @Value("${stats.store}") String store,
                                Serde<ResultatPrelevement> resultatSerde) {
        return new StatsTopology(topic, store, resultatSerde, new StatsAdherentSerde());
    }

    /** Déclarer la KTable dans le StreamsBuilder suffit à enregistrer la topology avant le démarrage de Streams. */
    @Bean
    KTable<String, StatsAdherent> statsParAdherent(StreamsBuilder builder, StatsTopology topology) {
        return topology.definir(builder);
    }

    @Bean
    LectureStats lectureStats(StreamsBuilderFactoryBean streamsFactory, @Value("${stats.store}") String store) {
        return new LectureStatsStateStore(streamsFactory, store);
    }

    @Bean
    ConsulterStats consulterStats(LectureStats lectureStats) {
        return new ConsulterStats(lectureStats);
    }
}
