package io.github.abdeljalil59.mutuelle.stats.config;

import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class StreamsConfigTest {

    /** Prouve que Kafka Streams accepte et transmet le préfixe « consumer. » utilisé dans application.yml. */
    @Test
    void leConsommateurPrincipalLitEnReadCommitted() {
        StreamsConfig config = new StreamsConfig(Map.of(
                StreamsConfig.APPLICATION_ID_CONFIG, "stats-service",
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                "consumer.isolation.level", "read_committed"));

        Map<String, Object> consommateur = config.getMainConsumerConfigs("stats-service", "client", 0);

        assertThat(consommateur).containsEntry("isolation.level", "read_committed");
    }
}
