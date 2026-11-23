package io.github.abdeljalil59.mutuelle.stats.adapter.out.streams;

import io.github.abdeljalil59.mutuelle.stats.application.StatsIndisponibleException;
import io.github.abdeljalil59.mutuelle.stats.application.port.out.LectureStats;
import io.github.abdeljalil59.mutuelle.stats.domain.StatsAdherent;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Interactive Queries : lit le State Store local en lecture seule. Mono-instance : le store ne contient que les
 * partitions attribuées à cette instance (voir docs/kafka-streams.md).
 */
public class LectureStatsStateStore implements LectureStats {

    private final StreamsBuilderFactoryBean streamsFactory;
    private final String nomStore;

    public LectureStatsStateStore(StreamsBuilderFactoryBean streamsFactory, String nomStore) {
        this.streamsFactory = streamsFactory;
        this.nomStore = nomStore;
    }

    @Override
    public Optional<StatsAdherent> parAdherent(String idAdherent) {
        try {
            return Optional.ofNullable(store().get(idAdherent));
        } catch (InvalidStateStoreException e) {
            throw indisponible(e);
        }
    }

    @Override
    public Map<String, StatsAdherent> toutes() {
        Map<String, StatsAdherent> resultat = new TreeMap<>();
        try (KeyValueIterator<String, StatsAdherent> it = store().all()) {
            it.forEachRemaining(entree -> resultat.put(entree.key, entree.value));
        } catch (InvalidStateStoreException e) {
            throw indisponible(e);
        }
        return resultat;
    }

    private ReadOnlyKeyValueStore<String, StatsAdherent> store() {
        KafkaStreams streams = streamsFactory.getKafkaStreams();
        if (streams == null) {
            throw new StatsIndisponibleException("Kafka Streams n'est pas démarré", null);
        }
        return streams.store(StoreQueryParameters.fromNameAndType(nomStore, QueryableStoreTypes.keyValueStore()));
    }

    private static StatsIndisponibleException indisponible(InvalidStateStoreException e) {
        return new StatsIndisponibleException("Le state store n'est pas prêt (démarrage ou rééquilibrage)", e);
    }
}
