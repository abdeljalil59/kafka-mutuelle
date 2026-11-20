package io.github.abdeljalil59.mutuelle.prelevement.config;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.github.abdeljalil59.mutuelle.events.ResultatPrelevement;
import io.github.abdeljalil59.mutuelle.events.avro.CotisationEmise;
import io.github.abdeljalil59.mutuelle.prelevement.domain.PrelevementInvalideException;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.util.backoff.BackOff;

@Configuration
public class ErreursKafkaConfig {

    // Le topic de rejets doit avoir au moins autant de partitions que le topic source (doc spring-kafka) :
    // même valeur que cotisations.emises.
    private static final int PARTITIONS_REJETS = 3;

    // Un rejet attend une enquête humaine : 30 jours, contre 7 jours (log.retention.hours=168, constaté sur le broker
    // du compose) pour un topic ordinaire. Passe par retention.ms, qui est en millisecondes.
    private static final Duration RETENTION_REJETS = Duration.ofDays(30);

    // Nom par défaut de spring-kafka (<topic>-dlt), dérivé du topic source : source unique pour le topic et le routage.
    private static String topicRejets(String topicSource) {
        return topicSource + "-dlt";
    }

    // La rétention n'est posée qu'à la CRÉATION du topic : spring.kafka.admin.modify-topic-configs vaut false par
    // défaut (constaté dans les métadonnées de Spring Boot), donc un topic qui existe déjà garde sa rétention. Pour
    // l'appliquer au topic existant du compose (PowerShell ou cmd, pas Git Bash qui réécrit le chemin /opt/…) :
    //   docker exec kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter
    //       --topic cotisations.emises-dlt --add-config retention.ms=2592000000
    // Relecture : mêmes options avec --describe au lieu de --alter ... --add-config.
    @Bean
    NewTopic topicRejetsCotisations(@Value("${prelevement.kafka.topic}") String topicSource) {
        return TopicBuilder.name(topicRejets(topicSource)).partitions(PARTITIONS_REJETS).replicas(1)
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(RETENTION_REJETS.toMillis()))
                .build();
    }

    // Les rejets contiennent soit les octets d'origine (désérialisation ratée), soit un CotisationEmise Avro
    // (exception dans le listener) ; les résultats publiés sont des ResultatPrelevement Avro (ResultatsKafkaConfig) :
    // une seule fabrique de producteurs pour les deux templates, sérialiseur choisi par type exact.
    // KafkaAvroSerializer lit schema.registry.url dans la configuration du producteur : la fabrique appelle configure()
    // sur le DelegatingByTypeSerializer, qui la transmet à chacun de ses délégués. Constaté par ErreursKafkaConfigTest
    // (propagation aux délégués) et par les tests d'intégration du DLT (la fabrique configure bien le sérialiseur).
    @Bean
    DefaultKafkaProducerFactoryCustomizer serialiseursRejets() {
        return usine -> {
            @SuppressWarnings("unchecked")
            DefaultKafkaProducerFactory<Object, Object> usineRejets = (DefaultKafkaProducerFactory<Object, Object>) usine;
            usineRejets.setValueSerializerSupplier(() -> new DelegatingByTypeSerializer(Map.of(
                    byte[].class, new ByteArraySerializer(),
                    CotisationEmise.class, new KafkaAvroSerializer(),
                    ResultatPrelevement.class, new KafkaAvroSerializer())));
        };
    }

    // Réessais exponentiels bornés (kafka.error-handler.* dans application.yml) : avec 1000 ms, un multiplicateur de 2 et
    // 4 réessais, les pauses sont de 1, 2, 4 puis 8 s : 5 livraisons et 15 s au plus avant le topic de rejets. Les
    // pauses bloquent le thread du consommateur (BackOffHandler par défaut) ; la doc plafonne chaque pause par ce qui
    // reste de max.poll.interval.ms. Le défaut de DefaultErrorHandler, FixedBackOff(0, 9), faisait 10 livraisons sans
    // pause : il épuisait ses réessais en millisecondes, donc une panne passagère d'Oracle envoyait tout au DLT.
    // Une DeserializationException est fatale par défaut : le message part directement au topic de rejets (1 livraison).
    // Avec Avro, les invariants du contrat ne sont plus vérifiés à la désérialisation mais par DemandePrelevement :
    // une PrelevementInvalideException est déterministe, la réessayer n'apporte rien (1 livraison aussi).
    @Bean
    CommonErrorHandler gestionnaireErreursConsommation(
            @Qualifier("kafkaTemplate") KafkaTemplate<Object, Object> kafkaTemplate,
            @Value("${prelevement.kafka.topic}") String topicSource,
            @Value("${kafka.error-handler.initial-interval}") long intervalleInitialMs,
            @Value("${kafka.error-handler.multiplier}") double multiplicateur,
            @Value("${kafka.error-handler.max-retries}") int reessaisMax) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (message, exception) -> new TopicPartition(topicRejets(topicSource), message.partition()));
        DefaultErrorHandler gestionnaire = new DefaultErrorHandler(recoverer,
                backOffDeConsommation(intervalleInitialMs, multiplicateur, reessaisMax));
        gestionnaire.addNotRetryableExceptions(PrelevementInvalideException.class);
        return gestionnaire;
    }

    // Isolé du bean pour que les tests lisent la suite des pauses sans démarrer de contexte. L'intervalle maximal
    // d'ExponentialBackOff reste à son défaut (30 s) : il ne joue pas avec 1 s x 2 sur 4 réessais.
    static BackOff backOffDeConsommation(long intervalleInitialMs, double multiplicateur, int reessaisMax) {
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(reessaisMax);
        backOff.setInitialInterval(intervalleInitialMs);
        backOff.setMultiplier(multiplicateur);
        return backOff;
    }
}
