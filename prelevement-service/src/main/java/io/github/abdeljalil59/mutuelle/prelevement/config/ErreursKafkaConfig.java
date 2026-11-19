package io.github.abdeljalil59.mutuelle.prelevement.config;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.github.abdeljalil59.mutuelle.events.avro.CotisationEmise;
import io.github.abdeljalil59.mutuelle.prelevement.domain.PrelevementInvalideException;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
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
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;

@Configuration
public class ErreursKafkaConfig {

    // Le topic de rejets doit avoir au moins autant de partitions que le topic source (doc spring-kafka) :
    // même valeur que cotisations.emises.
    private static final int PARTITIONS_REJETS = 3;

    // Nom par défaut de spring-kafka (<topic>-dlt), dérivé du topic source : source unique pour le topic et le routage.
    private static String topicRejets(String topicSource) {
        return topicSource + "-dlt";
    }

    @Bean
    NewTopic topicRejetsCotisations(@Value("${prelevement.kafka.topic}") String topicSource) {
        return TopicBuilder.name(topicRejets(topicSource)).partitions(PARTITIONS_REJETS).replicas(1).build();
    }

    // Les rejets contiennent soit les octets d'origine (désérialisation ratée), soit un CotisationEmise Avro
    // (exception dans le listener) : un seul KafkaTemplate, sérialiseur choisi par type exact.
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
                    CotisationEmise.class, new KafkaAvroSerializer())));
        };
    }

    // BackOff par défaut de DefaultErrorHandler (FixedBackOff(0, 9) : 10 livraisons sans pause) pour les exceptions
    // non fatales, à revoir quand le traitement écrira en base. Une DeserializationException est fatale par défaut :
    // le message part directement au topic de rejets, sans nouvelle tentative.
    // Avec Avro, les invariants du contrat ne sont plus vérifiés à la désérialisation mais par DemandePrelevement :
    // une PrelevementInvalideException est déterministe, la réessayer n'apporte rien (même traitement qu'avant).
    @Bean
    CommonErrorHandler gestionnaireErreursConsommation(
            KafkaTemplate<Object, Object> kafkaTemplate, @Value("${prelevement.kafka.topic}") String topicSource) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (message, exception) -> new TopicPartition(topicRejets(topicSource), message.partition()));
        DefaultErrorHandler gestionnaire = new DefaultErrorHandler(recoverer);
        gestionnaire.addNotRetryableExceptions(PrelevementInvalideException.class);
        return gestionnaire;
    }
}
