package io.github.abdeljalil59.mutuelle.impaye.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class ErreursKafkaConfig {

    // Réessais à intervalle fixe (kafka.error-handler.* dans application.yml). Pas de DeadLetterPublishingRecoverer
    // (D9) : sans recoverer, DefaultErrorHandler journalise en ERREUR le message dont les réessais sont épuisés, puis
    // passe au suivant. Ce message n'est pas rejoué : limite assumée de la première version. Un message illisible
    // (DeserializationException) est fatal par défaut : journalisé et ignoré sans réessai.
    @Bean
    CommonErrorHandler gestionnaireErreursConsommation(
            @Value("${kafka.error-handler.interval-ms}") long intervalleMs,
            @Value("${kafka.error-handler.max-retries}") long reessaisMax) {
        return new DefaultErrorHandler(new FixedBackOff(intervalleMs, reessaisMax));
    }
}
