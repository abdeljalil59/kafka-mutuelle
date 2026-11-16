package io.github.abdeljalil59.mutuelle.prelevement.config;

import io.github.abdeljalil59.mutuelle.prelevement.application.TraiterDemandePrelevement;
import io.github.abdeljalil59.mutuelle.prelevement.domain.PlafondPrelevement;
import java.math.BigDecimal;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PrelevementConfig {

    // Pas de valeur par défaut dans le code : une propriété absente ou invalide fait échouer le démarrage.
    @Bean
    PlafondPrelevement plafondPrelevement(@Value("${prelevement.plafond}") String plafond) {
        return new PlafondPrelevement(new BigDecimal(plafond));
    }

    @Bean
    TraiterDemandePrelevement traiterDemandePrelevement(PlafondPrelevement plafond) {
        return new TraiterDemandePrelevement(plafond);
    }
}
