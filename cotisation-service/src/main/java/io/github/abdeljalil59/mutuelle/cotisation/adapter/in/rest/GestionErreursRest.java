package io.github.abdeljalil59.mutuelle.cotisation.adapter.in.rest;

import io.github.abdeljalil59.mutuelle.cotisation.application.port.out.PublicationImpossibleException;
import io.github.abdeljalil59.mutuelle.cotisation.domain.CotisationInvalideException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GestionErreursRest {

    private static final Logger LOGGER = LoggerFactory.getLogger(GestionErreursRest.class);

    @ExceptionHandler(CotisationInvalideException.class)
    ProblemDetail cotisationInvalide(CotisationInvalideException exception) {
        ProblemDetail probleme = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, exception.getMessage());
        probleme.setTitle("Cotisation invalide");
        return probleme;
    }

    @ExceptionHandler(PublicationImpossibleException.class)
    ProblemDetail publicationImpossible(PublicationImpossibleException exception) {
        LOGGER.error("Publication impossible", exception);
        // Le 503 couvre un échec constaté (message non écrit) comme un délai dépassé
        // (message peut-être déjà écrit) : le texte ne promet donc pas l'absence d'écriture.
        ProblemDetail probleme = ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE,
                "La publication n'a pas pu être confirmée ; la cotisation a peut-être été publiée.");
        probleme.setTitle("Publication impossible");
        return probleme;
    }
}
