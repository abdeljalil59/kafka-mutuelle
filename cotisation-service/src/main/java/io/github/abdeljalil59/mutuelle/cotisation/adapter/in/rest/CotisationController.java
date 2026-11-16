package io.github.abdeljalil59.mutuelle.cotisation.adapter.in.rest;

import io.github.abdeljalil59.mutuelle.cotisation.application.EmettreCotisation;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/cotisations")
public class CotisationController {

    private final EmettreCotisation emettreCotisation;

    public CotisationController(EmettreCotisation emettreCotisation) {
        this.emettreCotisation = emettreCotisation;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public CotisationReponse emettre(@RequestBody EmettreCotisationRequete requete) {
        return CotisationReponse.depuis(emettreCotisation.emettre(requete.idAdherent(), requete.montant()));
    }
}
