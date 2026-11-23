package io.github.abdeljalil59.mutuelle.stats.adapter.in.rest;

import io.github.abdeljalil59.mutuelle.stats.application.ConsulterStats;
import io.github.abdeljalil59.mutuelle.stats.application.StatsIndisponibleException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

@RestController
public class StatsController {

    private final ConsulterStats consulterStats;

    public StatsController(ConsulterStats consulterStats) {
        this.consulterStats = consulterStats;
    }

    @GetMapping("/stats/{idAdherent}")
    public ResponseEntity<StatsReponse> parAdherent(@PathVariable String idAdherent) {
        return consulterStats.parAdherent(idAdherent)
                .map(StatsReponse::de)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/stats")
    public Map<String, StatsReponse> toutes() {
        Map<String, StatsReponse> reponse = new LinkedHashMap<>();
        consulterStats.toutes().forEach((id, stats) -> reponse.put(id, StatsReponse.de(stats)));
        return reponse;
    }

    @ExceptionHandler(StatsIndisponibleException.class)
    ResponseEntity<Void> indisponible() {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build();
    }
}
