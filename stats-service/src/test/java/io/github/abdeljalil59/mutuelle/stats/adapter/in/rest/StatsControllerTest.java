package io.github.abdeljalil59.mutuelle.stats.adapter.in.rest;

import io.github.abdeljalil59.mutuelle.stats.application.ConsulterStats;
import io.github.abdeljalil59.mutuelle.stats.application.StatsIndisponibleException;
import io.github.abdeljalil59.mutuelle.stats.domain.StatsAdherent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Contrôleur seul (MockMvc autonome) : le cas d'usage est simulé, aucun broker ni contexte Spring. */
class StatsControllerTest {

    private final ConsulterStats consulterStats = mock(ConsulterStats.class);
    private MockMvc mvc;

    @BeforeEach
    void preparer() {
        mvc = MockMvcBuilders.standaloneSetup(new StatsController(consulterStats)).build();
    }

    @Test
    void renvoieLesStatsDUnAdherentConnu() throws Exception {
        when(consulterStats.parAdherent("ADH-1"))
                .thenReturn(Optional.of(new StatsAdherent(2, 1, 235.0, 1_700_000_000_000L)));

        mvc.perform(get("/stats/ADH-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.nombreAcceptes").value(2))
                .andExpect(jsonPath("$.nombreRejetes").value(1))
                .andExpect(jsonPath("$.montantTotal").value(235.0))
                .andExpect(jsonPath("$.dernierEvenement").value(1_700_000_000_000L));
    }

    @Test
    void renvoie404PourUnAdherentInconnu() throws Exception {
        when(consulterStats.parAdherent("ADH-X")).thenReturn(Optional.empty());

        mvc.perform(get("/stats/ADH-X")).andExpect(status().isNotFound());
    }

    @Test
    void listeTousLesAdherents() throws Exception {
        Map<String, StatsAdherent> toutes = new TreeMap<>();
        toutes.put("ADH-1", new StatsAdherent(1, 0, 10.0, 1L));
        toutes.put("ADH-2", new StatsAdherent(0, 1, 20.0, 2L));
        when(consulterStats.toutes()).thenReturn(toutes);

        mvc.perform(get("/stats"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$['ADH-1'].nombreAcceptes").value(1))
                .andExpect(jsonPath("$['ADH-2'].nombreRejetes").value(1));
    }

    @Test
    void listeVideQuandAucunAdherent() throws Exception {
        when(consulterStats.toutes()).thenReturn(Map.of());

        mvc.perform(get("/stats")).andExpect(status().isOk()).andExpect(jsonPath("$").isEmpty());
    }

    @Test
    void renvoie503QuandLeStoreNEstPasPret() throws Exception {
        when(consulterStats.parAdherent("ADH-1")).thenThrow(new StatsIndisponibleException("pas prêt", null));

        mvc.perform(get("/stats/ADH-1")).andExpect(status().isServiceUnavailable());
    }
}
