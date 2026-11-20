package io.github.abdeljalil59.mutuelle.impaye.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.serializer.DeserializationException;

class ErreursKafkaConfigTest {

    // Intervalle de 1 ms : ces tests comptent des livraisons, pas des secondes.
    private final DefaultErrorHandler gestionnaire =
            (DefaultErrorHandler) new ErreursKafkaConfig().gestionnaireErreursConsommation(1L, 3L);

    // Une livraison échouée = un handleOne ; il rend true quand le message est abandonné (plus aucun réessai).
    @SuppressWarnings("unchecked")
    private int livraisonsJusquALAbandon(Exception exception) {
        ConsumerRecord<String, String> message = new ConsumerRecord<>("prelevements.resultats", 0, 5L, "adh1", "valeur");
        int livraisons = 0;
        boolean abandonne = false;
        while (!abandonne && livraisons < 20) {
            livraisons++;
            abandonne = gestionnaire.handleOne(exception, message, mock(Consumer.class), mock(MessageListenerContainer.class));
        }
        return livraisons;
    }

    @Test
    void reessaie_trois_fois_puis_abandonne_le_message_sans_lever_d_exception() {
        int livraisons = livraisonsJusquALAbandon(new ListenerExecutionFailedException(
                "échec du listener", new IllegalStateException("base indisponible")));

        // 1 livraison initiale + 3 réessais.
        assertThat(livraisons).isEqualTo(4);
    }

    @Test
    void ne_rejoue_pas_un_message_illisible() {
        int livraisons = livraisonsJusquALAbandon(new ListenerExecutionFailedException("échec du listener",
                new DeserializationException("failed to deserialize", new byte[] {1, 2, 3}, false,
                        new IllegalStateException("octet magique inconnu"))));

        assertThat(livraisons).isEqualTo(1);
    }
}
