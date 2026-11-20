package io.github.abdeljalil59.mutuelle.prelevement.adapter.in.kafka;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.kafka.listener.RetryListener;

// RetryListener de test, posé sur le DefaultAfterRollbackProcessor du contexte (setRetryListeners) : aucun changement du code de
// production. Compte par clé de message les livraisons échouées et les récupérations (envois au topic de rejets).
// Constaté (ErreursKafkaConfigTest) : failedDelivery est notifié à chaque livraison échouée, y compris la dernière,
// donc le nombre de livraisons d'un message rejeté est son nombre de failedDelivery.
class CompteurDeLivraisons implements RetryListener {

    private final Map<String, Integer> echecsParCle = new ConcurrentHashMap<>();
    private final Map<String, Integer> recuperationsParCle = new ConcurrentHashMap<>();
    private final AtomicInteger notificationsParLot = new AtomicInteger();

    @Override
    public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
        echecsParCle.merge(String.valueOf(record.key()), 1, Integer::sum);
    }

    @Override
    public void recovered(ConsumerRecord<?, ?> record, Exception ex) {
        recuperationsParCle.merge(String.valueOf(record.key()), 1, Integer::sum);
    }

    // Variante par lot de RetryListener, notifiée par un autre chemin de spring-kafka que la gestion par record.
    @Override
    public void failedDelivery(ConsumerRecords<?, ?> records, Exception ex, int deliveryAttempt) {
        notificationsParLot.incrementAndGet();
    }

    int livraisonsEchouees(String cle) {
        return echecsParCle.getOrDefault(cle, 0);
    }

    int envoisAuTopicDeRejets(String cle) {
        return recuperationsParCle.getOrDefault(cle, 0);
    }

    int notificationsParLot() {
        return notificationsParLot.get();
    }

    @Override
    public String toString() {
        return "livraisons échouées=" + echecsParCle + ", récupérations=" + recuperationsParCle
                + ", notifications par lot=" + notificationsParLot.get();
    }
}
