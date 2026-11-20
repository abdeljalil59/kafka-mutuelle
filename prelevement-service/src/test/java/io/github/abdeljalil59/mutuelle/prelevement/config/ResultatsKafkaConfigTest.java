package io.github.abdeljalil59.mutuelle.prelevement.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.ProducerFactory;

class ResultatsKafkaConfigTest {

    private final ResultatsKafkaConfig config = new ResultatsKafkaConfig();

    @Test
    void declare_le_topic_des_resultats_avec_trois_partitions() {
        NewTopic topic = config.topicResultats("prelevements.resultats");

        assertThat(topic.name()).isEqualTo("prelevements.resultats");
        assertThat(topic.numPartitions()).isEqualTo(3);
        assertThat(topic.replicationFactor()).isEqualTo((short) 1);
    }

    // Garde-fou : le kafkaTemplate de Boot est @ConditionalOnMissingBean(KafkaTemplate) (constaté dans le bytecode de
    // KafkaAutoConfiguration). Si ce bean était déclaré KafkaTemplate, Boot ne créerait plus le sien, et le template du topic
    // de rejets et celui du RetryDemoListener (kafkaTemplate="kafkaTemplate") disparaîtraient. Le type déclaré doit rester
    // l'interface KafkaOperations.
    @Test
    void le_template_des_resultats_est_declare_avec_le_type_KafkaOperations_et_non_KafkaTemplate() throws Exception {
        Class<?> typeDeclare = ResultatsKafkaConfig.class
                .getDeclaredMethod("resultatsKafkaTemplate", ProducerFactory.class).getReturnType();

        assertThat(typeDeclare).isEqualTo(KafkaOperations.class);
    }
}
