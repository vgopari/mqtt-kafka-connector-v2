package io.kafka.connector.mqtt.source;

import io.kafka.connector.mqtt.VersionUtil;
import io.kafka.connector.mqtt.config.MqttBrokerValidator;
import io.kafka.connector.mqtt.config.MqttSourceConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class MqttSourceConnectorV2 extends SourceConnector {

    private final Logger logger = LoggerFactory.getLogger(MqttSourceConnectorV2.class);

    private MqttSourceConnectorConfig config;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        logger.info("props.get(MqttSourceConnectorConfig.WRITE_DATA_TO_MULTIPLE_KAFKA_TOPICS)" + props.get(MqttSourceConnectorConfig.WRITE_DATA_TO_MULTIPLE_KAFKA_TOPICS));
        this.config = new MqttSourceConnectorConfig(props);
        validateMqttConnection();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MqttSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>(maxTasks);

        // Obtain the MQTT topics and Kafka topic configuration
        String mqttTopicsConfig = config.getMqttTopics(); // MQTT topics
        String kafkaTopicConfig = config.getKafkaTopic(); // Kafka topic
        boolean writeToMultipleTopics = config.getWriteDataToMultipleKakaTopics();
//        String kafkaTopicPrefix = config.getKafkaTopicPrefixConfig();

        // Split MQTT topics and partition them across the available tasks
        List<String> mqttTopics = Arrays.asList(mqttTopicsConfig.split(","));
        List<List<String>> partitions = ConnectorUtils.groupPartitions(mqttTopics, maxTasks);

        for (int i = 0; i < partitions.size(); i++) {
            Map<String, String> taskConfig = new LinkedHashMap<>(config.originalsStrings());
            List<String> subtopics = partitions.get(i);

            // Assign the subset of MQTT topics to each task
            taskConfig.put(MqttSourceConnectorConfig.MQTT_TOPICS_CONFIG, String.join(",", subtopics));

            // Assign the Kafka topic to each task
            if (writeToMultipleTopics) {
                logger.info("TRUEUtur");
                taskConfig.put(MqttSourceConnectorConfig.KAFKA_TOPIC_CONFIG,   "" + i);
            } else {
                taskConfig.put(MqttSourceConnectorConfig.KAFKA_TOPIC_CONFIG, kafkaTopicConfig);
            }

            // Assign a unique task ID
            taskConfig.put("task.id", Integer.toString(i));

            configs.add(taskConfig);
        }

        return configs;
    }

    @Override
    public void stop() {
        // Do nothing
    }

    @Override
    public ConfigDef config() {
        return MqttSourceConnectorConfig.conf();
    }

    private void validateMqttConnection() {
        try {
            MqttBrokerValidator validator = new MqttBrokerValidator(config);
            String brokerUri = config.getMqttBrokerConfig();
            if (!validator.testMqttConnection(brokerUri)) {
                throw new ConfigException("MQTT connection validation failed");
            }
        } catch (Exception e) {
            logger.error("MQTT connection validation failed", e);
            throw new ConfigException("MQTT connection validation failed", e);
        }
    }

}
