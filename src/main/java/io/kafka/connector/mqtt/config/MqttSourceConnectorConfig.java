package io.kafka.connector.mqtt.config;

import io.kafka.connector.mqtt.validators.DependencyRecommender;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class MqttSourceConnectorConfig extends MqttConnectorConfig {

    public static final String WRITE_DATA_TO_MULTIPLE_KAFKA_TOPICS = "write.data.to.multiple.kafka.topics";
    public static final String KAFKA_TOPIC_CONFIG = "kafka.topic";
//    public static final String KAFKA_TOPIC_PREFIX_CONFIG = "kafka.topic.prefix";
    public static final String MQTT_TOPICS_CONFIG = "mqtt.topics";
    public static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";
    private static final Logger log = LoggerFactory.getLogger(MqttSourceConnectorConfig.class);

    public MqttSourceConnectorConfig(Map<String, String> originals) {
        super(conf(), originals);
    }

    public static ConfigDef conf() {
        ConfigDef baseConfigDef = MqttConnectorConfig.conf();

        return baseConfigDef
                .define(
                        WRITE_DATA_TO_MULTIPLE_KAFKA_TOPICS,
                        ConfigDef.Type.BOOLEAN,
                        ConfigDef.Importance.HIGH,
                        "Write to multiple Kafka topics",
                        "Kafka Topic Configuration",
                        1,
                        ConfigDef.Width.NONE,
                        "Write Data to Multiple Kafka Topics",
                        new DependencyRecommender())
                .define(
                        KAFKA_TOPIC_CONFIG,
                        ConfigDef.Type.STRING,
                        "",
                        ConfigDef.Importance.HIGH,
                        "Kafka Topic",
                        "Kafka Topic Configuration",
                        2,
                        ConfigDef.Width.NONE,
                        "Kafka Topic",
                        new DependencyRecommender())
//                .define(
//                        KAFKA_TOPIC_PREFIX_CONFIG,
//                        ConfigDef.Type.STRING,
//                        "",
//                        ConfigDef.Importance.HIGH,
//                        "Kafka topic prefix for creating multiple topics, " +
//                                "this prefix will be appended with the MQTT topic name. " +
//                                "MQTT topic containing characters `/`, `:` will be replaced with `.` and `_` respectively. " +
//                                "For example: Kafka Topic Prefix: `Kafka`, MQTT Topic Name: `MQTT-topic`," +
//                                "Kafka topic will be created as `Kafka-MQTT-topic`",
//                        "Kafka Topic Configuration",
//                        2,
//                        ConfigDef.Width.NONE,
//                        "Kafka Topic Prefix",
//                        new DependencyRecommender())
                .define(
                        MQTT_TOPICS_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        "MQTT Topics",
                        "MQTT Topic Configuration",
                        2,
                        ConfigDef.Width.NONE,
                        "MQTT Topic Name")
                .define(
                        BOOTSTRAP_SERVERS_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        "Bootstrap Server",
                        "Kafka Broker Configuration",
                        1,
                        ConfigDef.Width.NONE,
                        "Bootstrap Server"
                        );
    }

    public String getMqttTopics() {
        return this.getString(MQTT_TOPICS_CONFIG);
    }

    public String getKafkaTopic() {
        return this.getString(KAFKA_TOPIC_CONFIG);
    }

    public Boolean getWriteDataToMultipleKakaTopics() {
        return this.getBoolean(WRITE_DATA_TO_MULTIPLE_KAFKA_TOPICS);
    }

//    public String getKafkaTopicPrefixConfig() {
//        return this.getString(KAFKA_TOPIC_PREFIX_CONFIG);
//    }

}