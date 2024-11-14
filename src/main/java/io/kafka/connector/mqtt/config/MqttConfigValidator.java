//package io.kafka.connector.mqtt.config;
//
//import io.kafka.connector.mqtt.config.MqttSourceConnectorConfig;
//import org.apache.kafka.common.config.ConfigDef;
//import org.apache.kafka.common.config.ConfigException;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.Map;
//
//public class MqttConfigValidator implements ConfigDef.Validator {
//
//    private static final Logger log = LoggerFactory.getLogger(MqttConfigValidator.class);
//
//    private final MqttSourceConnectorConfig connectorConfig;
//
//    public MqttConfigValidator(MqttSourceConnectorConfig connectorConfig) {
//        this.connectorConfig = connectorConfig;
//    }
//
//    @Override
//    public void ensureValid(String name, Object value) {
//        Boolean writeToMultipleTopics = connectorConfig.getWriteDataToMultipleKakaTopics();
//        log.info(writeToMultipleTopics + "IN validateConfig");
//        if (Boolean.TRUE.equals(writeToMultipleTopics)) {
//            // Remove KAFKA_TOPIC_CONFIG if WRITE_DATA_TO_MULTIPLE_KAFKA_TOPICS is true
//            connectorConfig.valuesWithPrefixOverride()
//            props.remove(MqttSourceConnectorConfig.KAFKA_TOPIC_CONFIG);
//        }
//    }
//
//    @Override
//    public String toString() {
//        return "Custom MQTT Config Validator";
//    }
//
//}
