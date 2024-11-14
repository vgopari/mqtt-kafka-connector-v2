package io.kafka.connector.mqtt.validators;

import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static io.kafka.connector.mqtt.config.MqttConnectorConfig.MQTT_SUBSCRIBE_ALL_CONFIG;
import static io.kafka.connector.mqtt.config.MqttSourceConnectorConfig.*;

public class DependencyRecommender implements ConfigDef.Recommender {


    private static final Logger log = LoggerFactory.getLogger(DependencyRecommender.class);

    @Override
    public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
        if (name.equals(KAFKA_TOPIC_CONFIG)) {
            Boolean writeMultiple = (Boolean) parsedConfig.get(WRITE_DATA_TO_MULTIPLE_KAFKA_TOPICS);
            if (Boolean.TRUE.equals(writeMultiple)) {
                List.of();
            }
        }
        return Collections.emptyList();
    }

    @Override
    public boolean visible(String name, Map<String, Object> parsedConfig) {
        switch (name) {
            case WRITE_DATA_TO_MULTIPLE_KAFKA_TOPICS -> {
                Boolean subscribeAll = (Boolean) parsedConfig.get(MQTT_SUBSCRIBE_ALL_CONFIG);
                return subscribeAll != null && subscribeAll;
            }
            case KAFKA_TOPIC_CONFIG -> {
                Boolean subscribeAll = (Boolean) parsedConfig.get(MQTT_SUBSCRIBE_ALL_CONFIG);
                Boolean writeMultiple = (Boolean) parsedConfig.get(WRITE_DATA_TO_MULTIPLE_KAFKA_TOPICS);
//                if (subscribeAll != null && subscribeAll) {
//                    return writeMultiple != null && !writeMultiple;
//                }
                log.info("WRITEMULTIPLE" + writeMultiple);
                return (subscribeAll != null && !subscribeAll) || (writeMultiple != null && !writeMultiple);
            }
//            case KAFKA_TOPIC_PREFIX_CONFIG -> {
//                Boolean subscribeAll = (Boolean) parsedConfig.get(MQTT_SUBSCRIBE_ALL_CONFIG);
//                Boolean writeMultiple = (Boolean) parsedConfig.get(WRITE_DATA_TO_MULTIPLE_KAFKA_TOPICS);
//                return (subscribeAll != null && subscribeAll) && (writeMultiple != null && writeMultiple);
//            }
            default -> {
                return true;
            }
        }
    }
}
