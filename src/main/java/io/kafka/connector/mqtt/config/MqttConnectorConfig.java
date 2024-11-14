package io.kafka.connector.mqtt.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;

import javax.net.ssl.SSLContext;
import javax.xml.validation.Validator;
import java.util.List;
import java.util.Map;

import static io.kafka.connector.mqtt.config.MqttSourceConnectorConfig.WRITE_DATA_TO_MULTIPLE_KAFKA_TOPICS;

public class MqttConnectorConfig extends AbstractConfig {

    public static final String MQTT_BROKER_CONFIG = "mqtt.broker";
    public static final String MQTT_USERNAME_CONFIG = "mqtt.username";
    public static final String MQTT_PASSWORD_CONFIG = "mqtt.password";
    public static final String MQTT_SUBSCRIBE_ALL_CONFIG = "mqtt.subscribe.all";

    public MqttConnectorConfig(ConfigDef definition, Map<?, ?> originals) {
        super(definition, originals);
    }

    public static ConfigDef conf() {
        return new ConfigDef()
                .define(MQTT_BROKER_CONFIG,
                        ConfigDef.Type.STRING,
                        "",
                        new MqttBrokerValidator(null),
                        ConfigDef.Importance.HIGH,
                        "MQTT Broker URI",
                        "MQTT Topic Configuration",
                        1,
                        ConfigDef.Width.NONE,
                        "MQTT Broker URI")
                .define(MQTT_USERNAME_CONFIG,
                        ConfigDef.Type.PASSWORD,
                        "",
                        ConfigDef.Importance.HIGH,
                        "MQTT Username",
                        "MQTT Topic Configuration",
                        3,
                        ConfigDef.Width.NONE,
                        "MQTT username")
                .define(MQTT_PASSWORD_CONFIG,
                        ConfigDef.Type.PASSWORD,
                        "",
                        ConfigDef.Importance.HIGH,
                        "MQTT Password",
                        "MQTT Topic Configuration",
                        4,
                        ConfigDef.Width.NONE,
                        "MQTT password")
                .define(MQTT_SUBSCRIBE_ALL_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        ConfigDef.Importance.MEDIUM,
                        "Subscribe to all subtopics",
                        "MQTT Topic Configuration",
                        5,
                        ConfigDef.Width.NONE,
                        "Subscribe to all MQTT subtopics?");
    }

    public String getMqttBrokerConfig() {
        return this.getString(MQTT_BROKER_CONFIG);
    }

    public String getUsername() {
        return this.getPassword(MQTT_USERNAME_CONFIG).value();
    }

    public String getPassword() {
        return this.getPassword(MQTT_PASSWORD_CONFIG).value();
    }
}
