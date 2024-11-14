package io.kafka.connector.mqtt.config;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;

public class MqttBrokerValidator implements ConfigDef.Validator {
    private static final Logger logger = LoggerFactory.getLogger(MqttBrokerValidator.class);

    private final MqttConnectorConfig connectorConfig;

    public MqttBrokerValidator(MqttConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    @Override
    public void ensureValid(String name, Object value) {
        if (value instanceof String brokerUri) {
            if(!brokerUri.isEmpty()) {
                try {
                    // Validate URI
                    URI uri = new URI(brokerUri);
                    validateUriScheme(uri);
                    validateUriHost(uri);

                    // Check MQTT broker connection
                    if (!testMqttConnection(brokerUri)) {
                        throw new ConfigException(name, value, "Unable to connect to MQTT broker");
                    }
                } catch (URISyntaxException e) {
                    throw new ConfigException(name, value, "Invalid URI format");
                } catch (MqttException e) {
                    throw new ConfigException(name, value, "Error during MQTT connection test");
                }
            }
        } else {
            throw new ConfigException(name, value, "Invalid type for broker URI, expected a string");
        }
    }

    private void validateUriScheme(URI uri) {
        String scheme = uri.getScheme();
        if (scheme == null || (!scheme.equals("tcp") && !scheme.equals("mqtts"))) {
            throw new ConfigException("broker.uri", uri.toString(), "Invalid URI scheme, expected tcp or mqtts");
        }
    }

    private void validateUriHost(URI uri) {
        if (uri.getHost() == null) {
            throw new ConfigException("broker.uri", uri.toString(), "Invalid URI, host is missing");
        }
    }

    public boolean testMqttConnection(String brokerUri) throws MqttException {
        try (AutoCloseableMqttClient client = new AutoCloseableMqttClient(brokerUri)) {
            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(true);
            if(connectorConfig!=null) {
                options.setUserName(connectorConfig.getUsername());
                options.setPassword(connectorConfig.getPassword().toCharArray());
            }
            client.getClient().connect(options);
            return true; // Connection was successful
        } catch (MqttException e) {
            logger.error("MQTT connection failed", e);
            throw new InvalidConfigurationException(e.getMessage() + ": " + brokerUri);
        }
    }

    private static class AutoCloseableMqttClient implements AutoCloseable {
        private final MqttClient client;

        public AutoCloseableMqttClient(String brokerUri) throws MqttException {
            this.client = new MqttClient(brokerUri, MqttClient.generateClientId());
        }

        public MqttClient getClient() {
            return client;
        }

        @Override
        public void close() {
            try {
                if (client.isConnected()) {
                    client.disconnect();
                }
                client.close();
            } catch (MqttException e) {
                logger.error("Error during MQTT client disconnect/close", e);
            }
        }
    }

    @Override
    public String toString() {
        return "Valid MQTT broker URI starting with tcp:// or mqtts://";
    }
}
