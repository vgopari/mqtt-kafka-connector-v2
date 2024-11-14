package io.kafka.connector.mqtt.source;

import io.kafka.connector.mqtt.VersionUtil;
import io.kafka.connector.mqtt.config.MqttConnectorConfig;
import io.kafka.connector.mqtt.config.MqttSourceConnectorConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class MqttSourceTask extends SourceTask implements MqttCallback {

    private final Logger logger = LoggerFactory.getLogger(MqttSourceTask.class);

    static final Map<String, Object> EMPTY_MAP = Map.of();
    private MqttClient mqttClient;
    private String kafkaTopic;
    boolean writeToMultipleTopics;
    private final List<SourceRecord> records = new ArrayList<>();
//    private String kafkaTopicPrefix;
    private AdminClient adminClient;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        MqttSourceConnectorConfig sourceConfig = new MqttSourceConnectorConfig(props);
        MqttConnectorConfig mqttConfig = new MqttConnectorConfig(MqttConnectorConfig.conf(), props);
//        kafkaTopicPrefix = sourceConfig.getString(MqttSourceConnectorConfig.KAFKA_TOPIC_PREFIX_CONFIG);
        List<String> mqttTopics = List.of(sourceConfig.getString(MqttSourceConnectorConfig.MQTT_TOPICS_CONFIG).split(","));
        boolean subscribeToAll = mqttConfig.getBoolean(MqttConnectorConfig.MQTT_SUBSCRIBE_ALL_CONFIG);
        writeToMultipleTopics = sourceConfig.getWriteDataToMultipleKakaTopics();

        if(!writeToMultipleTopics) {
            kafkaTopic = sourceConfig.getString(MqttSourceConnectorConfig.KAFKA_TOPIC_CONFIG);
        }

        String mqttBroker = mqttConfig.getString(MqttConnectorConfig.MQTT_BROKER_CONFIG);
        String mqttUsername = mqttConfig.getPassword(MqttConnectorConfig.MQTT_USERNAME_CONFIG).value();
        String mqttPassword = mqttConfig.getPassword(MqttConnectorConfig.MQTT_PASSWORD_CONFIG).value();

        Properties kafkaProps = new Properties();
        kafkaProps.put(MqttSourceConnectorConfig.BOOTSTRAP_SERVERS_CONFIG, sourceConfig.getString(MqttSourceConnectorConfig.BOOTSTRAP_SERVERS_CONFIG));
        adminClient = AdminClient.create(kafkaProps);


        try {
            mqttClient = new MqttClient(mqttBroker, MqttClient.generateClientId(), new MemoryPersistence());
            MqttConnectOptions options = new MqttConnectOptions();

            if (!mqttUsername.isEmpty()) {
                options.setUserName(mqttUsername);
                options.setPassword(mqttPassword.toCharArray());
            }

            mqttClient.setCallback(this);
            mqttClient.connect(options);

            if (subscribeToAll) {
                for (String topic : mqttTopics) {
                    mqttClient.subscribe(topic + "/#"); // Subscribe to all subtopics of each parent topic
                }
            } else {
                for (String topic : mqttTopics) {
                    mqttClient.subscribe(topic); // Subscribe only to specified topics
                }
            }
        } catch (MqttException e) {
            throw new ConnectException("Failed to connect to MQTT broker", e);
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        synchronized (records) {
            List<SourceRecord> temp = new ArrayList<>(records);
            records.clear();
            return temp;
        }
    }

    @Override
    public void stop() {
        try {
            if (mqttClient != null) {
                mqttClient.disconnect();
                mqttClient.close();
            }
            if (adminClient != null) {
                adminClient.close();
            }
        } catch (MqttException e) {
            logger.error("Failed to disconnect MQTT client", e);
        }
    }

    @Override
    public void connectionLost(Throwable cause) {
        logger.error("Connection to MQTT broker lost", cause);
    }

    @Override
    public void messageArrived(String topic, MqttMessage mqttMessage) throws ExecutionException, InterruptedException {

        ConnectHeaders headers = new ConnectHeaders();
        String targetKafkaTopic;
        if(writeToMultipleTopics) {
            targetKafkaTopic = sanitizeTopicName(topic);
            if (!adminClient.listTopics().names().get().contains(targetKafkaTopic)) {
                createKafkaTopicIfNotExists(targetKafkaTopic);
            }
        } else {
            targetKafkaTopic = sanitizeTopicName(kafkaTopic);
        }

        synchronized (records) {
            records.add(
                    new SourceRecord(EMPTY_MAP, EMPTY_MAP, targetKafkaTopic, null, Schema.STRING_SCHEMA,
                            topic, Schema.BYTES_SCHEMA, mqttMessage.getPayload(), System.currentTimeMillis(), headers)
            );
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        // Not needed for source connector
    }

    private void createKafkaTopicIfNotExists(String topicName) throws InterruptedException, ExecutionException {
        if (!adminClient.listTopics().names().get().contains(topicName)) {
            NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);
            adminClient.createTopics(Collections.singleton(newTopic)).all().get();
            logger.info("Created new Kafka topic: {}", topicName);
        }
    }

    private String sanitizeTopicName(String topic) {
        // Replace forward slashes with dots and semicolons with underscores
        return topic.replace("/", ".").replace(":", "_");
    }
}
