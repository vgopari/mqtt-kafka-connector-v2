package io.kafka.connector.mqtt;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class MqttConnectorApplication {

	public static void main(String[] args) {
		SpringApplication.run(MqttConnectorApplication.class, args);
	}

}
