package com.example.productor;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/kafka")
public class ProductorController {

	private static final String TOPIC_NAME = "tutorial-particiones";

	private final KafkaTemplate<String, String> kafkaTemplate;

	public ProductorController(KafkaTemplate<String, String> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}

	@GetMapping("/send")
	public String sendMessage(
			@RequestParam String message,
			@RequestParam(required = false) String key) {
		if (key != null && !key.isEmpty()) {
			kafkaTemplate.send(TOPIC_NAME, key, message);
			return "Mensaje enviado al topic '" + TOPIC_NAME
					+ "' con KEY: " + key + " | MENSAJE: " + message;
		}

		kafkaTemplate.send(TOPIC_NAME, message);
		return "Mensaje enviado al topic '" + TOPIC_NAME
				+ "' (sin key - distribución automática) | MENSAJE: " + message;
	}

	@GetMapping("/health")
	public String health() {
		return "Aplicación de Kafka Tutorial está corriendo";
	}
}
