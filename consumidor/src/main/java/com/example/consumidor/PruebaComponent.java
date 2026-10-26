package com.example.consumidor;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class PruebaComponent {

	@KafkaListener(topics = {
			"prueba",
			"boyas_inteligente",
			"temperatura_camion",
			"tutorial-particiones"
	}, groupId = "prueba-group-1")
	public void listen(
			@Payload String message,
			@Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
			@Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
			@Header(KafkaHeaders.OFFSET) long offset,
			@Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String key) {
		System.out.println("\n" + "=".repeat(100));
		System.out.println("MENSAJE RECIBIDO");
		System.out.printf("Topic: %s%n", topic);
		System.out.printf("Partición: %d%n", partition);
		System.out.printf("Offset: %d%n", offset);
		System.out.printf("Key: %s%n", key != null ? key : "(sin key)");
		System.out.printf("Mensaje: %s%n", message);
		System.out.println("=".repeat(100) + "\n");
	}
}
