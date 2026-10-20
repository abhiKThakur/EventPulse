package org.example.controller;

import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.Properties;

@RestController
@RequestMapping("/producer")
public class ProducerController {

    private static final String TOPIC_NAME = "testy";
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

    private final Producer<String, String> kafkaProducer;

    public ProducerController() {
        this(createKafkaProducer());
    }

    ProducerController(Producer<String, String> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    private static Producer<String, String> createKafkaProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<>(props);
    }

    @PostMapping("/event")
    public ResponseEntity<Map<String, String>> sendEventToKafka(
            @RequestBody String eventData) {

        if (eventData == null || eventData.isBlank()) {
            return ResponseEntity.badRequest()
                    .body(Map.of("error", "Event data must not be empty"));
        }

        ProducerRecord<String, String> record =
                new ProducerRecord<>(TOPIC_NAME, "userEvent", eventData);

        try {
            kafkaProducer.send(record).get();

            return ResponseEntity.status(HttpStatus.ACCEPTED)
                    .body(Map.of("message", "Event published to Kafka"));

        } catch (Exception exception) {
            System.err.println(
                    "Error sending message to Kafka: "
                            + exception.getMessage());

            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("error", "Unable to publish event to Kafka"));
        }
    }

    @PreDestroy
    public void closeProducer() {
        kafkaProducer.close();
    }
}