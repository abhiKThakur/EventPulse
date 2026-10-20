package org.example.controller;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ConsumerController {

    private final Counter kafkaEventsCounter;

    public ConsumerController(MeterRegistry meterRegistry) {
        this.kafkaEventsCounter = Counter.builder("kafka.events.received")
                .description("Total number of Kafka events received by the application")
                .register(meterRegistry);
    }
    @KafkaListener(topics = "testy", groupId = "metrics-consumer-group")
    public void listen(String eventData) {
        System.out.println("Received event: " + eventData);
        kafkaEventsCounter.increment();
    }
}