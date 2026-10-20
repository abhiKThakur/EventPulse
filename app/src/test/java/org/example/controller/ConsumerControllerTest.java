package org.example.controller;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConsumerControllerTest {

    private ConsumerController consumerController;
    private SimpleMeterRegistry meterRegistry;
    private Counter kafkaEventsCounter;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        consumerController = new ConsumerController(meterRegistry);

        kafkaEventsCounter = meterRegistry
                .find("kafka.events.received")
                .counter();
    }

    @Test
    void shouldIncrementCounterWhenEventIsReceived() {
        consumerController.listen("Hello Kafka");

        assertEquals(1.0, kafkaEventsCounter.count());
    }

    @Test
    void shouldIncrementCounterForMultipleEvents() {
        consumerController.listen("Event 1");
        consumerController.listen("Event 2");
        consumerController.listen("Event 3");

        assertEquals(3.0, kafkaEventsCounter.count());
    }

    @Test
    void shouldStartCounterAtZero() {
        assertEquals(0.0, kafkaEventsCounter.count());
    }
}