package org.example.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class KafkaConsumerConfigTest {

    private final KafkaConsumerConfig config = new KafkaConsumerConfig();

    @Test
    void shouldCreateErrorHandler() {
        DefaultErrorHandler errorHandler = config.kafkaErrorHandler();

        assertNotNull(errorHandler);
    }

    @Test
    void shouldCreateListenerContainerFactory() {
        var consumerFactory = org.mockito.Mockito
                .mock(org.springframework.kafka.core.ConsumerFactory.class);

        DefaultErrorHandler errorHandler = config.kafkaErrorHandler();

        var factory = config.kafkaListenerContainerFactory(
                consumerFactory, errorHandler);

        assertNotNull(factory);
    }
}