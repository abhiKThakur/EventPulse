package org.example.controller;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class ProducerControllerTest {

    private Producer<String, String> kafkaProducer;
    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        kafkaProducer = mock(Producer.class);

        ProducerController controller =
                new ProducerController(kafkaProducer);

        mockMvc = MockMvcBuilders
                .standaloneSetup(controller)
                .build();
    }

    @Test
    void shouldRejectEmptyEvent() throws Exception {
        mockMvc.perform(post("/producer/event")
                        .contentType(MediaType.TEXT_PLAIN)
                        .content(" "))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error")
                        .value("Event data must not be empty"));

        verify(kafkaProducer, never()).send(any(ProducerRecord.class));
    }

    @Test
    void shouldPublishValidEvent() throws Exception {
        RecordMetadata metadata = mock(RecordMetadata.class);

        @SuppressWarnings("unchecked")
        Future<RecordMetadata> future = mock(Future.class);

        when(future.get()).thenReturn(metadata);
        when(kafkaProducer.send(any(ProducerRecord.class)))
                .thenReturn(future);

        mockMvc.perform(post("/producer/event")
                        .contentType(MediaType.TEXT_PLAIN)
                        .content("Hello Kafka"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.message")
                        .value("Event published to Kafka"));

        verify(kafkaProducer).send(argThat(record ->
                record.topic().equals("testy")
                        && record.key().equals("userEvent")
                        && record.value().equals("Hello Kafka")
        ));
    }

    @Test
    void shouldReturnServiceUnavailableWhenPublishingFails()
            throws Exception {

        when(kafkaProducer.send(any(ProducerRecord.class)))
                .thenThrow(new RuntimeException("Kafka unavailable"));

        mockMvc.perform(post("/producer/event")
                        .contentType(MediaType.TEXT_PLAIN)
                        .content("Hello Kafka"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.error")
                        .value("Unable to publish event to Kafka"));
    }
}