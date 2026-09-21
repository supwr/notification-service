package com.notificationservice.api.infrastructure.messaging.consumer;

import com.notificationservice.api.application.usecase.send.SendAppointmentNotificationUseCase;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@SpringBootTest(properties = {
    "spring.main.web-application-type=none",
    "spring.kafka.consumer.auto-offset-reset=earliest",
    "spring.kafka.consumer.group-id=notification-service-test",
    "spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer",
    "spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer",
    "app.kafka.retry.attempts=4"
})
@EmbeddedKafka(
    partitions = 1,
    topics = {
        "scheduling.appointment.notification",
        "scheduling.appointment.notification-retry",
        "scheduling.appointment.notification-dlq"
    },
    bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
@ActiveProfiles("worker")
@Import(AppointmentNotificationConsumerRetryIntegrationTest.MockConfig.class)
class AppointmentNotificationConsumerRetryIntegrationTest {

    private static final String MAIN_TOPIC = "scheduling.appointment.notification";
    private static final String DLQ_TOPIC = MAIN_TOPIC + "-dlq";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private SendAppointmentNotificationUseCase useCase;

    @Test
    void shouldSendMessageToDlqAfterRetryExhaustion() throws Exception {
        doThrow(new IllegalStateException("boom")).when(useCase).execute(any());

        kafkaTemplate.send(recordWithHeaders(payload())).get(10, TimeUnit.SECONDS);

        verify(useCase, timeout(15_000).times(4)).execute(any());

        try (Consumer<String, String> consumer = createConsumer()) {
            embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, DLQ_TOPIC);
            final var records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10));
            assertFalse(records.isEmpty());
            final ConsumerRecord<String, String> record = records.iterator().next();
            assertEquals(payload(), record.value());
        }
    }

    private Consumer<String, String> createConsumer() {
        final Map<String, Object> props = KafkaTestUtils.consumerProps(
            "notification-service-dlq-checker",
            "true",
            embeddedKafkaBroker
        );
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new StringDeserializer()).createConsumer();
    }

    private ProducerRecord<String, String> recordWithHeaders(final String payload) {
        final ProducerRecord<String, String> record = new ProducerRecord<>(MAIN_TOPIC, null, payload);
        record.headers().add(new RecordHeader("X-Idempotency-Key", "8bc1e803-32a0-4e84-844b-9e061f93db47".getBytes(UTF_8)));
        record.headers().add(new RecordHeader("X-Source-Service", "scheduling-service".getBytes(UTF_8)));
        return record;
    }

    private String payload() {
        return """
            {
              "appointmentId": "8bc1e803-32a0-4e84-844b-9e061f93db47",
              "patientId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
              "doctorId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
              "fullname": "marcelo",
              "email": "patient@example.com",
              "body": "Um novo agendamento foi feito para 18/09/2026 às 21:22.",
              "appointmentDateTime": "2026-09-18 21:22:25.894 +0000"
            }
            """;
    }

    @TestConfiguration
    static class MockConfig {

        @Bean
        @Primary
        SendAppointmentNotificationUseCase useCase() {
            return mock(SendAppointmentNotificationUseCase.class);
        }
    }
}
