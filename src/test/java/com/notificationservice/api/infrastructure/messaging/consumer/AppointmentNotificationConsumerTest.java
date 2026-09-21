package com.notificationservice.api.infrastructure.messaging.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.notificationservice.api.application.usecase.send.SendAppointmentNotificationUseCase;
import com.notificationservice.api.domain.model.AppointmentNotification;
import com.notificationservice.api.infrastructure.messaging.mapper.NotificationEventMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class AppointmentNotificationConsumerTest {

    @Test
    void shouldDeserializeEventAndDelegateToUseCase() throws Exception {
        final SendAppointmentNotificationUseCase useCase = mock(SendAppointmentNotificationUseCase.class);
        final AppointmentNotificationConsumer consumer = new AppointmentNotificationConsumer(
            new ObjectMapper().findAndRegisterModules(),
            new NotificationEventMapper(),
            useCase
        );

        consumer.listen(recordWithHeaders(validPayload()));

        final ArgumentCaptor<AppointmentNotification> captor = ArgumentCaptor.forClass(AppointmentNotification.class);
        verify(useCase, times(1)).execute(captor.capture());
        assertEquals("patient@example.com", captor.getValue().email());
        assertEquals("8bc1e803-32a0-4e84-844b-9e061f93db47", captor.getValue().appointmentId().toString());
        assertEquals("8bc1e803-32a0-4e84-844b-9e061f93db47", captor.getValue().idempotencyKey());
        assertEquals("scheduling-service", captor.getValue().sourceService());
    }

    @Test
    void shouldRejectEventWithoutRequiredHeaders() {
        final SendAppointmentNotificationUseCase useCase = mock(SendAppointmentNotificationUseCase.class);
        final AppointmentNotificationConsumer consumer = new AppointmentNotificationConsumer(
            new ObjectMapper().findAndRegisterModules(),
            new NotificationEventMapper(),
            useCase
        );

        final ConsumerRecord<String, String> record = new ConsumerRecord<>(
            "scheduling.appointment.notification",
            0,
            0L,
            "key",
            validPayload()
        );

        assertThrows(IllegalArgumentException.class, () -> consumer.listen(record));
    }

    private ConsumerRecord<String, String> recordWithHeaders(final String payload) {
        final ConsumerRecord<String, String> record = new ConsumerRecord<>(
            "scheduling.appointment.notification",
            0,
            0L,
            "key",
            payload
        );
        record.headers().add(new RecordHeader("X-Idempotency-Key", "8bc1e803-32a0-4e84-844b-9e061f93db47".getBytes(UTF_8)));
        record.headers().add(new RecordHeader("X-Source-Service", "scheduling-service".getBytes(UTF_8)));
        return record;
    }

    private String validPayload() {
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
}
