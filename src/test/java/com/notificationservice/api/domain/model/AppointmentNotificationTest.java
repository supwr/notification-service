package com.notificationservice.api.domain.model;

import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AppointmentNotificationTest {

    @Test
    void shouldCreateDomainModelWhenPayloadIsValid() {
        assertDoesNotThrow(this::buildValidDomain);
    }

    @Test
    void shouldRejectBlankEmail() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new AppointmentNotification(
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                "Marcelo",
                " ",
                "Um novo agendamento foi feito.",
                OffsetDateTime.parse("2026-09-18T21:22:25.894Z"),
                "event-key-1",
                "scheduling-service"
            )
        );
    }

    private AppointmentNotification buildValidDomain() {
        return new AppointmentNotification(
            UUID.randomUUID(),
            UUID.randomUUID(),
            UUID.randomUUID(),
            "Marcelo",
            "patient@example.com",
            "Um novo agendamento foi feito.",
            OffsetDateTime.parse("2026-09-18T21:22:25.894Z"),
            "event-key-1",
            "scheduling-service"
        );
    }
}
