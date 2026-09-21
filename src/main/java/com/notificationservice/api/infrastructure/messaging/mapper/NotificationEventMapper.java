package com.notificationservice.api.infrastructure.messaging.mapper;

import com.notificationservice.api.application.dto.event.AppointmentNotificationEvent;
import com.notificationservice.api.domain.model.AppointmentNotification;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
public class NotificationEventMapper {

    public AppointmentNotification toDomain(
        final AppointmentNotificationEvent event,
        final Headers headers
    ) {
        return new AppointmentNotification(
            event.appointmentId(),
            event.patientId(),
            event.doctorId(),
            event.fullname(),
            event.email(),
            event.body(),
            event.appointmentDateTime(),
            requireHeader(headers, "X-Idempotency-Key"),
            requireHeader(headers, "X-Source-Service")
        );
    }

    private String requireHeader(final Headers headers, final String key) {
        final Header header = headers.lastHeader(key);
        if (header == null || header.value() == null) {
            throw new IllegalArgumentException("Appointment notification must include header " + key);
        }

        final String value = new String(header.value(), StandardCharsets.UTF_8);
        if (value.isBlank()) {
            throw new IllegalArgumentException("Appointment notification must include header " + key);
        }
        return value;
    }
}
