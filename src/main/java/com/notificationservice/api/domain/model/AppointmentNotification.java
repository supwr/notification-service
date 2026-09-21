package com.notificationservice.api.domain.model;

import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.UUID;

public record AppointmentNotification(
    UUID appointmentId,
    UUID patientId,
    UUID doctorId,
    String fullname,
    String email,
    String body,
    OffsetDateTime appointmentDateTime,
    String idempotencyKey,
    String sourceService
) {
    public AppointmentNotification {
        Objects.requireNonNull(appointmentId, "appointmentId must not be null");
        Objects.requireNonNull(patientId, "patientId must not be null");
        Objects.requireNonNull(doctorId, "doctorId must not be null");
        Objects.requireNonNull(appointmentDateTime, "appointmentDateTime must not be null");

        fullname = requireNotBlank(fullname, "fullname");
        email = requireNotBlank(email, "email");
        body = requireNotBlank(body, "body");
        idempotencyKey = requireNotBlank(idempotencyKey, "idempotencyKey");
        sourceService = requireNotBlank(sourceService, "sourceService");
    }

    private static String requireNotBlank(final String value, final String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }
}
