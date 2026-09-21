package com.notificationservice.api.application.dto.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.time.OffsetDateTime;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public record AppointmentNotificationEvent(
    UUID appointmentId,
    UUID patientId,
    UUID doctorId,
    String fullname,
    String email,
    String body,
    @JsonDeserialize(using = FlexibleOffsetDateTimeDeserializer.class)
    OffsetDateTime appointmentDateTime
) {
}
