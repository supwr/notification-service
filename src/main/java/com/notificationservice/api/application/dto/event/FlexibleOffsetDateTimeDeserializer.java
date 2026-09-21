package com.notificationservice.api.application.dto.event;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;

public class FlexibleOffsetDateTimeDeserializer extends JsonDeserializer<OffsetDateTime> {

    private static final List<DateTimeFormatter> SUPPORTED_FORMATS = List.of(
        DateTimeFormatter.ISO_OFFSET_DATE_TIME,
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS Z")
    );

    @Override
    public OffsetDateTime deserialize(final JsonParser parser, final DeserializationContext context) throws IOException {
        final String raw = parser.getText();
        if (raw == null || raw.isBlank()) {
            return null;
        }

        for (DateTimeFormatter formatter : SUPPORTED_FORMATS) {
            try {
                return OffsetDateTime.parse(raw.trim(), formatter);
            } catch (DateTimeParseException ignored) {
                // Try next format.
            }
        }

        throw new IOException("Unsupported appointmentDateTime format: " + raw);
    }
}
