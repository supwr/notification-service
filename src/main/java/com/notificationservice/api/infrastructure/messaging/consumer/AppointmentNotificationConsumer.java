package com.notificationservice.api.infrastructure.messaging.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.notificationservice.api.application.dto.event.AppointmentNotificationEvent;
import com.notificationservice.api.application.usecase.send.SendAppointmentNotificationUseCase;
import com.notificationservice.api.domain.model.AppointmentNotification;
import com.notificationservice.api.infrastructure.messaging.mapper.NotificationEventMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.SameIntervalTopicReuseStrategy;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@Profile("worker")
public class AppointmentNotificationConsumer {

    private final ObjectMapper objectMapper;
    private final NotificationEventMapper mapper;
    private final SendAppointmentNotificationUseCase useCase;

    public AppointmentNotificationConsumer(
        final ObjectMapper objectMapper,
        final NotificationEventMapper mapper,
        final SendAppointmentNotificationUseCase useCase
    ) {
        this.objectMapper = objectMapper;
        this.mapper = mapper;
        this.useCase = useCase;
    }

    @RetryableTopic(
        attempts = "${app.kafka.retry.attempts}",
        retryTopicSuffix = "${app.kafka.retry.retry-topic-suffix}",
        dltTopicSuffix = "${app.kafka.retry.dlq-topic-suffix}",
        sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.SINGLE_TOPIC
    )
    @KafkaListener(topics = "${app.kafka.topics.appointment-notification}", groupId = "${spring.kafka.consumer.group-id}")
    public void listen(final ConsumerRecord<String, String> record) throws IOException {
        final AppointmentNotificationEvent event = objectMapper.readValue(record.value(), AppointmentNotificationEvent.class);
        final AppointmentNotification notification = mapper.toDomain(event, record.headers());
        useCase.execute(notification);
    }
}
