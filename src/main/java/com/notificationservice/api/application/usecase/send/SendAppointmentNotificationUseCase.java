package com.notificationservice.api.application.usecase.send;

import com.notificationservice.api.domain.model.AppointmentNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class SendAppointmentNotificationUseCase {

    private static final Logger logger = LoggerFactory.getLogger(SendAppointmentNotificationUseCase.class);

    public void execute(final AppointmentNotification notification) {
        logger.info(
            "Simulating notification sending for appointmentId={}, patientId={}, email={}, sourceService={}, idempotencyKey={}",
            notification.appointmentId(),
            notification.patientId(),
            notification.email(),
            notification.sourceService(),
            notification.idempotencyKey()
        );
    }
}
