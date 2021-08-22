package com.mykafka.learn.producer;

import org.apache.commons.lang3.time.DateUtils;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.Date;

public class DeadLetterSender {

    private KafkaTemplate<Integer, String> template;

    public DeadLetterSender(KafkaTemplate<Integer, String> template) {
        this.template = template;
    }

    public void send(String toSend, int key, int retryAttempt) {
        Message<String> stringMessage = MessageBuilder
                .withPayload(toSend)
                .setHeader(KafkaHeaders.TOPIC, "deadEmails")
                .setHeader(KafkaHeaders.MESSAGE_KEY, key)
                .setHeader("resendAt", DateUtils.addMinutes(new Date(), 1))
                .setHeader("retryAttempt", retryAttempt)
                .build();
        this.template.send(stringMessage);
    }
}
