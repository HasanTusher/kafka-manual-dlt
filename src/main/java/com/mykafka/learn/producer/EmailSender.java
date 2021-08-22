package com.mykafka.learn.producer;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.Date;

public class EmailSender {
    private KafkaTemplate<Integer, String> template;

    public EmailSender(KafkaTemplate<Integer, String> template) {
        this.template = template;
    }

    public void send(String toSend, int key, int retryAttempt) {
        Message<String> stringMessage = MessageBuilder
                .withPayload(toSend)
                .setHeader(KafkaHeaders.TOPIC, "topic1")
                .setHeader(KafkaHeaders.MESSAGE_KEY, key)
                .setHeader("resendAt", new Date())
                .setHeader("retryAttempt", retryAttempt).build();
        this.template.send(stringMessage);
    }
}
