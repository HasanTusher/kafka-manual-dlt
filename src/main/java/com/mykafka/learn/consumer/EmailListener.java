package com.mykafka.learn.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mykafka.learn.data.ThouObj;
import com.mykafka.learn.producer.DeadLetterSender;
import com.mykafka.learn.service.MessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

import java.util.Date;

public class EmailListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(EmailListener.class);

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;


    @Autowired
    MessageHandler messageHandler;

    @Autowired
    DeadLetterSender deadLetterSender;

    @KafkaListener(id = "listen1", topics = "topic1", containerFactory = "emailListenerContainerFactory")
    public void listen1(@Payload String in,
                        @Header("resendAt") Date createdAt,
                        @Header("retryAttempt") int retryAttempt,
                        Acknowledgment ack
    ) {
        try{
            kafkaListenerEndpointRegistry.getListenerContainer("listen1").pause(); //pause the consumer so it does not timeout
            ThouObj thouObj = objectMapper.readValue(in, ThouObj.class);
            if(thouObj.getId() != 0){
                throw new Exception();
            }

            this.handleMessage(thouObj);

            kafkaListenerEndpointRegistry.getListenerContainer("listen1").resume();
        }catch (Exception e){
            this.deadLetterSender.send(in, 1, ++retryAttempt); // TODO: get actual key;add the record to dead letter topic
        }
        LOGGER.info("Main topic message:{}", in);
        ack.acknowledge();
        kafkaListenerEndpointRegistry.getListenerContainer("listen1").resume();
    }

    private void handleMessage(ThouObj thouObj) throws Exception{
        this.messageHandler.handle(thouObj);
    }
}
