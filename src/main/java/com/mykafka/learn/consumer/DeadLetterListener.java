package com.mykafka.learn.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mykafka.learn.producer.EmailSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.Date;

public class DeadLetterListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(EmailListener.class);

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    EmailSender emailSender;

    @Autowired
    KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @KafkaListener(id = "deadEmailListener", topics = "deadEmails", containerFactory = "deadLetterListenerContainerFactory")
    public void deadLetterEmailListener(@Payload String in,
                                        @Header("resendAt") Date resendAt,
                                        @Header("retryAttempt") int retryAttempt,
                                        Acknowledgment ack
    ) throws Exception {
        LOGGER.info("deadLetter received :{}", in);
        LOGGER.info("resendAt: {} ", resendAt.toString());
        LOGGER.info("retryAttempt: {} ", retryAttempt);
        try{
            if(retryAttempt >= 3)
            {
                LOGGER.error("Discarding message; retry limit reached: {}", in);
                ack.acknowledge();
                return;
            }

            if(new Date().before(resendAt)){
                kafkaListenerEndpointRegistry.getListenerContainer("deadEmailListener").pause();
                throw new Exception();
            }
            emailSender.send(in, 76, retryAttempt);

            //kafkaListenerEndpointRegistry.getListenerContainer("listen1").resume();
            ack.acknowledge();
            //kafkaListenerEndpointRegistry.getListenerContainer("listen1").resume();
        }catch (Exception e){
            LOGGER.info("Pausing dead message container;");
            throw e;
        }
    }

    @Scheduled(fixedDelay = 30000, initialDelay = 5000)
    public void scheduledJobExpireTask() {
        try{
            kafkaListenerEndpointRegistry.getListenerContainer("deadEmailListener").resume();
        }catch (Exception e){
            LOGGER.info("Could not resume deadEmailListener container");
        }
    }
}
