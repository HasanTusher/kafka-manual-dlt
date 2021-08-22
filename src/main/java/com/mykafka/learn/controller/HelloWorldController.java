package com.mykafka.learn.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mykafka.learn.data.ThouObj;
import com.mykafka.learn.producer.EmailSender;
import org.apache.commons.lang3.RandomStringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.concurrent.ThreadLocalRandom;

@RestController
public class HelloWorldController {

    @Autowired
    EmailSender emailSender;

    @Autowired
    ObjectMapper objectMapper;

    @GetMapping("/hello")
    String generateKafkaMessages() throws JsonProcessingException {
        ThouObj thouObj = this.getRandomThouObj();
        String s = this.objectMapper.writeValueAsString(thouObj);
        //this.emailSender.send(s, 1, 0 );
        return "SUCCESS";
    }

    private ThouObj getRandomThouObj() {
        return new ThouObj(this.getRandomInt(), RandomStringUtils.randomAlphabetic(10), RandomStringUtils.randomAlphabetic(100));
    }

    private int getRandomInt() {
       return  ThreadLocalRandom.current().nextInt(10, 1000 + 1);
    }

}
