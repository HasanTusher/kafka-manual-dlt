package com.mykafka.learn.service;

import com.mykafka.learn.consumer.EmailListener;
import com.mykafka.learn.data.ThouObj;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class MessageHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(EmailListener.class);

    public void handle(ThouObj thouObj) throws Exception{

        LOGGER.info("yaay success");

        LOGGER.info(thouObj.toString());

    }
}
