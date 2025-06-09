package com.confluent.csfle_avro;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import com.confluent.dto.PersonalData;

import java.util.concurrent.CompletableFuture;

@Service
public class Producer {

    private static final Logger logger = LoggerFactory.getLogger(Producer.class);
    private static final String TOPIC = "userInfo";

    @Autowired
    private KafkaTemplate<String, PersonalData> kafkaTemplate;

    public void sendMessage(String key, PersonalData value) {

        CompletableFuture<SendResult<String, PersonalData>> future = kafkaTemplate.send(TOPIC, key, value);
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                logger.info(String.format("Produced event to topic %s: key = %-10s value = %s",
                        result.getRecordMetadata().topic(), key, value));
            } else {
                ex.printStackTrace(System.out);
            }
        });

    }

}