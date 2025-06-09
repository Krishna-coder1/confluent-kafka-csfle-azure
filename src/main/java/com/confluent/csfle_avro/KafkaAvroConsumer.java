package com.confluent.csfle_avro;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.confluent.dto.PersonalData;

@Service
public class KafkaAvroConsumer {
    private static final String TOPIC = "userInfo";

    @KafkaListener(id = "myConsumer", topics = TOPIC, autoStartup = "true")
    public void read(ConsumerRecord<String, GenericRecord> consumerRecord) {
        String key = consumerRecord.key();
        Object employee = consumerRecord.value();
        System.out.println("");
        System.out.println("");
        System.out.printf("Decrypted avro message consumed for key : " + key + " value : %s%n",
                employee.toString());

    }
}
