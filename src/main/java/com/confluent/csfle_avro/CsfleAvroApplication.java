package com.confluent.csfle_avro;

import java.util.Random;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.confluent.dto.PersonalData;

@SpringBootApplication
@RestController
public class CsfleAvroApplication {

	@Autowired
	Producer producer;

	public static void main(String[] args) {
		SpringApplication.run(CsfleAvroApplication.class, args);
	}

	@GetMapping("/hello")
	public Integer getHello() {
		return new Random().nextInt(100);
	}

	@PostMapping("/events")
	public String sendEvents(@RequestBody PersonalData personalData) {
		System.out.print(personalData.toString());
		producer.sendMessage(getHello().toString(), personalData);
		return personalData.toString();
	}

}
