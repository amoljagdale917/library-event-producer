package com.learnkafka.controller;

import java.util.concurrent.ExecutionException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.producer.LibraryEventProducer;

import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
public class LibraryEventController {

	/**
	 *
	 * { "libraryEventId":null, "book":{ "bookId":456, "bookName":"Kafka using
	 * Spring Boot", "bookAuthor": "Amol Jagdale" } }
	 * 
	 * and check the topic where message is successfully publish or not using below
	 * cmd .\bin\windows\kafka-console-consumer.bat --bootstrap-server
	 * localhost:9092 --topic library-events
	 */

	@Autowired
	LibraryEventProducer libraryEventProducer;

	@PostMapping("/v1/libraryevent")
	public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody @Validated LibraryEvent libraryEvent)
			throws JsonProcessingException, InterruptedException, ExecutionException {
		log.info("Before Publish the Data into topic");
		// Asynchronous behavior
		// libraryEventProducer.sendLibraryEvent(libraryEvent);

		// Asynchronous Approach 2
		libraryEvent.setLibraryEventType(LibraryEventType.NEW);
		log.info("*********************************************************");
		log.info("Library Event Object {}", libraryEvent);
		libraryEventProducer.sendLibraryEventApproch2(libraryEvent);
		log.info("Library Event after Object {}", libraryEvent);

		// Synchronous behavior
		// SendResult<Integer, String> sendResult =
		// libraryEventProducer.sendLibraryEventSynchronous(libraryEvent);
		// log.info("sendResult is {} ", sendResult.toString());

		// Synchronous behavior approach 2
		// SendResult<Integer, String> sendResult =
		// libraryEventProducer.sendLibraryEventSynchronousApproach2(libraryEvent);
		// log.info("sendResult is {} ", sendResult.toString());

		log.info("After Publish the Data into topic");
		return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
	}

	@PutMapping("/v1/libraryevent")
	public ResponseEntity<?> putLibraryEvent(@RequestBody @Validated LibraryEvent libraryEvent)
			throws JsonProcessingException, InterruptedException, ExecutionException {
		if (libraryEvent.getLibraryEventId() == null) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please pass the Library Event");
		}
		libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
		libraryEventProducer.sendLibraryEventApproch2(libraryEvent);
		return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
	}

}
