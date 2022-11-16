package com.learnkafka.producer;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class LibraryEventProducer {
	
	//if we are doing with send method that time we need to defined topic here or it is mandatory field.
	public static final String TOPIC_NAME= "library-events";
	
	@Autowired
	KafkaTemplate<Integer, String> kafkaTemplate;
	
	//to convert libraryEvent object into string using ObjectMapper
	@Autowired
	ObjectMapper objectMapper;
	
	public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);
		ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(key, value);	
		listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				handlerSuccess(key, value, result);
			}

			@Override
			public void onFailure(Throwable ex) {
				handleFailure(key, value, ex);
			}
		});
	}
	
	public ListenableFuture<SendResult<Integer, String>> sendLibraryEventApproch2(LibraryEvent libraryEvent) throws JsonProcessingException {
		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);
		
		
		ProducerRecord<Integer, String> producerRecord = buildProducerRecord(TOPIC_NAME, key, value);
		ListenableFuture<SendResult<Integer, String>> sendResult = kafkaTemplate.send(producerRecord);
		sendResult.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				handlerSuccess(key, value, result);
			}

			@Override
			public void onFailure(Throwable ex) {
				handleFailure(key, value, ex);
			}
		});
		return sendResult;
	}

	private ProducerRecord<Integer, String> buildProducerRecord(String topicName, Integer key, String value) {
		return new ProducerRecord<Integer, String>(topicName, null, key, value, null);
	}

	public SendResult<Integer, String> sendLibraryEventSynchronous(LibraryEvent libraryEvent) throws JsonProcessingException, InterruptedException, ExecutionException{
		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);
		SendResult<Integer, String> sendResult = null;
		try {
			sendResult = kafkaTemplate.sendDefault(key, value).get();
		}catch (InterruptedException | ExecutionException ex) {
			log.error("ExecutionException/InterruptedException sending the message and exception is {}", ex.getMessage());
			throw ex;
		}catch (Exception e) {
			log.error("Exception sending the message and exception is {}", e.getMessage());
			throw e;
		}
		return sendResult;
	}
	
	public SendResult<Integer, String> sendLibraryEventSynchronousApproach2(LibraryEvent libraryEvent) throws InterruptedException, ExecutionException, JsonProcessingException{
		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);
		
		SendResult<Integer, String> sendResult = null;
		ProducerRecord<Integer, String> producerRecord = buildProducerRecord(TOPIC_NAME, key, value);
		
		try {
			sendResult = kafkaTemplate.send(producerRecord).get();
		} catch (InterruptedException | ExecutionException e) {
			log.error("ExecutionException/InterruptedException sending the message and exception is {}", e.getMessage());
			throw e;
		}catch(Exception e) {
			log.error("Exception sending the message and exception is {}", e.getMessage());
			throw e;
		}
		return sendResult;
		
	}
	
	private void handleFailure(Integer key, String value, Throwable ex) {
		log.error("Error sending the message and the exception is :{}", ex.getMessage());
		try{
			throw ex;
		}catch (Throwable e) {
			log.error("Error is failure : {}", e.getMessage());
		}

	}

	private void handlerSuccess(Integer key, String value, SendResult<Integer, String> result) {
		log.info("Message send successfully for the key : {} and value is {}, patition is {}", key, value, result.getRecordMetadata().partition());
	}
	
}
