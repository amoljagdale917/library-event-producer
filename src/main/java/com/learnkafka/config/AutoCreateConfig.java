package com.learnkafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class AutoCreateConfig {
	
	/*
	 * .\kafka-topics.bat --list --zookeeper localhost:2181 
	 * to check the topic are created or not
	 */
	@Bean
	public NewTopic libraryEvent() {
		return TopicBuilder.name("library-events")
		.partitions(3)
		.replicas(3)
		.build();	
	}
}
