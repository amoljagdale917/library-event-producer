package com.learnkafka.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class LibraryEvent {
	
	private Integer libraryEventId;
	private LibraryEventType libraryEventType;
	
	@NonNull
	private Book book;
}
