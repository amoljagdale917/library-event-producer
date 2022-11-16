/*
 * package com.learnkafka.controller;
 * 
 * import static org.hamcrest.CoreMatchers.is; import static
 * org.hamcrest.CoreMatchers.isA; import static org.mockito.Mockito.doNothing;
 * import static org.mockito.Mockito.when; import static
 * org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
 * import static
 * org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
 * import static
 * org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
 * import static
 * org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
 * 
 * import org.junit.jupiter.api.Test; import org.mockito.Mockito; import
 * org.springframework.beans.factory.annotation.Autowired; import
 * org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
 * import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
 * import org.springframework.boot.test.mock.mockito.MockBean; import
 * org.springframework.http.MediaType; import
 * org.springframework.test.web.servlet.MockMvc;
 * 
 * import com.fasterxml.jackson.databind.ObjectMapper; import
 * com.learnkafka.domain.Book; import com.learnkafka.domain.LibraryEvent; import
 * com.learnkafka.producer.LibraryEventProducer;
 * 
 *//**
	 * @author Amol
	 *
	 *//*
		 * //controller layer
		 * 
		 * @WebMvcTest(LibraryEventController.class)
		 * 
		 * @AutoConfigureMockMvc public class LibraryEventControllerTest {
		 * 
		 * @Autowired MockMvc mockMvc;
		 * 
		 * ObjectMapper objectMapper = new ObjectMapper();
		 * 
		 * @MockBean LibraryEventProducer libraryEventProducer;
		 * 
		 * @Test void postLibraryEvent() throws Exception { Book book =
		 * Book.builder().bookId(123).bookAuthor("Amol Jagdale").
		 * bookName("Spring boot using Kafka").build(); LibraryEvent libraryEvent =
		 * LibraryEvent.builder().libraryEventId(null).book(book).build();
		 * 
		 * String json = objectMapper.writeValueAsString(libraryEvent);
		 * 
		 * doNothing().when(libraryEventProducer).sendLibraryEventApproch2(Mockito.any(
		 * LibraryEvent.class));
		 * 
		 * mockMvc.perform(post("/v1/libraryevent") .content(json)
		 * .contentType(MediaType.APPLICATION_JSON)).andExpect(status().isCreated()); }
		 * 
		 * 
		 * @Test void updateLibraryEvent() throws Exception {
		 * 
		 * //given Book book = new Book().builder() .bookId(123) .bookAuthor("Dilip")
		 * .bookName("Kafka Using Spring Boot") .build();
		 * 
		 * LibraryEvent libraryEvent = LibraryEvent.builder() .libraryEventId(123)
		 * .book(book) .build(); String json =
		 * objectMapper.writeValueAsString(libraryEvent);
		 * //when(libraryEventProducer.sendLibraryEventApproch2(isA(LibraryEvent.class))
		 * ).thenReturn(null);
		 * when(libraryEventProducer.sendLibraryEventApproch2((LibraryEvent)
		 * isA(LibraryEvent.class))).thenReturn(null); //expect mockMvc.perform(
		 * put("/v1/libraryevent") .content(json)
		 * .contentType(MediaType.APPLICATION_JSON)) .andExpect(status().isOk());
		 * 
		 * }
		 * 
		 * 
		 * @Test void updateLibraryEvent_withNullLibraryEventId() throws Exception {
		 * 
		 * //given Book book = new Book().builder() .bookId(123) .bookAuthor("Dilip")
		 * .bookName("Kafka Using Spring Boot") .build();
		 * 
		 * LibraryEvent libraryEvent = LibraryEvent.builder() .libraryEventId(null)
		 * .book(book) .build(); String json =
		 * objectMapper.writeValueAsString(libraryEvent);
		 * when(libraryEventProducer.sendLibraryEventApproch2(isA(LibraryEvent.class))).
		 * thenReturn(null);
		 * 
		 * //expects mockMvc.perform( put("/v1/libraryevent") .content(json)
		 * .contentType(MediaType.APPLICATION_JSON))
		 * .andExpect(status().is4xxClientError())
		 * .andExpect(content().string("Please pass the LibraryEventId"));
		 * 
		 * }
		 * 
		 * 
		 * }
		 */