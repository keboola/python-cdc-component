package keboola.cdc.debezium;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.StopEngineException;
import keboola.cdc.debezium.converter.JsonConverter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DbChangeConsumerTest {
	private DbChangeConsumer dbChangeConsumer;
	@Mock
	private DuckDbWrapper dbWrapper;
	@Mock
	private JsonConverter converter;
	@Mock
	private DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> committer;

	@BeforeEach
	void setUp() {
		this.dbChangeConsumer = new DbChangeConsumer("", new SyncStats(),
				this.dbWrapper, (gson, wrapper, tableName, initialSchema) -> this.converter);
	}

	@Test
	void shouldHandleBatchSuccessfully() throws InterruptedException {
		ChangeEvent<String, String> event = mock(ChangeEvent.class);
		when(event.key()).thenReturn("key");
		when(event.value()).thenReturn(
				"""
						{"schema":{"name":"table.Value","fields":[]},"payload":{"kbc__event_timestamp":1714135955123}}
						"""
		);

		this.dbChangeConsumer.handleBatch(Collections.singletonList(event), this.committer);

		verify(this.committer).markProcessed(event);
		verify(this.committer).markBatchFinished();
		assertEquals(1, this.dbChangeConsumer.getRecordsCount().get());
	}

	@Test
	void doNotProcessEventsFromFutureOnlyMessage() throws InterruptedException {
		ChangeEvent<String, String> event = mock(ChangeEvent.class);
		when(event.key()).thenReturn("key");
		when(event.value()).thenReturn(
				"""
						{"schema":{"name":"table.Value","fields":[]},"payload":{"kbc__event_timestamp":2714135955123}}
						"""
		);

		Assertions.assertThrows(StopEngineException.class,
				() -> this.dbChangeConsumer.handleBatch(Collections.singletonList(event), this.committer));

		verify(this.committer, Mockito.never()).markProcessed(event);
		verify(this.committer, Mockito.never()).markBatchFinished();
		verify(this.converter, Mockito.never()).processJson(any());
		assertEquals(0, this.dbChangeConsumer.getRecordsCount().get());
	}

	@Test
	void doNotProcessEventsFromFuture() throws InterruptedException {
		var events = prepareEvents(1714135955123L, 1714135955123L, 1714135955123L, 2714135955123L);

		Assertions.assertThrows(StopEngineException.class,
				() -> this.dbChangeConsumer.handleBatch(events, this.committer));

		verify(this.committer, Mockito.times(3)).markProcessed(any());
		verify(this.committer, Mockito.never()).markBatchFinished();
		verify(this.converter, Mockito.times(3)).processJson(any());
		assertEquals(3, this.dbChangeConsumer.getRecordsCount().get());
	}

	private static List<ChangeEvent<String, String>> prepareEvents(Long... timestamps) {
		return Stream.of(timestamps)
				.map(timestamp -> {
					ChangeEvent<String, String> event = mock(ChangeEvent.class);
					when(event.key()).thenReturn("key");
					when(event.value()).thenReturn(
							String.format("""
									{"schema":{"name":"table.Value","fields":[]},"payload":{"kbc__event_timestamp":%d}}
									""", timestamp)
					);
					return event;
				})
				.toList();
	}

	@Test
	void shouldCloseWriterStreams() {
		this.dbChangeConsumer.closeWriterStreams();

		verify(this.dbWrapper).close();
	}
}
