package keboola.cdc.debezium;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import keboola.cdc.debezium.converter.JsonConverter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DbChangeConsumerTest {
	private DbChangeConsumer dbChangeConsumer;
	private AtomicInteger count;
	@Mock
	private DuckDbWrapper dbWrapper;
	@Mock
	private JsonConverter converter;
	@Mock
	private DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> committer;

	@BeforeEach
	void setUp() {
		this.count = new AtomicInteger();
		SyncStats syncStats = new SyncStats();
		syncStats.setStartTime(ZonedDateTime.now());
		this.dbChangeConsumer = new DbChangeConsumer(this.count, "", syncStats,
				this.dbWrapper, (gson, wrapper, tableName, initialSchema) -> this.converter);
	}

	@Test
	void shouldHandleBatchSuccessfully() throws InterruptedException {
		ChangeEvent<String, String> event = mock(ChangeEvent.class);
		when(event.key()).thenReturn("key");
		when(event.value()).thenReturn("{\"schema\":{\"name\":\"table.Value\",\"fields\":[]},\"payload\":{}}");

		this.dbChangeConsumer.handleBatch(Collections.singletonList(event), this.committer);

		verify(this.committer).markProcessed(event);
		verify(this.committer).markBatchFinished();
		assertEquals(1, this.count.get());
	}

	@Test
	void shouldCloseWriterStreams() {
		this.dbChangeConsumer.closeWriterStreams();

		verify(this.dbWrapper).close();
	}
}
