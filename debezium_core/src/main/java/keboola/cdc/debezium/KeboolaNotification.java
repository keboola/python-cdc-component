package keboola.cdc.debezium;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.pipeline.notification.Notification;
import io.debezium.pipeline.notification.channels.NotificationChannel;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;

@Slf4j
public class KeboolaNotification implements NotificationChannel {

	public static final String KEBOOLA_NOTIFICATION_CHANNEL = "keboola";

	@Override
	public void init(CommonConnectorConfig config) {
	}

	@Override
	public String name() {
		return KEBOOLA_NOTIFICATION_CHANNEL;
	}

	@Override
	public void send(Notification notification) {
		log.info("[Keboola Notification Service] {}", notification);
		if (Objects.equals(notification.getAggregateType(), "Initial Snapshot")
				|| Objects.equals(notification.getAggregateType(), "Incremental Snapshot")) {
			boolean snapshotInProgress = !Objects.equals(notification.getType(), "COMPLETED")
					&& !Objects.equals(notification.getType(), "ABORTED")
					&& !Objects.equals(notification.getType(), "SKIPPED");
			SyncStats.setSnapshotInProgress(snapshotInProgress);
		}
	}

	@Override
	public void close() {
	}
}
