package io.github.jimzucker.flinktraining.job;

import io.github.jimzucker.flinktraining.model.Json;
import io.github.jimzucker.flinktraining.model.Position;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

/**
 * Encodes a position for Kafka: the position key becomes the record key, so a
 * key's history stays on one partition and in order.
 *
 * <p>The record timestamp carries the originating trade's event time rather than
 * the moment of writing. Part 2 windows on that, and end-to-end latency is
 * measured against it.
 */
public class PositionSerialization implements KafkaRecordSerializationSchema<PositionState> {

    private static final long serialVersionUID = 1L;

    private final String topic;

    public PositionSerialization(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            PositionState state, KafkaSinkContext context, @Nullable Long timestamp) {
        Position position = new Position(
                state.key, state.symbol, state.quantity,
                state.lastTradeId, state.updateCount, state.eventTime);
        return new ProducerRecord<>(
                topic,
                null,
                state.eventTime,
                state.key.getBytes(StandardCharsets.UTF_8),
                Json.toBytes(position));
    }
}
