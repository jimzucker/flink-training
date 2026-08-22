package io.github.jimzucker.flinktraining.job;

import io.github.jimzucker.flinktraining.model.Json;
import io.github.jimzucker.flinktraining.model.MarketValue;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

/**
 * Encodes a market value for Kafka, keyed the same way the position was so a
 * key's history stays on one partition and in order.
 *
 * <p>The record timestamp is the window close, which is the instant the figure
 * describes.
 */
public class MarketValueSerialization implements KafkaRecordSerializationSchema<MarketValueState> {

    private static final long serialVersionUID = 1L;

    private final String topic;

    public MarketValueSerialization(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            MarketValueState state, KafkaSinkContext context, @Nullable Long timestamp) {
        MarketValue value = new MarketValue(
                state.key, state.symbol, state.quantity, state.price,
                state.marketValue, state.lastTradeId, state.windowEnd);
        return new ProducerRecord<>(
                topic, null, state.windowEnd,
                state.key.getBytes(StandardCharsets.UTF_8),
                Json.toBytes(value));
    }
}
