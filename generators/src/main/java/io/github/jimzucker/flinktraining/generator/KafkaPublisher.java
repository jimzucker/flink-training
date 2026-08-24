package io.github.jimzucker.flinktraining.generator;

import io.github.jimzucker.flinktraining.model.BlockTrade;
import io.github.jimzucker.flinktraining.model.Json;
import io.github.jimzucker.flinktraining.model.Price;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * Writes generated records to Kafka.
 *
 * <p>Keys are set explicitly — {@code tradeId} for orders, {@code symbol} for
 * prices — because the key is what puts every price for a symbol on one
 * partition, and that is what guarantees prices are consumed in the order they
 * were produced.
 */
public final class KafkaPublisher implements AutoCloseable {

    private final Producer<String, byte[]> producer;
    private final String ordersTopic;
    private final String pricesTopic;

    public KafkaPublisher(GeneratorConfig config) {
        this(new KafkaProducer<>(producerProperties(config.bootstrapServers())),
                config.ordersTopic(), config.pricesTopic());
    }

    KafkaPublisher(Producer<String, byte[]> producer, String ordersTopic, String pricesTopic) {
        this.producer = producer;
        this.ordersTopic = ordersTopic;
        this.pricesTopic = pricesTopic;
    }

    private static int envInt(String name, int fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : Integer.parseInt(value.trim());
    }

    static Properties producerProperties(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        // Ordering per partition has to hold for the design's watermark
        // assumption. An idempotent producer gives that on its own: every batch
        // carries a sequence number and the broker rejects one that arrives out
        // of order, for up to five requests in flight.
        //
        // This previously pinned in-flight requests to one, which ordering did
        // not require. Measured, it made no difference to throughput -- the
        // producer sustains over a hundred thousand records a second either way,
        // and what capped the generator was its own pacer. The constraint is
        // dropped because it was unnecessary, not because it was expensive.
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                envInt("MAX_IN_FLIGHT", 5));
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        // gzip rather than lz4/snappy/zstd: those load a native library, which on
        // Java 17+ prints a restricted-method warning on every start. The demo is
        // given live from a console, so a clean start matters more than the
        // difference in compression cost at these volumes.
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        return props;
    }

    public void publish(BlockTrade trade) {
        producer.send(new ProducerRecord<>(
                ordersTopic, null, trade.eventTime(), trade.tradeId(), Json.toBytes(trade)));
    }

    public void publish(Price price) {
        producer.send(new ProducerRecord<>(
                pricesTopic, null, price.eventTime(), price.symbol(), Json.toBytes(price)));
    }

    public void flush() {
        producer.flush();
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }

    /** Byte size of a record as it goes on the wire, for logging throughput honestly. */
    static int wireSize(String key, byte[] value) {
        return key.getBytes(StandardCharsets.UTF_8).length + value.length;
    }
}
