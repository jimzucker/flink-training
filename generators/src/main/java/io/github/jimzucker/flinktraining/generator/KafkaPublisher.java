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

    private static long envLong(String name, long fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : Long.parseLong(value.trim());
    }

    private static String envString(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value.trim();
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
        props.put(ProducerConfig.LINGER_MS_CONFIG, envInt("LINGER_MS", 5));
        // Kafka's 16KB default holds about fifty records at this size, so the
        // producer spends its time in request round-trips rather than moving
        // bytes. Measured uncompressed, the generator sustains about 104MB/s
        // against the 163MB/s one producer got against this broker, and the gap
        // is request rate. Bigger batches are free: the broker does the same
        // work per byte and Flink does not see this setting at all.
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, envInt("BATCH_SIZE", 262144));
        // So a stalled broker parks records instead of blocking the caller.
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, envLong("BUFFER_MEMORY", 268435456L));
        // lz4 rather than gzip. gzip was chosen to avoid the native-library warning
        // that lz4 and snappy trigger, so that a demo given live from a console
        // starts clean -- but that warning arrives on Java 24+, and this project
        // targets 17, which is what the images and CI run. Verified: zero warning
        // lines under Java 17.
        //
        // The cost was real. Measured on this broker with acks=all and
        // idempotence, gzip caps one producer at about 150,000 records/sec
        // against lz4's 514,000, and that ceiling is what the generator ran into
        // during the scale tests -- offered load never passed ~120,000/sec no
        // matter what rate was asked for.
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, envString("COMPRESSION_TYPE", "lz4"));
        return props;
    }

    public void publish(BlockTrade trade) {
        producer.send(new ProducerRecord<>(
                ordersTopic, null, trade.eventTime(), trade.tradeId(), Json.toBytes(trade)));
    }

    /**
     * Publishes to a named partition rather than letting the key decide.
     *
     * <p>Only used when several threads produce trades. Byte-identical replay
     * needs each partition to receive its records in a fixed order, and hashing
     * the key sends records from every thread to every partition, so which
     * arrives first varies from run to run. Naming the partition gives each one a
     * single writer, and the order within it is that writer's sequence order.
     *
     * <p>The key is still the trade id: only the placement changes, not what
     * downstream sees on the record.
     */
    public void publish(BlockTrade trade, int partition) {
        producer.send(new ProducerRecord<>(
                ordersTopic, partition, trade.eventTime(), trade.tradeId(), Json.toBytes(trade)));
    }

    /** Partition count of the orders topic, as the broker currently reports it. */
    public int ordersPartitionCount() {
        return producer.partitionsFor(ordersTopic).size();
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
