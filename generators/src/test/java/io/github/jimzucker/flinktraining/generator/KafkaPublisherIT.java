package io.github.jimzucker.flinktraining.generator;

import io.github.jimzucker.flinktraining.model.BlockTrade;
import io.github.jimzucker.flinktraining.model.Json;
import io.github.jimzucker.flinktraining.model.Price;
import io.github.jimzucker.flinktraining.model.ReferenceData;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Round-trips generated records through a real broker.
 *
 * <p>Spins up its own Kafka rather than using the compose stack, so the build
 * needs a Docker daemon but not a running project. That keeps it usable on a
 * clean CI runner where nothing has been started by hand.
 *
 * <p>The image matches the one in {@code docker/compose.yml}, so this is testing
 * against the same broker version the demo runs on.
 */
@Testcontainers
class KafkaPublisherIT {

    private static final String ORDERS = "orders";
    private static final String PRICES = "prices";
    private static final int PARTITIONS = 4;

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.2"));

    @BeforeAll
    static void createTopics() throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        try (Admin admin = Admin.create(props)) {
            admin.createTopics(List.of(
                    new NewTopic(ORDERS, PARTITIONS, (short) 1),
                    new NewTopic(PRICES, PARTITIONS, (short) 1))).all().get();
        }
    }

    private static GeneratorConfig config() {
        return new GeneratorConfig(KAFKA.getBootstrapServers(), ORDERS, PRICES,
                GeneratorConfig.DEMO_TRADES_PER_SECOND, GeneratorConfig.DEMO_PRICES_PER_SECOND,
                GeneratorConfig.DEFAULT_SEED, GeneratorConfig.REPLAY_START_EPOCH_MILLIS, 0L);
    }

    private static <T> List<ConsumerRecord<String, byte[]>> drain(String topic, int expected) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-" + topic + "-" + System.nanoTime());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        List<ConsumerRecord<String, byte[]>> out = new ArrayList<>();
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + 30_000;
            while (out.size() < expected && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, byte[]> batch = consumer.poll(Duration.ofMillis(500));
                batch.forEach(out::add);
            }
        }
        return out;
    }

    @Test
    @DisplayName("block trades round-trip through Kafka unchanged, keyed by tradeId")
    void tradesRoundTrip() {
        BlockTradeGenerator generator = BlockTradeGenerator.replaying(
                GeneratorConfig.DEFAULT_SEED, GeneratorConfig.REPLAY_START_EPOCH_MILLIS, 100L);
        List<BlockTrade> sent = new ArrayList<>();
        try (KafkaPublisher publisher = new KafkaPublisher(config())) {
            for (int i = 0; i < 40; i++) {
                BlockTrade trade = generator.next();
                sent.add(trade);
                publisher.publish(trade);
            }
            publisher.flush();
        }

        List<ConsumerRecord<String, byte[]>> received = drain(ORDERS, sent.size());
        assertThat(received).hasSize(sent.size());

        Map<String, BlockTrade> byId = new HashMap<>();
        for (ConsumerRecord<String, byte[]> record : received) {
            BlockTrade trade = Json.fromBytes(record.value(), BlockTrade.class);
            assertThat(record.key())
                    .as("orders must be keyed by tradeId")
                    .isEqualTo(trade.tradeId());
            assertThat(record.timestamp())
                    .as("record timestamp must carry event time")
                    .isEqualTo(trade.eventTime());
            byId.put(trade.tradeId(), trade);
        }
        for (BlockTrade original : sent) {
            assertThat(byId.get(original.tradeId()))
                    .as("round trip of %s", original.tradeId())
                    .isEqualTo(original);
        }
    }

    @Test
    @DisplayName("every price for a symbol lands on one partition, so order is preserved")
    void pricesForASymbolShareAPartition() {
        // This is the guarantee the whole watermark design rests on: because the
        // key is the symbol, a symbol's prices cannot be spread across partitions
        // and therefore cannot be consumed out of order.
        PriceGenerator generator = PriceGenerator.replaying(
                GeneratorConfig.DEFAULT_SEED + 1, GeneratorConfig.REPLAY_START_EPOCH_MILLIS, 1_000L);
        int expected = 25 * ReferenceData.SYMBOLS.size();
        try (KafkaPublisher publisher = new KafkaPublisher(config())) {
            for (int i = 0; i < expected; i++) {
                publisher.publish(generator.next());
            }
            publisher.flush();
        }

        List<ConsumerRecord<String, byte[]>> received = drain(PRICES, expected);
        assertThat(received).hasSize(expected);

        Map<String, Set<Integer>> partitionsBySymbol = new HashMap<>();
        Map<Integer, List<Long>> timesByPartition = new HashMap<>();
        for (ConsumerRecord<String, byte[]> record : received) {
            Price price = Json.fromBytes(record.value(), Price.class);
            assertThat(record.key())
                    .as("prices must be keyed by symbol")
                    .isEqualTo(price.symbol());
            partitionsBySymbol.computeIfAbsent(price.symbol(), k -> new HashSet<>()).add(record.partition());
            timesByPartition.computeIfAbsent(record.partition(), k -> new ArrayList<>()).add(record.timestamp());
        }

        assertThat(partitionsBySymbol).hasSize(ReferenceData.SYMBOLS.size());
        partitionsBySymbol.forEach((symbol, partitions) ->
                assertThat(partitions)
                        .as("%s must occupy exactly one partition", symbol)
                        .hasSize(1));

        // Within a partition, event time never goes backwards.
        timesByPartition.forEach((partition, times) ->
                assertThat(times)
                        .as("event times within partition %s", partition)
                        .isSorted());
    }
}
