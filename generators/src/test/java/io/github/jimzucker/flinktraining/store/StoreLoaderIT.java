package io.github.jimzucker.flinktraining.store;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.Jedis;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The loader end to end: a transactional producer standing in for the Flink
 * sinks, a real broker, a real Redis.
 */
@Testcontainers
class StoreLoaderIT {

    private static final String TOPIC = "positions-by-symbol";

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.2"))
            // As in docker/compose.yml: topics exist only when created on purpose.
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

    @Container
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7.4.2")).withExposedPorts(6379);

    private static StoreConfig config(String group) {
        return new StoreConfig(KAFKA.getBootstrapServers(), Map.of(TOPIC, "pos:sym"), group,
                REDIS.getHost(), REDIS.getMappedPort(6379), 2, 1000, 0);
    }

    private static KafkaProducer<String, String> producer(String txId) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        producer.initTransactions();
        return producer;
    }

    private static String position(String symbol, long quantity) {
        return "{\"key\":\"" + symbol + "\",\"quantity\":" + quantity + ",\"symbol\":\"" + symbol + "\"}";
    }

    private static void createTopic() throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        try (Admin admin = Admin.create(props)) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 4, (short) 1))).all().get();
        }
    }

    private static void deleteTopic() throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        try (Admin admin = Admin.create(props)) {
            admin.deleteTopics(List.of(TOPIC)).all().get();
            for (int i = 0; i < 100 && admin.listTopics().names().get().contains(TOPIC); i++) {
                Thread.sleep(100);
            }
        }
    }

    private static void await(BooleanSupplier condition, Duration within, String what) throws Exception {
        long deadline = System.nanoTime() + within.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("not within " + within + ": " + what);
    }

    /** Runs the loader's threads until the returned flag is cleared. */
    private static AutoCloseable runLoader(StoreConfig config) throws Exception {
        AtomicBoolean running = new AtomicBoolean(true);
        LoaderMetrics metrics = new LoaderMetrics(0);
        TopicIds ids = new TopicIds(config.bootstrapServers(), config.topics().keySet());
        ids.refresh();
        java.util.concurrent.ScheduledExecutorService refresher =
                java.util.concurrent.Executors.newSingleThreadScheduledExecutor();
        refresher.scheduleWithFixedDelay(ids::refresh, 1, 1, java.util.concurrent.TimeUnit.SECONDS);
        List<StoreLoader.Worker> workers = List.of(
                new StoreLoader.Worker("it-0", config, ids, metrics, running),
                new StoreLoader.Worker("it-1", config, ids, metrics, running));
        List<Thread> threads = workers.stream().map(w -> new Thread(w, w.name)).toList();
        threads.forEach(Thread::start);
        return () -> {
            running.set(false);
            workers.forEach(StoreLoader.Worker::wakeup);
            for (Thread t : threads) {
                t.join(15_000);
            }
            refresher.shutdownNow();
            ids.close();
            metrics.close();
        };
    }

    @Test
    @DisplayName("committed values land, aborted ones never do, and a recreated topic replaces the old keys")
    void endToEnd() throws Exception {
        createTopic();
        Jedis jedis = new Jedis(REDIS.getHost(), REDIS.getMappedPort(6379));
        jedis.flushAll();

        try (KafkaProducer<String, String> producer = producer("it-tx")) {
            producer.beginTransaction();
            for (long q = 1; q <= 500; q++) {
                producer.send(new ProducerRecord<>(TOPIC, "AAPL", position("AAPL", q)));
                producer.send(new ProducerRecord<>(TOPIC, "MSFT", position("MSFT", -q)));
            }
            producer.commitTransaction();

            // A transaction the job aborted, as it would on a failed checkpoint.
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(TOPIC, "AAPL", position("AAPL", 999_999)));
            producer.send(new ProducerRecord<>(TOPIC, "IBM", position("IBM", 1)));
            producer.flush();
            producer.abortTransaction();

            producer.beginTransaction();
            producer.send(new ProducerRecord<>(TOPIC, "GOOG", position("GOOG", 7)));
            producer.commitTransaction();
        }

        try (AutoCloseable loader = runLoader(config("it-store"))) {
            // Each key is on its own partition, read by whichever thread owns it,
            // so one key landing says nothing about the others.
            await(() -> "7".equals(jedis.hget("pos:sym:GOOG", "quantity")), Duration.ofSeconds(60),
                    "GOOG loaded");
            await(() -> "500".equals(jedis.hget("pos:sym:AAPL", "quantity")), Duration.ofSeconds(60),
                    "AAPL at its last committed value");
            await(() -> "-500".equals(jedis.hget("pos:sym:MSFT", "quantity")), Duration.ofSeconds(60),
                    "MSFT at its last committed value");
            assertThat(jedis.exists("pos:sym:IBM")).as("from the aborted transaction").isFalse();
        }

        // A restarted loader in a fresh group reads everything again; nothing moves.
        try (AutoCloseable loader = runLoader(config("it-store-second"))) {
            Thread.sleep(5_000);
            assertThat(jedis.hget("pos:sym:AAPL", "quantity")).isEqualTo("500");
        }

        // The scale scripts delete and recreate the topics between cases.
        try (AutoCloseable loader = runLoader(config("it-store"))) {
            Thread.sleep(3_000);
            deleteTopic();
            createTopic();
            try (KafkaProducer<String, String> producer = producer("it-tx-2")) {
                producer.beginTransaction();
                producer.send(new ProducerRecord<>(TOPIC, "AAPL", position("AAPL", 3)));
                producer.commitTransaction();
            }
            await(() -> "3".equals(jedis.hget("pos:sym:AAPL", "quantity")), Duration.ofSeconds(60),
                    "AAPL from the recreated topic");
            await(() -> !jedis.exists("pos:sym:MSFT"), Duration.ofSeconds(30),
                    "MSFT, which only the old topic had, removed");
            assertThat(jedis.exists("pos:sym:GOOG")).isFalse();
        }
    }
}
