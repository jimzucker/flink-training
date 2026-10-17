package io.github.jimzucker.flinktraining.store;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.Jedis;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The write-if-newer script against a real Redis, the same version compose runs.
 */
@Testcontainers
class RedisStoreIT {

    private static final String TOPIC = "positions-by-symbol";
    private static final Map<String, String> PREFIXES = Map.of(TOPIC, "pos:sym");

    @Container
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7.4.2")).withExposedPorts(6379);

    private Jedis jedis;

    @BeforeEach
    void clean() {
        jedis = new Jedis(REDIS.getHost(), REDIS.getMappedPort(6379));
        jedis.flushAll();
    }

    private static LatestPerKey.Latest entry(long offset, long quantity) {
        return new LatestPerKey.Latest(TOPIC, 2, offset, "AAPL",
                ("{\"key\":\"AAPL\",\"quantity\":" + quantity + "}").getBytes(StandardCharsets.UTF_8));
    }

    private int write(RedisStore store, String topicId, LatestPerKey.Latest... entries) {
        return store.write(List.of(entries), PREFIXES, Map.of(TOPIC, topicId));
    }

    @Test
    @DisplayName("a newer offset replaces the value, with where it came from beside it")
    void newerWins() {
        try (RedisStore store = new RedisStore(REDIS.getHost(), REDIS.getMappedPort(6379))) {
            assertThat(write(store, "id-1", entry(10, 1))).isEqualTo(1);
            assertThat(write(store, "id-1", entry(11, 2))).isEqualTo(1);
        }
        assertThat(jedis.hgetAll("pos:sym:AAPL")).containsAllEntriesOf(Map.of(
                "quantity", "2", "kafka_offset", "11", "kafka_partition", "2", "kafka_topic_id", "id-1"));
    }

    @Test
    @DisplayName("a record read again after a restart never moves a key back")
    void olderNeverWins() {
        try (RedisStore store = new RedisStore(REDIS.getHost(), REDIS.getMappedPort(6379))) {
            write(store, "id-1", entry(11, 2));
            assertThat(write(store, "id-1", entry(10, 1))).isZero();
            assertThat(write(store, "id-1", entry(11, 2))).isZero();
        }
        assertThat(jedis.hget("pos:sym:AAPL", "quantity")).isEqualTo("2");
    }

    @Test
    @DisplayName("a recreated topic starts its offsets again, and its records still win")
    void newTopicIdWins() {
        try (RedisStore store = new RedisStore(REDIS.getHost(), REDIS.getMappedPort(6379))) {
            write(store, "id-1", entry(5_000, 2));
            assertThat(write(store, "id-2", entry(0, 7))).isEqualTo(1);
        }
        assertThat(jedis.hget("pos:sym:AAPL", "quantity")).isEqualTo("7");
    }

    @Test
    @DisplayName("a Redis that forgot the script, after a restart, gets it again and the batch still lands")
    void scriptReloaded() {
        try (RedisStore store = new RedisStore(REDIS.getHost(), REDIS.getMappedPort(6379))) {
            jedis.scriptFlush();
            assertThat(write(store, "id-1", entry(1, 9))).isEqualTo(1);
        }
        assertThat(jedis.hget("pos:sym:AAPL", "quantity")).isEqualTo("9");
    }

    @Test
    @DisplayName("keys only the old topic had are removed; the new topic's keys and other prefixes stay")
    void removeOld() {
        jedis.hset("pos:sym:OLD", Map.of("kafka_topic_id", "id-1"));
        jedis.hset("pos:sym:NEW", Map.of("kafka_topic_id", "id-2"));
        jedis.hset("pos:acct:A1/S1/OLD", Map.of("kafka_topic_id", "id-1"));
        try (RedisStore store = new RedisStore(REDIS.getHost(), REDIS.getMappedPort(6379))) {
            assertThat(store.removeOtherTopicIds("pos:sym", "id-2")).isEqualTo(1);
        }
        assertThat(jedis.exists("pos:sym:OLD")).isFalse();
        assertThat(jedis.exists("pos:sym:NEW")).isTrue();
        assertThat(jedis.exists("pos:acct:A1/S1/OLD")).isTrue();
    }
}
