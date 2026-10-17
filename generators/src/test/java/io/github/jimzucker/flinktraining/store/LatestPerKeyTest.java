package io.github.jimzucker.flinktraining.store;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class LatestPerKeyTest {

    private static ConsumerRecord<byte[], byte[]> record(String topic, int partition, long offset,
                                                         String key, String value) {
        return new ConsumerRecord<>(topic, partition, offset,
                key == null ? null : key.getBytes(StandardCharsets.UTF_8),
                value == null ? null : value.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("the last record for a key wins, and the earlier ones are not written")
    void lastWins() {
        LatestPerKey batch = new LatestPerKey();
        batch.add(record("positions-by-symbol", 0, 10, "AAPL", "{\"quantity\":1}"));
        batch.add(record("positions-by-symbol", 0, 11, "MSFT", "{\"quantity\":5}"));
        batch.add(record("positions-by-symbol", 0, 12, "AAPL", "{\"quantity\":3}"));

        List<LatestPerKey.Latest> latest = batch.latest();
        assertThat(batch.seen()).isEqualTo(3);
        assertThat(latest).hasSize(2);
        LatestPerKey.Latest aapl = latest.stream().filter(l -> l.key().equals("AAPL")).findFirst().orElseThrow();
        assertThat(aapl.offset()).isEqualTo(12);
        assertThat(new String(aapl.value(), StandardCharsets.UTF_8)).isEqualTo("{\"quantity\":3}");
    }

    @Test
    @DisplayName("the same key on two topics is two entries")
    void topicIsPartOfTheKey() {
        LatestPerKey batch = new LatestPerKey();
        batch.add(record("positions-by-symbol", 0, 1, "AAPL", "{}"));
        batch.add(record("mv-by-symbol", 0, 1, "AAPL", "{}"));
        assertThat(batch.latest()).extracting(LatestPerKey.Latest::topic)
                .containsExactlyInAnyOrder("positions-by-symbol", "mv-by-symbol");
    }

    @Test
    @DisplayName("a record with no key or no value is counted as read and not written")
    void nullsSkipped() {
        LatestPerKey batch = new LatestPerKey();
        batch.add(record("positions-by-symbol", 0, 1, null, "{}"));
        batch.add(record("positions-by-symbol", 0, 2, "AAPL", null));
        assertThat(batch.seen()).isEqualTo(2);
        assertThat(batch.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("decimals are stored as written on the topic, never in scientific notation")
    void plainDecimals() {
        List<String> out = new ArrayList<>();
        RedisStore.fields(("{\"key\":\"A1/S1/AAPL\",\"marketValue\":12000000.50,"
                + "\"price\":1.00E+2,\"quantity\":-40}").getBytes(StandardCharsets.UTF_8), out);
        assertThat(out).containsExactly(
                "key", "A1/S1/AAPL",
                "marketValue", "12000000.50",
                "price", "100",
                "quantity", "-40");
    }

    @Test
    @DisplayName("STORE_TOPICS reads as topic=prefix pairs, and anything else stops the loader")
    void topicSpec() {
        assertThat(StoreConfig.parseTopics(StoreConfig.DEFAULT_TOPICS))
                .containsEntry("positions-by-account", "pos:acct")
                .hasSize(4);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> StoreConfig.parseTopics("orders"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
