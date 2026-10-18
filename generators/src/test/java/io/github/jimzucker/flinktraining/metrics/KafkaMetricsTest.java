package io.github.jimzucker.flinktraining.metrics;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaMetricsTest {

    @Test
    @DisplayName("a key seen twice is one distinct key, and the set grows past its first size")
    void distinctKeys() {
        KafkaMetrics.LongSet keys = new KafkaMetrics.LongSet();
        for (int round = 0; round < 2; round++) {
            for (int i = 0; i < 50_000; i++) {
                keys.add(KafkaMetrics.hash(("ACC" + i + "/SUB1/AAPL").getBytes(StandardCharsets.UTF_8)));
            }
        }
        assertThat(keys.size()).isEqualTo(50_000);
        assertThat(keys.capped).isFalse();
    }

    @Test
    @DisplayName("no key hashes to zero, which the set uses to mean an empty slot")
    void zeroIsNeverAHash() {
        assertThat(KafkaMetrics.hash(new byte[0])).isNotZero();
    }

    @Test
    @DisplayName("a group name with a quote or a backslash stays one Prometheus label")
    void labelsEscaped() {
        assertThat(KafkaMetrics.esc("a\"b\\c\nd")).isEqualTo("a\\\"b\\\\c\\nd");
    }
}
