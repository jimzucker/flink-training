package io.github.jimzucker.flinktraining.job;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The accumulator under Flink's own harness, so keyed state and its restore path
 * are exercised rather than a plain object.
 */
class AccumulatePositionTest {

    private KeyedOneInputStreamOperatorTestHarness<String, PositionUpdate, PositionState> harness;

    @BeforeEach
    void setUp() throws Exception {
        harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new AccumulatePosition(0)),
                update -> update.key,
                Types.STRING);
        harness.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        harness.close();
    }

    private List<PositionState> emitted() {
        List<PositionState> out = new ArrayList<>();
        harness.extractOutputStreamRecords().forEach(r -> out.add(r.getValue()));
        return out;
    }

    private void send(String key, long signedQuantity, String tradeId) throws Exception {
        harness.processElement(
                new PositionUpdate(key, "AAPL", signedQuantity, tradeId, 1_000L), 1_000L);
    }

    @Test
    @DisplayName("a position is a running signed sum, emitted on every update")
    void runningSignedSum() throws Exception {
        send("AAPL", 400, "T1");
        send("AAPL", -100, "T2");
        send("AAPL", 250, "T3");

        assertThat(emitted()).extracting(p -> p.quantity)
                .containsExactly(400L, 300L, 550L);
        assertThat(emitted()).extracting(p -> p.updateCount)
                .containsExactly(1L, 2L, 3L);
    }

    @Test
    @DisplayName("a key whose sells exceed its buys goes short, not to zero")
    void goesNegative() throws Exception {
        send("AAPL", 100, "T1");
        send("AAPL", -400, "T2");

        assertThat(emitted()).extracting(p -> p.quantity).containsExactly(100L, -300L);
    }

    @Test
    @DisplayName("a key that nets to zero reports zero, and still emits")
    void netsToZero() throws Exception {
        send("AAPL", 400, "T1");
        send("AAPL", -400, "T2");

        // It has to emit: a key going flat is a real change, and a dashboard that
        // silently kept showing 400 would be wrong.
        assertThat(emitted()).extracting(p -> p.quantity).containsExactly(400L, 0L);
        assertThat(emitted()).hasSize(2);
    }

    @Test
    @DisplayName("keys are independent")
    void keysAreIndependent() throws Exception {
        send("AAPL", 400, "T1");
        send("MSFT", 100, "T2");
        send("AAPL", 100, "T3");

        assertThat(emitted()).extracting(p -> p.key + "=" + p.quantity)
                .containsExactly("AAPL=400", "MSFT=100", "AAPL=500");
    }

    @Test
    @DisplayName("every key starts flat")
    void startsFlat() throws Exception {
        send("GOOG", -250, "T1");
        assertThat(emitted()).extracting(p -> p.quantity).containsExactly(-250L);
    }

    @Test
    @DisplayName("the position survives a snapshot and restore")
    void survivesRestore() throws Exception {
        send("AAPL", 400, "T1");
        var snapshot = harness.snapshot(1L, 1L);
        harness.close();

        harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new AccumulatePosition(0)),
                update -> update.key,
                Types.STRING);
        harness.initializeState(snapshot);
        harness.open();

        send("AAPL", 100, "T2");
        assertThat(emitted()).extracting(p -> p.quantity).containsExactly(500L);
        assertThat(emitted()).extracting(p -> p.updateCount).containsExactly(2L);
    }

    @Test
    @DisplayName("the last trade id travels with the position")
    void carriesLastTradeId() throws Exception {
        send("AAPL", 400, "T000000007");
        assertThat(emitted().get(0).lastTradeId).isEqualTo("T000000007");
    }
}
