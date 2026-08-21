package io.github.jimzucker.flinktraining.job;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithKeyedOperator;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Window semantics under Flink's own harness, driven by explicit watermarks so
 * the boundary behaviour is exercised rather than assumed.
 */
class MarketValueAtCloseTest {

    private static final long MINUTE = 60_000L;

    private KeyedTwoInputStreamOperatorTestHarness<String, PositionState, PriceState, MarketValueState> harness;

    @BeforeEach
    void setUp() throws Exception {
        // The broadcast input is not keyed; the harness still requires a selector
        // for it, and the operator never uses the value.
        harness = new KeyedTwoInputStreamOperatorTestHarness<>(
                new CoBroadcastWithKeyedOperator<>(
                        new MarketValueAtClose(MINUTE), List.of(MarketValueAtClose.PRICES)),
                (KeySelector<PositionState, String>) position -> position.key,
                (KeySelector<PriceState, String>) price -> price.symbol,
                Types.STRING);
        harness.setup();
        harness.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        harness.close();
    }

    private void position(String key, String symbol, long qty, long eventTime) throws Exception {
        harness.processElement1(new org.apache.flink.streaming.runtime.streamrecord.StreamRecord<>(
                new PositionState(key, symbol, qty, "T1", 1L, eventTime), eventTime));
    }

    private void price(String symbol, String value, long eventTime) throws Exception {
        harness.processElement2(new org.apache.flink.streaming.runtime.streamrecord.StreamRecord<>(
                new PriceState(symbol, new BigDecimal(value), eventTime), eventTime));
    }

    private void watermark(long time) throws Exception {
        harness.processWatermark1(new org.apache.flink.streaming.api.watermark.Watermark(time));
        harness.processWatermark2(new org.apache.flink.streaming.api.watermark.Watermark(time));
    }

    private List<MarketValueState> emitted() {
        List<MarketValueState> out = new ArrayList<>();
        harness.extractOutputStreamRecords().forEach(r -> out.add(r.getValue()));
        return out;
    }

    @Test
    @DisplayName("market value is quantity at close times price at close")
    void valueAtClose() throws Exception {
        price("AAPL", "100.00", 1_000L);
        position("AAPL", "AAPL", 400, 1_000L);
        // A later price, still before the boundary, is the one that must be used.
        price("AAPL", "101.25", 50_000L);
        watermark(MINUTE);

        assertThat(emitted()).hasSize(1);
        MarketValueState mv = emitted().get(0);
        assertThat(mv.quantity).isEqualTo(400L);
        assertThat(mv.price).isEqualByComparingTo("101.25");
        assertThat(mv.marketValue).isEqualByComparingTo("40500.00");
        assertThat(mv.windowEnd).isEqualTo(MINUTE);
    }

    @Test
    @DisplayName("a price after the boundary belongs to the next window, not this one")
    void priceAfterBoundaryIsNotUsed() throws Exception {
        price("AAPL", "100.00", 1_000L);
        position("AAPL", "AAPL", 400, 1_000L);
        watermark(MINUTE);
        // Arrives in the second minute; must not retro-change the first.
        price("AAPL", "200.00", MINUTE + 10_000L);
        watermark(2 * MINUTE);

        assertThat(emitted()).extracting(m -> m.price.toPlainString())
                .containsExactly("100.00", "200.00");
    }

    @Test
    @DisplayName("a key with no activity still emits every window")
    void quietKeyStillEmits() throws Exception {
        price("AAPL", "100.00", 1_000L);
        position("AAPL", "AAPL", 400, 1_000L);

        watermark(MINUTE);
        watermark(2 * MINUTE);
        watermark(3 * MINUTE);

        // Nothing traded after the first minute, but the position is still held,
        // so the count per minute stays steady rather than varying with activity.
        assertThat(emitted()).hasSize(3);
        assertThat(emitted()).extracting(m -> m.windowEnd)
                .containsExactly(MINUTE, 2 * MINUTE, 3 * MINUTE);
        assertThat(emitted()).allSatisfy(m -> assertThat(m.quantity).isEqualTo(400L));
    }

    @Test
    @DisplayName("a short position produces a negative market value")
    void shortPositionIsNegative() throws Exception {
        price("AAPL", "100.00", 1_000L);
        position("AAPL", "AAPL", -300, 1_000L);
        watermark(MINUTE);

        assertThat(emitted().get(0).marketValue).isEqualByComparingTo("-30000.00");
    }

    @Test
    @DisplayName("a key with no price yet stays silent rather than reporting zero")
    void noPriceMeansNoFabricatedNumber() throws Exception {
        position("AAPL", "AAPL", 400, 1_000L);
        watermark(MINUTE);
        assertThat(emitted()).isEmpty();

        // Once a price exists the key starts reporting.
        price("AAPL", "100.00", MINUTE + 1_000L);
        watermark(2 * MINUTE);
        assertThat(emitted()).hasSize(1);
        assertThat(emitted().get(0).marketValue).isEqualByComparingTo("40000.00");
    }

    @Test
    @DisplayName("the last position before the boundary is the one used")
    void latestPositionWins() throws Exception {
        price("AAPL", "10.00", 1_000L);
        position("AAPL", "AAPL", 100, 1_000L);
        position("AAPL", "AAPL", 250, 30_000L);
        position("AAPL", "AAPL", 175, 59_000L);
        watermark(MINUTE);

        assertThat(emitted()).hasSize(1);
        assertThat(emitted().get(0).quantity).isEqualTo(175L);
        assertThat(emitted().get(0).marketValue).isEqualByComparingTo("1750.00");
    }

    @Test
    @DisplayName("keys are independent and each emits once per window")
    void keysAreIndependent() throws Exception {
        price("AAPL", "100.00", 1_000L);
        price("MSFT", "200.00", 1_000L);
        position("AAPL", "AAPL", 10, 1_000L);
        position("MSFT", "MSFT", 20, 1_000L);
        watermark(MINUTE);

        assertThat(emitted()).hasSize(2);
        assertThat(emitted()).extracting(m -> m.key + "=" + m.marketValue.toPlainString())
                .containsExactlyInAnyOrder("AAPL=1000.00", "MSFT=4000.00");
    }

    @Test
    @DisplayName("catching up emits every missed window, not just the latest")
    void catchUpEmitsEveryMissedWindow() throws Exception {
        // Processing running ahead of event time -- a replay, or recovery after a
        // restart. Registering timers from the current watermark rather than from
        // the record would skip straight to the next future boundary and emit
        // nothing at all for the windows the data actually covers.
        price("AAPL", "10.00", 1_000L);
        position("AAPL", "AAPL", 100, 1_000L);
        watermark(3 * MINUTE + 30_000L);

        assertThat(emitted()).extracting(m -> m.windowEnd)
                .containsExactly(MINUTE, 2 * MINUTE, 3 * MINUTE);
    }

    @Test
    @DisplayName("a window closes against its own price, not one from its future")
    void windowUsesItsOwnClosingPrice() throws Exception {
        // The price stream races ahead of the positions during a replay. Keeping
        // only the latest price would value the first window at the third
        // window's price: plausible-looking, and reconciling against nothing.
        position("AAPL", "AAPL", 100, 1_000L);
        price("AAPL", "10.00", 30_000L);
        price("AAPL", "20.00", MINUTE + 30_000L);
        price("AAPL", "30.00", 2 * MINUTE + 30_000L);

        watermark(3 * MINUTE);

        assertThat(emitted()).extracting(m -> m.windowEnd + "=" + m.price.toPlainString())
                .containsExactly(
                        MINUTE + "=10.00",
                        (2 * MINUTE) + "=20.00",
                        (3 * MINUTE) + "=30.00");
    }

    @Test
    @DisplayName("a window with no price of its own uses the last one before it")
    void fallsBackToTheLastPriceBefore() throws Exception {
        position("AAPL", "AAPL", 100, 1_000L);
        price("AAPL", "10.00", 30_000L);
        // No price at all during the second minute.
        watermark(2 * MINUTE);

        assertThat(emitted()).extracting(m -> m.windowEnd + "=" + m.price.toPlainString())
                .containsExactly(MINUTE + "=10.00", (2 * MINUTE) + "=10.00");
    }

    @Test
    @DisplayName("an account key uses its own symbol's price")
    void accountKeyUsesItsSymbolPrice() throws Exception {
        price("AAPL", "100.00", 1_000L);
        price("MSFT", "200.00", 1_000L);
        position("ACC1/SUB1/MSFT", "MSFT", 5, 1_000L);
        watermark(MINUTE);

        assertThat(emitted()).hasSize(1);
        assertThat(emitted().get(0).price).isEqualByComparingTo("200.00");
        assertThat(emitted().get(0).marketValue).isEqualByComparingTo("1000.00");
    }
}
