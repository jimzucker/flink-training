package io.github.jimzucker.flinktraining.job;

import io.github.jimzucker.flinktraining.model.Allocation;
import io.github.jimzucker.flinktraining.model.BlockTrade;
import io.github.jimzucker.flinktraining.model.Side;
import io.github.jimzucker.flinktraining.model.Json;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * {@link SplitOnce} must emit exactly what {@link ToSymbolUpdate} and
 * {@link SplitByAllocation} emit between them.
 *
 * <p>The point of the parse-once arm is to change the cost of the pipeline and
 * nothing else. If the two arms produced different updates, the throughput
 * comparison would be measuring two different jobs — which is the mistake
 * [`apples to apples`](../../../../../../../../../docs/skill-validation/findings.md)
 * exists to prevent. This proves equivalence at the cheapest level there is,
 * before any rig time is spent.
 */
class ParseOnceEquivalenceTest {

    /** Collects the main output, as the symbol aggregation would see it. */
    private static final class Sink implements Collector<PositionUpdate> {
        final List<PositionUpdate> got = new ArrayList<>();
        public void collect(PositionUpdate u) { got.add(u); }
        public void close() { }
    }

    /** Captures the side output, as the account aggregation would see it. */
    private static final class Ctx extends ProcessFunction<String, PositionUpdate>.Context {
        final List<PositionUpdate> side = new ArrayList<>();
        Ctx(ProcessFunction<String, PositionUpdate> f) { f.super(); }
        public Long timestamp() { return null; }
        public org.apache.flink.streaming.api.TimerService timerService() { return null; }
        public <X> void output(OutputTag<X> tag, X value) {
            assertEquals(SplitOnce.BY_ACCOUNT.getId(), tag.getId(), "unexpected side output");
            side.add((PositionUpdate) value);
        }
    }

    /** The jobs module does not depend on the generator, so build the order here. */
    private static String order(int fillerFields) {
        java.util.Map<String, String> filler = new java.util.LinkedHashMap<>();
        for (int i = 1; i <= fillerFields; i++) {
            filler.put(String.format("f%02d", i), "XXXX");
        }
        List<Allocation> allocations = List.of(
                new Allocation("ACC1", "SUB1", 100, filler),
                new Allocation("ACC2", "SUB1", 100, filler),
                new Allocation("ACC3", "SUB1", 100, filler),
                new Allocation("ACC4", "SUB1", 100, filler));
        return Json.toJson(new BlockTrade("T1", "AAPL", Side.BUY, 400, allocations, 1_789_000_000_000L));
    }

    private static void assertSame(PositionUpdate a, PositionUpdate b, String where) {
        assertEquals(a.key, b.key, where + ": key");
        assertEquals(a.symbol, b.symbol, where + ": symbol");
        assertEquals(a.signedQuantity, b.signedQuantity, where + ": signedQuantity");
        assertEquals(a.tradeId, b.tradeId, where + ": tradeId");
        assertEquals(a.eventTime, b.eventTime, where + ": eventTime");
        assertEquals(a.filler == null ? null : List.of(a.filler),
                b.filler == null ? null : List.of(b.filler), where + ": filler");
    }

    private void equivalentAt(int fillerFields) throws Exception {
        String json = order(fillerFields);

        Sink twoSymbol = new Sink();
        new ToSymbolUpdate().flatMap(json, twoSymbol);
        Sink twoAccount = new Sink();
        new SplitByAllocation().flatMap(json, twoAccount);

        SplitOnce once = new SplitOnce();
        Sink oneSymbol = new Sink();
        Ctx ctx = new Ctx(once);
        once.processElement(json, ctx, oneSymbol);

        assertEquals(twoSymbol.got.size(), oneSymbol.got.size(), "symbol updates");
        assertEquals(twoAccount.got.size(), ctx.side.size(), "account updates");
        assertFalse(ctx.side.isEmpty(), "the side output carried nothing");

        for (int i = 0; i < twoSymbol.got.size(); i++) {
            assertSame(twoSymbol.got.get(i), oneSymbol.got.get(i), "symbol[" + i + "]");
        }
        for (int i = 0; i < twoAccount.got.size(); i++) {
            assertSame(twoAccount.got.get(i), ctx.side.get(i), "account[" + i + "]");
        }
    }

    @Test
    @DisplayName("one parse emits what two parses emit, on a plain order")
    void equivalentWithoutFiller() throws Exception {
        equivalentAt(0);
    }

    @Test
    @DisplayName("one parse emits what two parses emit, on a 2 KB order")
    void equivalentWithFiller() throws Exception {
        equivalentAt(32);
    }

    @Test
    @DisplayName("the 2 KB order still carries its filler through the side output")
    void fillerSurvivesTheSideOutput() throws Exception {
        SplitOnce once = new SplitOnce();
        Sink main = new Sink();
        Ctx ctx = new Ctx(once);
        once.processElement(order(32), ctx, main);

        assertEquals(1, main.got.size(), "one symbol update per order");
        assertEquals(null, main.got.get(0).filler, "the symbol side carries no filler");
        for (PositionUpdate u : ctx.side) {
            assertNotNull(u.filler, "an account update lost its filler");
            assertEquals(64, u.filler.length, "32 fields as name,value pairs");
        }
    }
}
