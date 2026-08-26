package io.github.jimzucker.flinktraining.job;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Emits one market value per key per window, taken at the window close.
 *
 * <p>Implemented with event-time timers on window boundaries rather than a
 * windowed aggregate, because the required semantics are a snapshot at an
 * instant, not a summary of an interval: the position as of the boundary,
 * multiplied by the last price at or before that boundary.
 *
 * <p>Prices are held <em>per window</em>, not as a single latest value. Keeping
 * only the latest is correct when records arrive in step with the clock, but
 * wrong the moment processing runs ahead of event time — during a replay, or
 * after a restart while catching up, the price stream races ahead and a window
 * closes against a price from its future. The number would still look plausible
 * and reconcile against nothing.
 *
 * <p>Once a key has been seen it keeps emitting every window, whether or not it
 * traded, so the count per minute is steady rather than varying with activity.
 */
public class MarketValueAtClose
        extends KeyedBroadcastProcessFunction<String, PositionState, PriceState, MarketValueState> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(MarketValueAtClose.class);

    /** Closing price per symbol per window, keyed {@code SYMBOL|windowEnd}. */
    public static final MapStateDescriptor<String, BigDecimal> PRICES =
            new MapStateDescriptor<>("closing-price-by-symbol-window", Types.STRING, Types.BIG_DEC);

    private static final String SEPARATOR = "|";

    /** How far back a window may look for the last price, and how much is retained. */
    private static final int MAX_LOOKBACK_WINDOWS = 240;

    private final long windowMillis;

    /** Position as of each window close, keyed by window end. */
    private transient MapState<Long, Long> closingQuantity;
    /** The trade that last moved the position in each window, for traceability. */
    private transient MapState<Long, String> closingTradeId;
    /** Event time of the position recorded for each window, so a later one wins. */
    private transient MapState<Long, Long> closingAt;
    /** Update count of that position, which breaks ties on event time. */
    private transient MapState<Long, Long> closingCount;
    private transient ValueState<String> symbol;
    /** The boundary a timer is already registered for, so it is set once per window. */
    private transient ValueState<Long> pendingBoundary;

    /**
     * Distinct keys this subtask has emitted a market value for. Summed across
     * subtasks it is the key count the expected-output table states.
     */
    private transient Set<String> keysEmitted;

    public MarketValueAtClose(long windowMillis) {
        this.windowMillis = windowMillis;
    }

    static String priceKey(String symbol, long windowEnd) {
        return symbol + SEPARATOR + windowEnd;
    }

    @Override
    public void open(Configuration parameters) {
        closingQuantity = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("closing-quantity-by-window", Types.LONG, Types.LONG));
        closingTradeId = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("closing-trade-by-window", Types.LONG, Types.STRING));
        closingAt = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("closing-at-by-window", Types.LONG, Types.LONG));
        closingCount = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("closing-count-by-window", Types.LONG, Types.LONG));
        symbol = getRuntimeContext().getState(new ValueStateDescriptor<>("symbol", Types.STRING));
        pendingBoundary = getRuntimeContext().getState(new ValueStateDescriptor<>("pendingBoundary", Types.LONG));

        keysEmitted = new HashSet<>();
        getRuntimeContext().getMetricGroup().gauge("activeKeys", () -> keysEmitted.size());
    }

    @Override
    public void processElement(PositionState position, ReadOnlyContext ctx,
                               Collector<MarketValueState> out) throws Exception {
        symbol.update(position.symbol);

        // Recorded against the window the position falls in, not as a single
        // latest value. Watermarks are emitted periodically rather than per
        // record, so several positions from after a boundary are routinely
        // processed before the timer for that boundary fires. Keeping only the
        // latest would then close the window against a position from its own
        // future -- the same mistake as keeping only the latest price, and just
        // as invisible, since the number still looks like a position.
        // The closing value is the one with the greatest event time in the window,
        // not simply the last to arrive. Orders are keyed by trade id and spread
        // across partitions, so Part 1 emits a symbol's positions from several
        // partitions at once and they do not arrive in event-time order.
        //
        // Two trades on the same key can share a millisecond, and then event time
        // alone does not say which position closed the window. Taking whichever
        // arrived last is not an answer: positions are keyed by trade id and
        // spread across partitions, so arrival order varies between runs and the
        // window could close on the running total from *before* the other trade,
        // understating the position. updateCount is the accumulation sequence for
        // the key, so the greater one is genuinely later -- a tie-break on the
        // data rather than on timing, and identical on every replay.
        long window = Windows.nextBoundary(position.eventTime, windowMillis);
        Long recordedAt = closingAt.get(window);
        Long recordedCount = closingCount.get(window);
        boolean later = recordedAt == null
                || position.eventTime > recordedAt
                || (position.eventTime == recordedAt && position.updateCount > recordedCount);
        if (later) {
            closingQuantity.put(window, position.quantity);
            closingTradeId.put(window, position.lastTradeId);
            closingAt.put(window, position.eventTime);
            closingCount.put(window, position.updateCount);
        }

        // Register from the record's own event time, not from the current
        // watermark. When processing runs ahead -- a replay, or catching up after
        // a restart -- the watermark is already past this record, and registering
        // from it would skip every window the record belongs to. A timer for a
        // boundary the watermark has passed fires immediately, which is exactly
        // the catch-up behaviour wanted: one emission per window, in order.
        registerNext(position.eventTime, ctx.timerService());
    }

    @Override
    public void processBroadcastElement(PriceState price, Context ctx,
                                        Collector<MarketValueState> out) throws Exception {
        BroadcastState<String, BigDecimal> prices = ctx.getBroadcastState(PRICES);
        long window = Windows.nextBoundary(price.eventTime, windowMillis);

        // Prices for a symbol arrive in order, so the last one written for a
        // window is the one that closed it.
        prices.put(priceKey(price.symbol, window), price.price);
        prune(prices, price.symbol, window);
    }

    @Override
    public void onTimer(long windowEnd, OnTimerContext ctx, Collector<MarketValueState> out)
            throws Exception {
        String sym = symbol.value();
        Long qty = closingAt(closingQuantity, windowEnd);
        if (qty == null || sym == null) {
            return;
        }

        BigDecimal price = closingPrice(ctx.getBroadcastState(PRICES), sym, windowEnd);
        if (price == null) {
            // No price at or before this boundary. A market value of zero would be
            // a fabricated number; staying silent is the honest option and the
            // next window carries the key once a price exists.
            LOG.debug("no price at or before {} for {}", windowEnd, sym);
        } else {
            keysEmitted.add(ctx.getCurrentKey());
            out.collect(new MarketValueState(
                    ctx.getCurrentKey(), sym, qty, price,
                    price.multiply(BigDecimal.valueOf(qty)),
                    closingAt(closingTradeId, windowEnd), windowEnd));
        }

        pruneClosing(windowEnd);

        // Keep emitting every window, traded or not.
        pendingBoundary.update(null);
        registerNext(windowEnd, ctx.timerService());
    }

    /**
     * The last value recorded at or before {@code windowEnd}, searching back
     * window by window. A window with no activity inherits the previous one,
     * which is what makes a quiet key keep reporting the position it still holds.
     */
    private <T> T closingAt(MapState<Long, T> state, long windowEnd) throws Exception {
        for (int back = 0; back < MAX_LOOKBACK_WINDOWS; back++) {
            T value = state.get(windowEnd - (long) back * windowMillis);
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    /** Collapses history older than the window just emitted into that window. */
    private void pruneClosing(long windowEnd) throws Exception {
        Long carried = closingQuantity.get(windowEnd);
        String carriedTrade = closingTradeId.get(windowEnd);
        Long carriedCount = closingCount.get(windowEnd);
        if (carried == null) {
            carried = closingAt(closingQuantity, windowEnd);
            carriedTrade = closingAt(closingTradeId, windowEnd);
            carriedCount = closingAt(closingCount, windowEnd);
        }
        List<Long> expired = new ArrayList<>();
        for (Long window : closingQuantity.keys()) {
            if (window <= windowEnd) {
                expired.add(window);
            }
        }
        for (Long window : expired) {
            closingQuantity.remove(window);
            closingTradeId.remove(window);
            closingAt.remove(window);
            closingCount.remove(window);
        }
        if (carried != null) {
            closingQuantity.put(windowEnd, carried);
            closingTradeId.put(windowEnd, carriedTrade);
            closingAt.put(windowEnd, windowEnd);
            closingCount.put(windowEnd, carriedCount == null ? 0L : carriedCount);
        }
    }

    /** The last price at or before {@code windowEnd}, searching back window by window. */
    private BigDecimal closingPrice(ReadOnlyBroadcastState<String, BigDecimal> prices,
                                    String sym, long windowEnd) throws Exception {
        for (int back = 0; back < MAX_LOOKBACK_WINDOWS; back++) {
            BigDecimal price = prices.get(priceKey(sym, windowEnd - back * windowMillis));
            if (price != null) {
                return price;
            }
        }
        return null;
    }

    /** Bounds the retained history so a long run cannot grow state without limit. */
    private void prune(BroadcastState<String, BigDecimal> prices, String sym, long newest)
            throws Exception {
        long cutoff = newest - (long) MAX_LOOKBACK_WINDOWS * windowMillis;
        List<String> expired = new ArrayList<>();
        for (Map.Entry<String, BigDecimal> entry : prices.immutableEntries()) {
            String key = entry.getKey();
            int at = key.lastIndexOf(SEPARATOR);
            if (at < 0 || !key.startsWith(sym + SEPARATOR)) {
                continue;
            }
            if (Long.parseLong(key.substring(at + 1)) < cutoff) {
                expired.add(key);
            }
        }
        for (String key : expired) {
            prices.remove(key);
        }
    }

    private void registerNext(long from, TimerService timers) throws Exception {
        long boundary = Windows.nextBoundary(from, windowMillis);
        Long already = pendingBoundary.value();
        if (already != null && already >= boundary) {
            return;
        }
        timers.registerEventTimeTimer(boundary);
        pendingBoundary.update(boundary);
    }
}
