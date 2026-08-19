package io.github.jimzucker.flinktraining.generator;

import io.github.jimzucker.flinktraining.model.Price;
import io.github.jimzucker.flinktraining.model.ReferenceData;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Produces a price for every symbol on each tick, as a pure seeded sequence.
 *
 * <p>Prices walk in quarter increments from a round opening price. Quarters are
 * exactly representable and keep the arithmetic checkable by eye, which matters
 * when someone asks where a market value came from.
 *
 * <p>Emitting all symbols on every tick is deliberate: it means no symbol can go
 * quiet, so the price side never stalls a watermark on its own.
 */
public final class PriceGenerator implements Iterator<List<Price>> {

    private static final BigDecimal STEP = new BigDecimal("0.25");
    private static final int MAX_STEPS = 4;
    /** Prices never walk below this, so a long run cannot drift to zero or negative. */
    private static final BigDecimal FLOOR = new BigDecimal("1.00");

    private final Random random;
    private final long startEpochMillis;
    private final long intervalMillis;
    private final Map<String, BigDecimal> current = new HashMap<>();

    private long tick;

    public PriceGenerator(long seed, long startEpochMillis, long intervalMillis) {
        if (intervalMillis <= 0) {
            throw new IllegalArgumentException("intervalMillis must be positive");
        }
        this.random = new Random(seed);
        this.startEpochMillis = startEpochMillis;
        this.intervalMillis = intervalMillis;
        this.current.putAll(ReferenceData.OPENING_PRICES);
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    /** One price per symbol, in a stable symbol order so the sequence is byte-stable. */
    @Override
    public List<Price> next() {
        long eventTime = startEpochMillis + tick * intervalMillis;
        List<Price> prices = new ArrayList<>(ReferenceData.SYMBOLS.size());

        for (String symbol : ReferenceData.SYMBOLS) {
            int steps = random.nextInt(2 * MAX_STEPS + 1) - MAX_STEPS;
            BigDecimal moved = current.get(symbol).add(STEP.multiply(BigDecimal.valueOf(steps)));
            if (moved.compareTo(FLOOR) < 0) {
                moved = FLOOR;
            }
            current.put(symbol, moved);
            prices.add(new Price(symbol, moved, eventTime));
        }

        tick++;
        return prices;
    }

    /** Latest price per symbol, for assertions and for logging what the demo is running with. */
    public Map<String, BigDecimal> currentPrices() {
        return Map.copyOf(current);
    }
}
