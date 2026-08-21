package io.github.jimzucker.flinktraining.generator;

import io.github.jimzucker.flinktraining.model.Price;
import io.github.jimzucker.flinktraining.model.ReferenceData;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.function.LongSupplier;

/**
 * Produces one price per call, cycling through the symbols in order.
 *
 * <p>Round-robin rather than a burst of every symbol at once, so the configured
 * rate is a straightforward count of prices per second and every symbol is
 * repriced at an even cadence. No symbol can go quiet, which is one half of the
 * idleness problem the design has to avoid.
 *
 * <p>Prices walk in quarter increments from a round opening price. Quarters are
 * exactly representable and keep the arithmetic checkable by eye, which matters
 * when someone asks where a market value came from.
 */
public final class PriceGenerator implements Iterator<Price> {

    private static final BigDecimal STEP = new BigDecimal("0.25");
    private static final int MAX_STEPS = 4;
    /** Prices never walk below this, so a long run cannot drift to zero or negative. */
    private static final BigDecimal FLOOR = new BigDecimal("1.00");

    private final Random random;
    private final LongSupplier clock;
    private final Map<String, BigDecimal> current = new HashMap<>();

    private int cursor;

    public PriceGenerator(long seed, LongSupplier clock) {
        this.random = new Random(seed);
        this.clock = clock;
        this.current.putAll(ReferenceData.OPENING_PRICES);
    }

    public static PriceGenerator replaying(long seed, long startEpochMillis, long intervalMillis) {
        if (intervalMillis <= 0) {
            throw new IllegalArgumentException("intervalMillis must be positive");
        }
        long[] n = {0};
        return new PriceGenerator(seed, () -> startEpochMillis + n[0]++ * intervalMillis);
    }

    public static PriceGenerator live(long seed) {
        return new PriceGenerator(seed, System::currentTimeMillis);
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public Price next() {
        String symbol = ReferenceData.SYMBOLS.get(cursor);
        cursor = (cursor + 1) % ReferenceData.SYMBOLS.size();

        int steps = random.nextInt(2 * MAX_STEPS + 1) - MAX_STEPS;
        BigDecimal moved = current.get(symbol).add(STEP.multiply(BigDecimal.valueOf(steps)));
        if (moved.compareTo(FLOOR) < 0) {
            moved = FLOOR;
        }
        current.put(symbol, moved);
        return new Price(symbol, moved, clock.getAsLong());
    }

    /** Latest price per symbol, for assertions and for logging what the demo is running with. */
    public Map<String, BigDecimal> currentPrices() {
        return Map.copyOf(current);
    }
}
