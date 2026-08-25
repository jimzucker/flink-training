package io.github.jimzucker.flinktraining.generator;

import io.github.jimzucker.flinktraining.model.Allocation;
import io.github.jimzucker.flinktraining.model.BlockTrade;
import io.github.jimzucker.flinktraining.model.ReferenceData;
import io.github.jimzucker.flinktraining.model.Side;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SplittableRandom;
import java.util.function.LongSupplier;

/**
 * Produces block trades as a seeded sequence.
 *
 * <p>The <em>content</em> of the sequence — symbol, side, quantity, allocations
 * — is a pure function of the seed, so the same
 * seed always produces the same trades in the same order. Only the timestamp
 * comes from outside.
 *
 * <p>Sides are drawn at random. A generator that only ever bought would prove
 * nothing about sign handling downstream.
 */
public final class BlockTradeGenerator implements Iterator<BlockTrade> {

    /** Each allocation takes this many shares of every block. */
    public static final long QUANTITY_PER_ALLOCATION = 100L;

    private final long seed;
    private final LongSupplier clock;

    private long sequence;

    /**
     * @param clock supplies the event time for each trade. Wall clock for a live
     *              demo, where latency is measured from this instant to the sink;
     *              a counter for a replay, where event times must be identical
     *              across runs.
     */
    public BlockTradeGenerator(long seed, LongSupplier clock) {
        this.seed = seed;
        this.clock = clock;
    }

    /** Event times advance by a fixed step from a fixed origin: a reproducible replay. */
    public static BlockTradeGenerator replaying(long seed, long startEpochMillis, long intervalMillis) {
        if (intervalMillis <= 0) {
            throw new IllegalArgumentException("intervalMillis must be positive");
        }
        long[] n = {0};
        return new BlockTradeGenerator(seed, () -> startEpochMillis + n[0]++ * intervalMillis);
    }

    /** Event times come from the wall clock, so end-to-end latency is measurable. */
    public static BlockTradeGenerator live(long seed) {
        return new BlockTradeGenerator(seed, System::currentTimeMillis);
    }

    /** An infinite sequence: the caller decides how many to take. */
    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public BlockTrade next() {
        BlockTrade trade = at(seed, sequence, clock.getAsLong());
        sequence++;
        return trade;
    }

    /**
     * The trade at a position in the sequence, derived from the seed and that
     * position alone.
     *
     * <p>Written this way so several threads can produce disjoint parts of one
     * sequence and still agree on its contents. A generator that advances a
     * shared {@link Random} makes trade <em>n</em> depend on every draw before
     * it, which is fine in one thread and meaningless in four. Here the draws for
     * position <em>n</em> come from a generator seeded by <em>n</em>, so the
     * sequence is the same whether one thread walks it or four divide it.
     *
     * <p>The seed is mixed rather than added: {@code new Random(seed + n)} for
     * consecutive n produces visibly correlated first draws, which would show up
     * as runs of the same symbol.
     */
    public static BlockTrade at(long seed, long sequence, long eventTime) {
        SplittableRandom random = new SplittableRandom(mix(seed, sequence));
        String symbol = ReferenceData.SYMBOLS.get(random.nextInt(ReferenceData.SYMBOLS.size()));
        Side side = random.nextBoolean() ? Side.BUY : Side.SELL;

        // Every block is split across all four accounts, which is what makes the
        // account-side rate four times the symbol-side rate.
        List<Allocation> allocations = new ArrayList<>(ReferenceData.ACCOUNTS.size());
        for (String account : ReferenceData.ACCOUNTS) {
            allocations.add(new Allocation(
                    account, ReferenceData.SUB_ACCOUNT, QUANTITY_PER_ALLOCATION));
        }
        long quantity = QUANTITY_PER_ALLOCATION * ReferenceData.ACCOUNTS.size();

        return new BlockTrade(
                tradeId(sequence), symbol, side, quantity, allocations, eventTime);
    }

    /** splitmix64's finalizer: scatters nearby inputs to unrelated outputs. */
    private static long mix(long seed, long sequence) {
        long z = seed * 0x9E3779B97F4A7C15L + sequence;
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }

    /** Zero-padded so trade ids sort in emission order in logs and dashboards. */
    static String tradeId(long sequence) {
        return String.format("T%09d", sequence);
    }
}
