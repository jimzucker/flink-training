package io.github.jimzucker.flinktraining.generator;

import io.github.jimzucker.flinktraining.model.Allocation;
import io.github.jimzucker.flinktraining.model.BlockTrade;
import io.github.jimzucker.flinktraining.model.ReferenceData;
import io.github.jimzucker.flinktraining.model.Side;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.function.LongSupplier;

/**
 * Produces block trades as a seeded sequence.
 *
 * <p>The <em>content</em> of the sequence — symbol, side, quantity, which
 * sub-accounts are allocated to — is a pure function of the seed, so the same
 * seed always produces the same trades in the same order. Only the timestamp
 * comes from outside.
 *
 * <p>Sides are drawn at random. A generator that only ever bought would prove
 * nothing about sign handling downstream.
 */
public final class BlockTradeGenerator implements Iterator<BlockTrade> {

    /** Each allocation takes this many shares of every block. */
    public static final long QUANTITY_PER_ALLOCATION = 100L;

    private final Random random;
    private final LongSupplier clock;

    private long sequence;

    /**
     * @param clock supplies the event time for each trade. Wall clock for a live
     *              demo, where latency is measured from this instant to the sink;
     *              a counter for a replay, where event times must be identical
     *              across runs.
     */
    public BlockTradeGenerator(long seed, LongSupplier clock) {
        this.random = new Random(seed);
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
        String symbol = ReferenceData.SYMBOLS.get(random.nextInt(ReferenceData.SYMBOLS.size()));
        Side side = random.nextBoolean() ? Side.BUY : Side.SELL;

        // Every block is split across all four accounts, one sub-account each,
        // which is what makes the account-side rate four times the symbol-side
        // rate while the key space spans every account/sub-account/symbol triple.
        List<Allocation> allocations = new ArrayList<>(ReferenceData.ACCOUNTS.size());
        for (String account : ReferenceData.ACCOUNTS) {
            String subAccount = ReferenceData.SUB_ACCOUNTS.get(
                    random.nextInt(ReferenceData.SUB_ACCOUNTS.size()));
            allocations.add(new Allocation(account, subAccount, QUANTITY_PER_ALLOCATION));
        }
        long quantity = QUANTITY_PER_ALLOCATION * ReferenceData.ACCOUNTS.size();

        BlockTrade trade = new BlockTrade(
                tradeId(sequence), symbol, side, quantity, allocations, clock.getAsLong());
        sequence++;
        return trade;
    }

    /** Zero-padded so trade ids sort in emission order in logs and dashboards. */
    static String tradeId(long sequence) {
        return String.format("T%09d", sequence);
    }
}
