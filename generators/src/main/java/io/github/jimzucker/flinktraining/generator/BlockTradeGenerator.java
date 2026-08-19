package io.github.jimzucker.flinktraining.generator;

import io.github.jimzucker.flinktraining.model.Allocation;
import io.github.jimzucker.flinktraining.model.BlockTrade;
import io.github.jimzucker.flinktraining.model.ReferenceData;
import io.github.jimzucker.flinktraining.model.Side;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * Produces block trades as a pure, seeded sequence.
 *
 * <p>There is no clock in here and no Kafka. Event times come from a counter, so
 * the sequence a seed produces is the same whether it is consumed as fast as a
 * test can pull it or paced at ten a second by a publisher. That separation is
 * what lets the demo be both randomised and exactly reproducible: the randomness
 * is a fixed sequence that replays identically, and a slow consumer can never
 * change the data.
 *
 * <p>Sides are drawn at random. A generator that only ever bought would prove
 * nothing about sign handling downstream.
 */
public final class BlockTradeGenerator implements Iterator<BlockTrade> {

    /** Each account takes this many shares of every block. */
    public static final long QUANTITY_PER_ALLOCATION = 100L;

    private final Random random;
    private final long startEpochMillis;
    private final long intervalMillis;

    private long sequence;

    public BlockTradeGenerator(long seed, long startEpochMillis, long intervalMillis) {
        if (intervalMillis <= 0) {
            throw new IllegalArgumentException("intervalMillis must be positive");
        }
        this.random = new Random(seed);
        this.startEpochMillis = startEpochMillis;
        this.intervalMillis = intervalMillis;
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

        // Every block is split across all four accounts, which is what makes the
        // account-side rate four times the symbol-side rate.
        List<Allocation> allocations = new ArrayList<>(ReferenceData.ACCOUNTS.size());
        for (String account : ReferenceData.ACCOUNTS) {
            allocations.add(new Allocation(account, ReferenceData.SUB_ACCOUNT, QUANTITY_PER_ALLOCATION));
        }
        long quantity = QUANTITY_PER_ALLOCATION * ReferenceData.ACCOUNTS.size();

        BlockTrade trade = new BlockTrade(
                tradeId(sequence),
                symbol,
                side,
                quantity,
                allocations,
                startEpochMillis + sequence * intervalMillis);
        sequence++;
        return trade;
    }

    /** Zero-padded so trade ids sort in emission order in logs and dashboards. */
    static String tradeId(long sequence) {
        return String.format("T%09d", sequence);
    }
}
