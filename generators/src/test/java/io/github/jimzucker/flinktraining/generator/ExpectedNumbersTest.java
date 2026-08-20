package io.github.jimzucker.flinktraining.generator;

import io.github.jimzucker.flinktraining.model.AccountKey;
import io.github.jimzucker.flinktraining.model.Allocation;
import io.github.jimzucker.flinktraining.model.BlockTrade;
import io.github.jimzucker.flinktraining.model.Price;
import io.github.jimzucker.flinktraining.model.ReferenceData;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The assignment's expected-output table, as executable assertions.
 *
 * <p>These are the numbers the demo is judged against, so they are asserted
 * against the generator rather than eyeballed on a dashboard. Each test names
 * the sink it protects.
 */
class ExpectedNumbersTest {

    private static final long SEED = GeneratorConfig.DEFAULT_SEED;
    private static final long START = GeneratorConfig.REPLAY_START_EPOCH_MILLIS;
    private static final int TRADES_PER_SECOND = GeneratorConfig.DEMO_TRADES_PER_SECOND;

    private static List<BlockTrade> oneSecondOfTrades() {
        BlockTradeGenerator generator =
                BlockTradeGenerator.replaying(SEED, START, 1_000L / TRADES_PER_SECOND);
        List<BlockTrade> trades = new ArrayList<>();
        for (int i = 0; i < TRADES_PER_SECOND; i++) {
            trades.add(generator.next());
        }
        return trades;
    }

    @Test
    @DisplayName("input: 10 trades per second")
    void tenTradesPerSecond() {
        List<BlockTrade> trades = oneSecondOfTrades();
        assertThat(trades).hasSize(10);

        // One second of event time, exclusive of the next second's first trade.
        long span = trades.get(trades.size() - 1).eventTime() - trades.get(0).eventTime();
        assertThat(span).isEqualTo(900L);
    }

    @Test
    @DisplayName("sink 3: 10 updates/sec over 4 unique symbol keys")
    void sinkThree() {
        List<BlockTrade> trades = oneSecondOfTrades();

        assertThat(trades).hasSize(10);
        Set<String> symbolKeys = trades.stream().map(BlockTrade::symbol).collect(Collectors.toSet());
        assertThat(symbolKeys).hasSizeLessThanOrEqualTo(ReferenceData.SYMBOL_KEY_COUNT);

        // Over a longer run every symbol must appear, or the key count on the
        // dashboard would never reach 4.
        BlockTradeGenerator longer = BlockTradeGenerator.replaying(SEED, START, 100L);
        Set<String> seen = new HashSet<>();
        for (int i = 0; i < 200; i++) {
            seen.add(longer.next().symbol());
        }
        assertThat(seen).containsExactlyInAnyOrderElementsOf(ReferenceData.SYMBOLS);
    }

    @Test
    @DisplayName("sink 4: 40 updates/sec over 160 unique account keys")
    void sinkFour() {
        List<BlockTrade> trades = oneSecondOfTrades();

        long allocations = trades.stream().mapToLong(t -> t.allocations().size()).sum();
        assertThat(allocations)
                .as("10 trades x 4 allocations")
                .isEqualTo(40L);

        // 4 accounts x 10 sub-accounts x 4 symbols, over a run long enough to cover every triple.
        BlockTradeGenerator longer = BlockTradeGenerator.replaying(SEED, START, 100L);
        Set<String> accountKeys = new HashSet<>();
        for (int i = 0; i < 20_000; i++) {
            BlockTrade trade = longer.next();
            for (Allocation allocation : trade.allocations()) {
                accountKeys.add(AccountKey.of(allocation, trade.symbol()));
            }
        }
        assertThat(accountKeys).hasSize(ReferenceData.ACCOUNT_KEY_COUNT);
        assertThat(ReferenceData.ACCOUNT_KEY_COUNT).isEqualTo(160);
    }

    @Test
    @DisplayName("prices round-robin the symbols, so no symbol goes quiet")
    void pricesCycleEverySymbol() {
        PriceGenerator generator = PriceGenerator.replaying(SEED, START, 1L);

        // One full cycle covers every symbol exactly once, in a stable order.
        List<String> cycle = new ArrayList<>();
        for (int i = 0; i < ReferenceData.SYMBOLS.size(); i++) {
            cycle.add(generator.next().symbol());
        }
        assertThat(cycle).containsExactlyElementsOf(ReferenceData.SYMBOLS);

        // And it keeps cycling: over many prices every symbol is evenly served.
        Map<String, Long> counts = new HashMap<>();
        for (int i = 0; i < 4_000; i++) {
            counts.merge(generator.next().symbol(), 1L, Long::sum);
        }
        assertThat(counts).hasSize(ReferenceData.SYMBOL_KEY_COUNT);
        assertThat(counts.values()).allSatisfy(c -> assertThat(c).isEqualTo(1_000L));
    }

    @Test
    @DisplayName("the demo price rate is 1000/sec")
    void demoPriceRate() {
        assertThat(GeneratorConfig.DEMO_PRICES_PER_SECOND).isEqualTo(1_000);
        assertThat(GeneratorConfig.DEMO_TRADES_PER_SECOND).isEqualTo(10);
    }
}
