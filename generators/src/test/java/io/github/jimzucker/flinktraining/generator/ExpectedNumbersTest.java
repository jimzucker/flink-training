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
    private static final long START = GeneratorConfig.DEFAULT_START_EPOCH_MILLIS;
    private static final int TRADES_PER_SECOND = GeneratorConfig.DEMO_TRADES_PER_SECOND;

    private static List<BlockTrade> oneSecondOfTrades() {
        BlockTradeGenerator generator =
                new BlockTradeGenerator(SEED, START, 1_000L / TRADES_PER_SECOND);
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
        BlockTradeGenerator longer = new BlockTradeGenerator(SEED, START, 100L);
        Set<String> seen = new HashSet<>();
        for (int i = 0; i < 200; i++) {
            seen.add(longer.next().symbol());
        }
        assertThat(seen).containsExactlyInAnyOrderElementsOf(ReferenceData.SYMBOLS);
    }

    @Test
    @DisplayName("sink 4: 40 updates/sec over 16 unique account keys")
    void sinkFour() {
        List<BlockTrade> trades = oneSecondOfTrades();

        long allocations = trades.stream().mapToLong(t -> t.allocations().size()).sum();
        assertThat(allocations)
                .as("10 trades x 4 allocations")
                .isEqualTo(40L);

        // 4 accounts x 4 symbols, reached over a run long enough to cover every pair.
        BlockTradeGenerator longer = new BlockTradeGenerator(SEED, START, 100L);
        Set<String> accountKeys = new HashSet<>();
        for (int i = 0; i < 500; i++) {
            BlockTrade trade = longer.next();
            for (Allocation allocation : trade.allocations()) {
                accountKeys.add(AccountKey.of(allocation, trade.symbol()));
            }
        }
        assertThat(accountKeys).hasSize(ReferenceData.ACCOUNT_KEY_COUNT);
        assertThat(ReferenceData.ACCOUNT_KEY_COUNT).isEqualTo(16);
    }

    @Test
    @DisplayName("sinks 5 and 6: one price per symbol per tick, so no symbol goes quiet")
    void priceTicksCoverEverySymbol() {
        PriceGenerator generator = new PriceGenerator(SEED, START, 1_000L);

        for (int tick = 0; tick < 10; tick++) {
            List<Price> prices = generator.next();
            assertThat(prices).hasSize(ReferenceData.SYMBOL_KEY_COUNT);
            assertThat(prices.stream().map(Price::symbol).toList())
                    .containsExactlyElementsOf(ReferenceData.SYMBOLS);
        }
    }
}
