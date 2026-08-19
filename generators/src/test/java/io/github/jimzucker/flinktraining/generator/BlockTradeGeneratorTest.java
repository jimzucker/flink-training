package io.github.jimzucker.flinktraining.generator;

import io.github.jimzucker.flinktraining.model.Allocation;
import io.github.jimzucker.flinktraining.model.BlockTrade;
import io.github.jimzucker.flinktraining.model.ReferenceData;
import io.github.jimzucker.flinktraining.model.Side;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class BlockTradeGeneratorTest {

    private static List<BlockTrade> take(int n, long seed) {
        BlockTradeGenerator generator = new BlockTradeGenerator(seed, 1_000L, 100L);
        List<BlockTrade> trades = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            trades.add(generator.next());
        }
        return trades;
    }

    @Test
    @DisplayName("allocations always sum to the block quantity")
    void allocationsSumToBlock() {
        for (BlockTrade trade : take(500, 7L)) {
            long allocated = trade.allocations().stream().mapToLong(Allocation::quantity).sum();
            assertThat(allocated)
                    .as("allocations of %s", trade.tradeId())
                    .isEqualTo(trade.quantity());
        }
    }

    @Test
    @DisplayName("every block is split across all four accounts, each exactly once")
    void splitAcrossEveryAccount() {
        for (BlockTrade trade : take(200, 7L)) {
            assertThat(trade.allocations()).hasSize(ReferenceData.ALLOCATIONS_PER_TRADE);
            assertThat(trade.allocations().stream().map(Allocation::account).toList())
                    .containsExactlyElementsOf(ReferenceData.ACCOUNTS);
        }
    }

    @Test
    @DisplayName("the block quantity differs from any single allocation")
    void blockIsNotAnAllocation() {
        // If the split were wrong and each allocation carried the whole block,
        // positions would be four times too large and nothing else would catch it.
        BlockTrade trade = take(1, 7L).get(0);
        assertThat(trade.quantity()).isEqualTo(400L);
        assertThat(trade.allocations()).allSatisfy(a -> assertThat(a.quantity()).isEqualTo(100L));
    }

    @Test
    @DisplayName("both buys and sells are produced")
    void producesBothSides() {
        Map<Side, Long> bySide = new HashMap<>();
        for (BlockTrade trade : take(1_000, 7L)) {
            bySide.merge(trade.side(), 1L, Long::sum);
        }
        assertThat(bySide).containsOnlyKeys(Side.BUY, Side.SELL);
        assertThat(bySide.get(Side.BUY)).isGreaterThan(100L);
        assertThat(bySide.get(Side.SELL)).isGreaterThan(100L);
        assertThat(bySide.get(Side.BUY) + bySide.get(Side.SELL)).isEqualTo(1_000L);
    }

    @Test
    @DisplayName("buys add and sells subtract")
    void signFollowsSide() {
        assertThat(Side.BUY.apply(100L)).isEqualTo(100L);
        assertThat(Side.SELL.apply(100L)).isEqualTo(-100L);

        for (BlockTrade trade : take(50, 7L)) {
            long expected = trade.side() == Side.BUY ? trade.quantity() : -trade.quantity();
            assertThat(trade.signedQuantity()).isEqualTo(expected);
        }
    }

    @Test
    @DisplayName("event time advances by exactly the interval, never backwards")
    void eventTimeIsMonotonic() {
        List<BlockTrade> trades = take(100, 7L);
        for (int i = 1; i < trades.size(); i++) {
            assertThat(trades.get(i).eventTime() - trades.get(i - 1).eventTime())
                    .as("gap before %s", trades.get(i).tradeId())
                    .isEqualTo(100L);
        }
    }

    @Test
    @DisplayName("trade ids are unique and sort in emission order")
    void tradeIdsSortInOrder() {
        List<String> ids = take(1_000, 7L).stream().map(BlockTrade::tradeId).toList();
        assertThat(ids).doesNotHaveDuplicates();
        assertThat(ids).isSorted();
    }
}
