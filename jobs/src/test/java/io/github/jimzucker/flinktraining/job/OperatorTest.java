package io.github.jimzucker.flinktraining.job;

import io.github.jimzucker.flinktraining.model.AccountKey;
import io.github.jimzucker.flinktraining.model.Allocation;
import io.github.jimzucker.flinktraining.model.BlockTrade;
import io.github.jimzucker.flinktraining.model.Json;
import io.github.jimzucker.flinktraining.model.Side;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class OperatorTest {

    private static BlockTrade trade(String id, String symbol, Side side, long perAllocation) {
        List<Allocation> allocations = List.of(
                new Allocation("ACC1", "SUB1", perAllocation),
                new Allocation("ACC2", "SUB1", perAllocation),
                new Allocation("ACC3", "SUB1", perAllocation),
                new Allocation("ACC4", "SUB1", perAllocation));
        return new BlockTrade(id, symbol, side, perAllocation * 4, allocations, 1_000L);
    }

    private static <T> List<T> collect(java.util.function.Consumer<Collector<T>> run) {
        List<T> out = new ArrayList<>();
        run.accept(new Collector<>() {
            @Override
            public void collect(T record) {
                out.add(record);
            }

            @Override
            public void close() {
            }
        });
        return out;
    }

    @Test
    @DisplayName("the symbol side emits once per trade, not once per allocation")
    void symbolSideEmitsOncePerTrade() throws Exception {
        // This is the difference between sink 3 reading 10/sec and 40/sec.
        BlockTrade t = trade("T1", "AAPL", Side.BUY, 100);
        List<PositionUpdate> updates =
                collect(c -> {
                    try {
                        new ToSymbolUpdate().flatMap(Json.toJson(t), c);
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                });

        assertThat(updates).hasSize(1);
        assertThat(updates.get(0).key).isEqualTo("AAPL");
        assertThat(updates.get(0).signedQuantity).isEqualTo(400L);
    }

    @Test
    @DisplayName("the account side emits once per allocation")
    void accountSideEmitsOncePerAllocation() {
        BlockTrade t = trade("T1", "AAPL", Side.BUY, 100);
        List<PositionUpdate> updates =
                collect(c -> {
                    try {
                        new SplitByAllocation().flatMap(Json.toJson(t), c);
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                });

        assertThat(updates).hasSize(4);
        assertThat(updates).extracting(u -> u.key).containsExactly(
                AccountKey.of("ACC1", "SUB1", "AAPL"),
                AccountKey.of("ACC2", "SUB1", "AAPL"),
                AccountKey.of("ACC3", "SUB1", "AAPL"),
                AccountKey.of("ACC4", "SUB1", "AAPL"));
        assertThat(updates).allSatisfy(u -> assertThat(u.signedQuantity).isEqualTo(100L));
    }

    @Test
    @DisplayName("a sell decrements both sides")
    void sellDecrementsBothSides() {
        BlockTrade t = trade("T1", "AAPL", Side.SELL, 100);

        List<PositionUpdate> symbol = collect(c -> {
            try {
                new ToSymbolUpdate().flatMap(Json.toJson(t), c);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
        List<PositionUpdate> account = collect(c -> {
            try {
                new SplitByAllocation().flatMap(Json.toJson(t), c);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });

        assertThat(symbol.get(0).signedQuantity).isEqualTo(-400L);
        assertThat(account).allSatisfy(u -> assertThat(u.signedQuantity).isEqualTo(-100L));
    }

    @Test
    @DisplayName("the two sides reconcile: allocations sum to the block")
    void sidesReconcile() {
        // Whatever else changes, these two aggregations must agree in total, or
        // the dashboard shows two different answers to the same question.
        List<BlockTrade> trades = List.of(
                trade("T1", "AAPL", Side.BUY, 100),
                trade("T2", "AAPL", Side.SELL, 250),
                trade("T3", "MSFT", Side.BUY, 75));

        long symbolTotal = 0;
        long accountTotal = 0;
        for (BlockTrade t : trades) {
            symbolTotal += collect((Collector<PositionUpdate> c) -> {
                try {
                    new ToSymbolUpdate().flatMap(Json.toJson(t), c);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }).stream().mapToLong(u -> u.signedQuantity).sum();

            accountTotal += collect((Collector<PositionUpdate> c) -> {
                try {
                    new SplitByAllocation().flatMap(Json.toJson(t), c);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }).stream().mapToLong(u -> u.signedQuantity).sum();
        }

        assertThat(symbolTotal).isEqualTo(accountTotal);
    }
}
