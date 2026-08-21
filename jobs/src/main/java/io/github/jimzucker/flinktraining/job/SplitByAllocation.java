package io.github.jimzucker.flinktraining.job;

import io.github.jimzucker.flinktraining.model.AccountKey;
import io.github.jimzucker.flinktraining.model.Allocation;
import io.github.jimzucker.flinktraining.model.BlockTrade;
import io.github.jimzucker.flinktraining.model.Json;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Splits a block trade into one update per allocation, keyed by
 * account/sub-account/symbol.
 *
 * <p>The trade's side is applied to each allocation, so a sold block decrements
 * every account it touched.
 */
public class SplitByAllocation implements FlatMapFunction<String, PositionUpdate> {

    private static final long serialVersionUID = 1L;

    @Override
    public void flatMap(String json, Collector<PositionUpdate> out) {
        BlockTrade trade = Json.fromJson(json, BlockTrade.class);
        for (Allocation allocation : trade.allocations()) {
            out.collect(new PositionUpdate(
                    AccountKey.of(allocation, trade.symbol()),
                    trade.symbol(),
                    trade.signedQuantity(allocation),
                    trade.tradeId(),
                    trade.eventTime()));
        }
    }
}
