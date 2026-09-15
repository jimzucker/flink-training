package io.github.jimzucker.flinktraining.job;

import io.github.jimzucker.flinktraining.model.AccountKey;
import io.github.jimzucker.flinktraining.model.Allocation;
import io.github.jimzucker.flinktraining.model.BlockTrade;
import io.github.jimzucker.flinktraining.model.Json;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * Splits a block trade into one update per allocation, keyed by
 * account/sub-account/symbol.
 *
 * <p>The trade's side is applied to each allocation, so a sold block decrements
 * every account it touched. Any filler fields on an allocation ride along on its
 * update to the aggregation; they do not change a position.
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
                    trade.eventTime(),
                    carried(allocation.filler())));
        }
    }

    /** Name, value, name, value... in the order the order carried them; null when there are none. */
    static String[] carried(Map<String, String> filler) {
        if (filler == null || filler.isEmpty()) {
            return null;
        }
        String[] out = new String[filler.size() * 2];
        int i = 0;
        for (Map.Entry<String, String> e : filler.entrySet()) {
            out[i++] = e.getKey();
            out[i++] = e.getValue();
        }
        return out;
    }
}
