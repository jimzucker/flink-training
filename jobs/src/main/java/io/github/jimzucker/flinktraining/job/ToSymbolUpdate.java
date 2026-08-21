package io.github.jimzucker.flinktraining.job;

import io.github.jimzucker.flinktraining.model.BlockTrade;
import io.github.jimzucker.flinktraining.model.Json;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * One update per block trade, keyed by symbol.
 *
 * <p>Deliberately *not* fed from the allocation split. A symbol's position moves
 * once per trade, so sink 3 emits at the trade rate; the account side goes
 * through the split and emits at four times that. Driving both from the split
 * would make sink 3 emit four updates per trade, which is the wrong number even
 * though the resulting quantity would be identical — allocations sum to the
 * block.
 */
public class ToSymbolUpdate implements FlatMapFunction<String, PositionUpdate> {

    private static final long serialVersionUID = 1L;

    @Override
    public void flatMap(String json, Collector<PositionUpdate> out) {
        BlockTrade trade = Json.fromJson(json, BlockTrade.class);
        out.collect(new PositionUpdate(
                trade.symbol(),
                trade.symbol(),
                trade.signedQuantity(),
                trade.tradeId(),
                trade.eventTime()));
    }
}
