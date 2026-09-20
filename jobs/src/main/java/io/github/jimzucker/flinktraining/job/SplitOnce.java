package io.github.jimzucker.flinktraining.job;

import io.github.jimzucker.flinktraining.model.AccountKey;
import io.github.jimzucker.flinktraining.model.Allocation;
import io.github.jimzucker.flinktraining.model.BlockTrade;
import io.github.jimzucker.flinktraining.model.Json;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Both position updates from one parse: the symbol update on the main output,
 * the account updates on a side output.
 *
 * <p>The demo's pipeline reads the orders topic twice — {@link ToSymbolUpdate}
 * and {@link SplitByAllocation} each deserialise the whole order — so every
 * order is parsed once per aggregation. At 326 bytes that costs little. At
 * 2 KB the stage timings put the two parses at 68% of the added per-order cost,
 * about 6.9 µs of it in the second one
 * (<a href="../../../../../../../../../docs/skill-validation/payload-2k.md">payload-2k.md</a>).
 *
 * <p>This parses once and fans out through a side output rather than a second
 * source read. A side output is used rather than chaining two operators off one
 * parsed stream because, with object reuse off, Flink copies a record per
 * downstream consumer — which could cost more than the parse it saves.
 *
 * <p>The updates it emits are identical to the two functions it replaces, field
 * for field, which is what makes the comparison one variable.
 */
public class SplitOnce extends ProcessFunction<String, PositionUpdate> {

    private static final long serialVersionUID = 1L;

    /** Account-keyed updates leave here; the symbol-keyed one goes to the main output. */
    public static final OutputTag<PositionUpdate> BY_ACCOUNT =
            new OutputTag<>("by-account", org.apache.flink.api.common.typeinfo.TypeInformation.of(PositionUpdate.class));

    @Override
    public void processElement(String json, Context ctx, Collector<PositionUpdate> out) {
        BlockTrade trade = Json.fromJson(json, BlockTrade.class);

        out.collect(new PositionUpdate(
                trade.symbol(),
                trade.symbol(),
                trade.signedQuantity(),
                trade.tradeId(),
                trade.eventTime()));

        for (Allocation allocation : trade.allocations()) {
            ctx.output(BY_ACCOUNT, new PositionUpdate(
                    AccountKey.of(allocation, trade.symbol()),
                    trade.symbol(),
                    trade.signedQuantity(allocation),
                    trade.tradeId(),
                    trade.eventTime(),
                    SplitByAllocation.carried(allocation.filler())));
        }
    }
}
