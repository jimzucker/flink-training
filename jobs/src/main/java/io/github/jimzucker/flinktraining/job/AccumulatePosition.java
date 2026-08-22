package io.github.jimzucker.flinktraining.job;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Maintains a signed running position per key and emits it on every change.
 *
 * <p>Emitting on every update rather than on a timer is what makes sinks 3 and 4
 * match the input rate exactly, and it is what lets the demo point at a number
 * and say which trade produced it.
 */
public class AccumulatePosition extends KeyedProcessFunction<String, PositionUpdate, PositionState> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(AccumulatePosition.class);

    /** Log every Nth update per key, so a busy run stays readable but stays traceable. */
    private final long logEvery;

    private transient ValueState<Long> quantity;
    private transient ValueState<Long> updates;

    /**
     * Distinct keys this subtask has handled, published as a gauge.
     *
     * <p>The expected-output table is stated in unique keys -- four by symbol,
     * sixteen by account -- and Flink has no metric for that. Keys are
     * partitioned across subtasks, so summing the gauge gives the total without
     * double counting. It counts from when the subtask started, so a restart
     * resets it until every key has been seen again.
     */
    private transient Set<String> keysSeen;

    public AccumulatePosition(long logEvery) {
        this.logEvery = logEvery;
    }

    @Override
    public void open(Configuration parameters) {
        quantity = getRuntimeContext().getState(new ValueStateDescriptor<>("quantity", Long.class));
        updates = getRuntimeContext().getState(new ValueStateDescriptor<>("updates", Long.class));

        keysSeen = new HashSet<>();
        getRuntimeContext().getMetricGroup().gauge("activeKeys", () -> keysSeen.size());
    }

    @Override
    public void processElement(PositionUpdate update, Context ctx, Collector<PositionState> out)
            throws Exception {
        // Every key starts flat. A key whose sells exceed its buys goes negative,
        // which is a short position and not an error.
        keysSeen.add(update.key);

        long previous = quantity.value() == null ? 0L : quantity.value();
        long count = (updates.value() == null ? 0L : updates.value()) + 1;
        long current = previous + update.signedQuantity;

        quantity.update(current);
        updates.update(count);

        if (logEvery > 0 && count % logEvery == 0) {
            LOG.info("position {} {} {} -> {} after {} updates (trade {})",
                    update.key, previous,
                    update.signedQuantity >= 0 ? "+" + update.signedQuantity : update.signedQuantity,
                    current, count, update.tradeId);
        }

        out.collect(new PositionState(
                update.key, update.symbol, current, update.tradeId, count, update.eventTime));
    }
}
