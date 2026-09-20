package io.github.jimzucker.flinktraining.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link PositionsJob} with one variable changed: the order is parsed once.
 *
 * <p>Everything else is the same job — same aggregation, same sinks, same
 * guarantees, same topics. The only difference is that {@link SplitOnce}
 * replaces {@link ToSymbolUpdate} and {@link SplitByAllocation}, so the order's
 * JSON is deserialised once instead of once per aggregation.
 *
 * <p>It exists as a separate main class rather than a flag on the original so
 * the baseline jar is untouched and the two arms differ in exactly one thing.
 * Point {@code pipeline.json}'s {@code job.mainClass} at this to measure it.
 */
public final class PositionsJobParseOnce {

    private static final Logger LOG = LoggerFactory.getLogger(PositionsJobParseOnce.class);

    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.fromArgs(args);
        LOG.info("positions job (parse once): bootstrap={} orders={} -> {} , {}",
                config.bootstrapServers(), config.ordersTopic(),
                config.positionsBySymbolTopic(), config.positionsByAccountTopic());
        LOG.info("parallelism={} checkpoint={}ms delivery=exactly-once; one parse per order",
                config.parallelism(), config.checkpointIntervalMillis());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(config.parallelism());
        env.enableCheckpointing(config.checkpointIntervalMillis());

        DataStream<String> orders = env
                .fromSource(PositionsJob.ordersSource(config), WatermarkStrategy.noWatermarks(), "orders")
                .name("orders");

        build(orders, config);

        env.execute("Part 1 - positions (parse once)");
    }

    /**
     * The same two aggregations, fed from one parse instead of two source reads.
     */
    public static void build(DataStream<String> orders, JobConfig config) {
        SingleOutputStreamOperator<PositionUpdate> bySymbol =
                orders.process(new SplitOnce()).name("split once");

        bySymbol
                .keyBy(update -> update.key)
                .process(new AccumulatePosition(config.logEvery())).name("aggregate by symbol")
                .sinkTo(PositionsJob.positionsSink(config, config.positionsBySymbolTopic(),
                        JobConfig.transactionalIdPrefix("positions-by-symbol-tx")))
                .name("positions-by-symbol");

        bySymbol.getSideOutput(SplitOnce.BY_ACCOUNT)
                .keyBy(update -> update.key)
                .process(new AccumulatePosition(config.logEvery())).name("aggregate by account")
                .sinkTo(PositionsJob.positionsSink(config, config.positionsByAccountTopic(),
                        JobConfig.transactionalIdPrefix("positions-by-account-tx")))
                .name("positions-by-account");
    }

    private PositionsJobParseOnce() {
    }
}
