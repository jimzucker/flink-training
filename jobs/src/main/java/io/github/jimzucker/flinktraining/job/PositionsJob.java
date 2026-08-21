package io.github.jimzucker.flinktraining.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Part 1 of the pipeline: block trades in, positions out, aggregated two ways in
 * parallel.
 *
 * <p>Both aggregations read the same source, which is the point of the exercise.
 * They differ in what they are keyed on and, importantly, in how often they
 * update:
 *
 * <ul>
 *   <li><b>By symbol</b> — one update per trade, so sink 3 emits at the trade
 *       rate over one key per symbol.</li>
 *   <li><b>By account</b> — one update per allocation, so sink 4 emits at four
 *       times the trade rate over one key per account and symbol.</li>
 * </ul>
 *
 * <p>Feeding the symbol side from the allocation split as well would give the
 * same quantities — allocations sum to the block — but four times the updates,
 * which is the wrong number.
 */
public final class PositionsJob {

    private static final Logger LOG = LoggerFactory.getLogger(PositionsJob.class);

    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.fromEnvironment();
        LOG.info("positions job: bootstrap={} orders={} -> {} , {} (parallelism {})",
                config.bootstrapServers(), config.ordersTopic(),
                config.positionsBySymbolTopic(), config.positionsByAccountTopic(),
                config.parallelism());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(config.parallelism());
        env.enableCheckpointing(config.checkpointIntervalMillis());

        DataStream<String> orders = env
                .fromSource(ordersSource(config), WatermarkStrategy.noWatermarks(), "orders")
                .name("orders");

        build(orders, config);

        env.execute("Part 1 - positions");
    }

    /**
     * The pipeline itself, separated from the environment so tests can drive it
     * with their own source and sinks.
     */
    public static void build(DataStream<String> orders, JobConfig config) {
        orders.flatMap(new ToSymbolUpdate()).name("by symbol")
                .keyBy(update -> update.key)
                .process(new AccumulatePosition(config.logEvery())).name("aggregate by symbol")
                .sinkTo(positionsSink(config, config.positionsBySymbolTopic()))
                .name("positions-by-symbol");

        orders.flatMap(new SplitByAllocation()).name("split by allocation")
                .keyBy(update -> update.key)
                .process(new AccumulatePosition(config.logEvery())).name("aggregate by account")
                .sinkTo(positionsSink(config, config.positionsByAccountTopic()))
                .name("positions-by-account");
    }

    private static KafkaSource<String> ordersSource(JobConfig config) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setTopics(config.ordersTopic())
                .setGroupId(config.consumerGroup())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    private static KafkaSink<PositionState> positionsSink(JobConfig config, String topic) {
        return KafkaSink.<PositionState>builder()
                .setBootstrapServers(config.bootstrapServers())
                // At-least-once: a position is a running sum, so a replayed record
                // would double-count. That only arises on a failure and restore,
                // which the demo does not exercise; exactly-once needs Kafka
                // transactions and a read-committed consumer, and is taken up with
                // the AWS deployment.
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer(new PositionSerialization(topic))
                .build();
    }

    private PositionsJob() {
    }
}
