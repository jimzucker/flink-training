package io.github.jimzucker.flinktraining.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

/**
 * Part 2 of the pipeline: prices joined to positions, market value out once per
 * minute.
 *
 * <p>Both position topics are processed the same way, differing only in what
 * they are keyed on. Prices are broadcast rather than keyed, because the account
 * side is keyed on account/sub-account/symbol and cannot be joined to a
 * symbol-keyed stream by key alone.
 */
public final class MarketValueJob {

    private static final Logger LOG = LoggerFactory.getLogger(MarketValueJob.class);

    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.fromEnvironment();
        LOG.info("market value job: bootstrap={} {} , {} -> {} , {}",
                config.bootstrapServers(),
                config.positionsBySymbolTopic(), config.positionsByAccountTopic(),
                config.mvBySymbolTopic(), config.mvByAccountTopic());
        LOG.info("window={}ms outOfOrderness={}ms idleness={}ms parallelism={} delivery=exactly-once",
                config.windowMillis(), config.outOfOrdernessMillis(),
                config.idlenessMillis(), config.parallelism());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(config.parallelism());
        env.enableCheckpointing(config.checkpointIntervalMillis());

        DataStream<PriceState> prices = env
                .fromSource(source(config, config.pricesTopic(), "mv-prices"),
                        watermarks(config), "prices")
                .map(new Parsers.ToPrice()).name("parse prices");

        BroadcastStream<PriceState> broadcastPrices =
                prices.broadcast(MarketValueAtClose.PRICES);

        emitMarketValue(env, config, config.positionsBySymbolTopic(),
                config.mvBySymbolTopic(), broadcastPrices, "by symbol", "mv-by-symbol-tx");
        emitMarketValue(env, config, config.positionsByAccountTopic(),
                config.mvByAccountTopic(), broadcastPrices, "by account", "mv-by-account-tx");

        env.execute("Part 2 - market value");
    }

    private static void emitMarketValue(
            StreamExecutionEnvironment env, JobConfig config,
            String positionsTopic, String marketValueTopic,
            BroadcastStream<PriceState> prices, String label, String transactionalIdPrefix) {

        env.fromSource(source(config, positionsTopic, "mv-" + positionsTopic),
                        watermarks(config), positionsTopic)
                .map(new Parsers.ToPosition()).name("parse " + label)
                .keyBy(position -> position.key)
                .connect(prices)
                .process(new MarketValueAtClose(config.windowMillis()))
                .name("market value " + label)
                .sinkTo(marketValueSink(config, marketValueTopic, transactionalIdPrefix))
                .name(marketValueTopic);
    }

    /**
     * Monotonically increasing timestamps, because a key's records occupy one
     * partition and Kafka preserves order within it — nothing can arrive late.
     *
     * <p>Idleness is the setting that matters here. A join advances its watermark
     * at the slower of its inputs, so a partition with no traffic would stall the
     * watermark and stop the windows firing, even though every record that did
     * arrive was in order. Sinks 5 and 6 would simply go quiet, which during a
     * demo is indistinguishable from a broken pipeline.
     */
    private static WatermarkStrategy<String> watermarks(JobConfig config) {
        WatermarkStrategy<String> strategy = WatermarkStrategy.forMonotonousTimestamps();
        // Zero disables it, which exists so the setting can be shown to matter:
        // with idleness off and any partition empty, no window ever fires.
        return config.idlenessMillis() > 0
                ? strategy.withIdleness(Duration.ofMillis(config.idlenessMillis()))
                : strategy;
    }

    private static KafkaSource<String> source(JobConfig config, String topic, String group) {
        Properties properties = new Properties();
        // The position topics are written transactionally; reading uncommitted
        // would consume records belonging to transactions that may still abort.
        properties.setProperty("isolation.level", "read_committed");

        return KafkaSource.<String>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setTopics(topic)
                .setGroupId(group)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setProperties(properties)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    private static KafkaSink<MarketValueState> marketValueSink(
            JobConfig config, String topic, String transactionalIdPrefix) {
        Properties properties = new Properties();
        properties.setProperty("transaction.timeout.ms",
                Long.toString(config.transactionTimeoutMillis()));

        return KafkaSink.<MarketValueState>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(transactionalIdPrefix)
                .setKafkaProducerConfig(properties)
                .setRecordSerializer(new MarketValueSerialization(topic))
                .build();
    }

    private MarketValueJob() {
    }
}
