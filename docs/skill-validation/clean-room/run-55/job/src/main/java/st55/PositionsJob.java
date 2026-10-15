package st55;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * Orders in; positions by symbol and by account/sub-account/symbol out, one per
 * input; prices broadcast to both; a market value per key at most every
 * interval for both.
 */
public class PositionsJob {

    static final OutputTag<Leg> ALLOCATIONS = new OutputTag<Leg>("allocations") {};
    static final OutputTag<Out> MV = new OutputTag<Out>("market-values") {};

    public static void main(String[] argv) throws Exception {
        Args a = new Args(argv);
        String bootstrap = a.get("bootstrap");
        int par = (int) a.getLong("par");
        long ckptMs = a.getLong("ckptMs");
        long mvIntervalMs = a.getLong("mvIntervalMs", 10_000);
        String group = a.get("group");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(par);
        env.enableCheckpointing(ckptMs, CheckpointingMode.EXACTLY_ONCE);
        env.getConfig().enableObjectReuse();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(50, Time.seconds(2)));

        KafkaSource<byte[]> orders = KafkaSource.<byte[]>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(a.get("in"))
                .setGroupId(group)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(ByteArrayDeserializer.class))
                .setProperty("commit.offsets.on.checkpoint", "true")
                .build();

        KafkaSource<byte[]> pricesSrc = KafkaSource.<byte[]>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(a.get("prices"))
                .setGroupId(group + "-prices")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(ByteArrayDeserializer.class))
                .setProperty("commit.offsets.on.checkpoint", "false")
                .build();

        DataStream<byte[]> raw = env.fromSource(orders, WatermarkStrategy.noWatermarks(), "kafka-source-orders")
                .uid("kafka-source-orders");

        SingleOutputStreamOperator<Leg> parsed = raw.process(new ParseOrders())
                .name("parse-orders").uid("parse-orders");

        BroadcastStream<Price> prices = env
                .fromSource(pricesSrc, WatermarkStrategy.noWatermarks(), "kafka-source-prices")
                .uid("kafka-source-prices")
                .map(PositionsJob::parsePrice).returns(TypeInformation.of(Price.class))
                .name("parse-prices").uid("parse-prices")
                .broadcast(PositionFn.PRICES);

        SingleOutputStreamOperator<Out> bySym = parsed
                .keyBy(l -> l.key)
                .connect(prices)
                .process(new PositionFn(true, mvIntervalMs, MV))
                .name("position-mv-by-symbol").uid("position-mv-by-symbol");

        SingleOutputStreamOperator<Out> byAcct = parsed.getSideOutput(ALLOCATIONS)
                .keyBy(l -> l.key)
                .connect(prices)
                .process(new PositionFn(false, mvIntervalMs, MV))
                .name("position-mv-by-account").uid("position-mv-by-account");

        bySym.sinkTo(sink(bootstrap, a.get("outSym"))).name("sink-positions-by-symbol").uid("sink-positions-by-symbol");
        byAcct.sinkTo(sink(bootstrap, a.get("outAcct"))).name("sink-positions-by-account").uid("sink-positions-by-account");
        bySym.getSideOutput(MV).sinkTo(sink(bootstrap, a.get("mvSym"))).name("sink-market-values-by-symbol").uid("sink-market-values-by-symbol");
        byAcct.getSideOutput(MV).sinkTo(sink(bootstrap, a.get("mvAcct"))).name("sink-market-values-by-account").uid("sink-market-values-by-account");

        env.execute("st55-positions");
    }

    /** Parse each order once; the order goes on (keyed by symbol), and each of
     *  its allocations goes out of a side output (keyed by account/sub/symbol). */
    static class ParseOrders extends ProcessFunction<byte[], Leg> {
        private transient ObjectMapper mapper;

        @Override
        public void open(Configuration parameters) {
            mapper = new ObjectMapper();
        }

        @Override
        public void processElement(byte[] value, Context ctx, Collector<Leg> out) throws Exception {
            JsonNode o = mapper.readTree(value);
            long id = o.get("id").asLong();
            String sym = o.get("sym").asText();
            out.collect(new Leg(id, sym, o.get("qty").asLong()));
            for (JsonNode al : o.get("alloc")) {
                ctx.output(ALLOCATIONS, new Leg(id,
                        Keys.acctKey(al.get("acct").asText(), al.get("sub").asText(), sym),
                        al.get("qty").asLong()));
            }
        }
    }

    private static final ObjectMapper PRICE_MAPPER = new ObjectMapper();

    static Price parsePrice(byte[] v) throws Exception {
        JsonNode n = PRICE_MAPPER.readTree(v);
        return new Price(n.get("sym").asText(), n.get("ts").asLong(), n.get("cents").asLong());
    }

    static KafkaSink<Out> sink(String bootstrap, String topic) {
        Properties p = new Properties();
        p.setProperty("compression.type", "lz4");
        p.setProperty("linger.ms", "20");
        p.setProperty("batch.size", "262144");
        return KafkaSink.<Out>builder()
                .setBootstrapServers(bootstrap)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setKafkaProducerConfig(p)
                .setRecordSerializer(KafkaRecordSerializationSchema.<Out>builder()
                        .setTopic(topic)
                        .setKeySerializationSchema((SerializationSchema<Out>) o -> o.key.getBytes(StandardCharsets.UTF_8))
                        .setValueSerializationSchema((SerializationSchema<Out>) o -> o.value.getBytes(StandardCharsets.UTF_8))
                        .build())
                .build();
    }
}
