package st54;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * Positions by symbol and by account / sub-account / symbol, each published as
 * it changes, and a market value for each, throttled to an interval, from
 * prices broadcast to both aggregations.
 *
 * orders -> parse-orders (once) -> main output keyBy symbol            -> position-mv-by-symbol
 *                               -> side output keyBy acct/sub/symbol   -> position-mv-by-account
 * prices -> broadcast to both.
 *
 * Exactly-once checkpointing; at-least-once sinks made idempotent by
 * publishing the absolute position per key. Every output record carries n,
 * the count of inputs applied to its key, which only rises.
 */
public final class PositionsJob {

    /** one leg of an order, for one key: the whole order for the symbol, one allocation for an account key */
    public static class Leg {
        public String key;
        public long id;
        public long qty;   // signed: a sell is negative

        public Leg() {}

        public Leg(String key, long id, long qty) {
            this.key = key;
            this.id = id;
            this.qty = qty;
        }
    }

    /** per-key state. maxId holds the highest order id applied from each input partition. */
    public static class PosState {
        public long position;
        public long n;
        public long[] maxId;
        public long lastMvN;
        public long timerAt;

        public PosState() {}
    }

    static final OutputTag<Leg> ALLOCATIONS = new OutputTag<Leg>("allocations") {};
    static final OutputTag<Tuple2<String, String>> MARKET_VALUES =
            new OutputTag<Tuple2<String, String>>("market-values") {};
    static final MapStateDescriptor<String, Tuple2<Long, Long>> PRICES = new MapStateDescriptor<>(
            "latest-price", Types.STRING, TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}));

    public static void main(String[] argv) throws Exception {
        Args a = new Args(argv);
        String bootstrap = a.get("bootstrap");
        String group = a.get("group");
        int par = Integer.parseInt(a.get("parallelism"));
        long ckpt = a.getLong("checkpointMs");
        long mvInterval = a.getLong("mvIntervalMs", 10_000L);
        int partitions = Integer.parseInt(a.get("partitions"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(par);
        env.enableCheckpointing(ckpt, CheckpointingMode.EXACTLY_ONCE);

        KafkaSource<byte[]> orders = KafkaSource.<byte[]>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(a.get("orders"))
                .setGroupId(group)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(ByteArrayDeserializer.class))
                .setProperty("commit.offsets.on.checkpoint", "true")
                .setProperty("partition.discovery.interval.ms", "-1")
                .build();
        // Prices always from the start of the topic: a fresh job must see every price,
        // and a restored one resumes from the checkpoint, not from a committed offset.
        KafkaSource<byte[]> prices = KafkaSource.<byte[]>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(a.get("prices"))
                .setGroupId(group + "-prices")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(ByteArrayDeserializer.class))
                .setProperty("commit.offsets.on.checkpoint", "false")
                .setProperty("partition.discovery.interval.ms", "-1")
                .build();

        SingleOutputStreamOperator<Leg> bySymbol = env
                .fromSource(orders, WatermarkStrategy.noWatermarks(), "orders-source").uid("orders-source")
                .process(new ParseOrders()).name("parse-orders").uid("parse-orders");
        DataStream<Leg> byAccount = bySymbol.getSideOutput(ALLOCATIONS);

        BroadcastStream<byte[]> priceBroadcast = env
                .fromSource(prices, WatermarkStrategy.noWatermarks(), "prices-source").uid("prices-source")
                .broadcast(PRICES);

        SingleOutputStreamOperator<Tuple2<String, String>> symOut = bySymbol
                .keyBy(l -> l.key)
                .connect(priceBroadcast)
                .process(new PositionAndValue(false, partitions, mvInterval))
                .name("position-mv-by-symbol").uid("position-mv-by-symbol");
        SingleOutputStreamOperator<Tuple2<String, String>> acctOut = byAccount
                .keyBy(l -> l.key)
                .connect(priceBroadcast)
                .process(new PositionAndValue(true, partitions, mvInterval))
                .name("position-mv-by-account").uid("position-mv-by-account");

        symOut.sinkTo(sink(bootstrap, a.get("posSym"))).name("sink-positions-by-symbol").uid("sink-positions-by-symbol");
        acctOut.sinkTo(sink(bootstrap, a.get("posAcct"))).name("sink-positions-by-account").uid("sink-positions-by-account");
        symOut.getSideOutput(MARKET_VALUES).sinkTo(sink(bootstrap, a.get("mvSym")))
                .name("sink-market-values-by-symbol").uid("sink-market-values-by-symbol");
        acctOut.getSideOutput(MARKET_VALUES).sinkTo(sink(bootstrap, a.get("mvAcct")))
                .name("sink-market-values-by-account").uid("sink-market-values-by-account");

        env.execute("st54-positions");
    }

    static KafkaSink<Tuple2<String, String>> sink(String bootstrap, String topic) {
        Properties p = new Properties();
        p.setProperty("compression.type", "lz4");
        p.setProperty("linger.ms", "20");
        p.setProperty("batch.size", "262144");
        return KafkaSink.<Tuple2<String, String>>builder()
                .setBootstrapServers(bootstrap)
                .setKafkaProducerConfig(p)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer(new KeyedJson(topic))
                .build();
    }

    /** key bytes = the aggregation key, so Kafka's own key hash puts each key in one partition */
    static final class KeyedJson implements KafkaRecordSerializationSchema<Tuple2<String, String>> {
        private final String topic;

        KeyedJson(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, String> t, KafkaSinkContext ctx, Long ts) {
            return new ProducerRecord<>(topic, t.f0.getBytes(StandardCharsets.UTF_8),
                    t.f1.getBytes(StandardCharsets.UTF_8));
        }
    }

    /** Parse each order once; the symbol leg on the main output, one leg per allocation on the side output. */
    static final class ParseOrders extends ProcessFunction<byte[], Leg> {
        private transient ObjectMapper om;

        @Override
        public void open(Configuration parameters) {
            om = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        }

        @Override
        public void processElement(byte[] value, Context ctx, Collector<Leg> out) throws Exception {
            JsonNode o = om.readTree(value);
            long id = o.get("id").asLong();
            String symbol = o.get("symbol").asText();
            long sign = "SELL".equals(o.get("side").asText()) ? -1 : 1;
            out.collect(new Leg(symbol, id, sign * o.get("quantity").asLong()));
            for (JsonNode al : o.get("allocations")) {
                ctx.output(ALLOCATIONS, new Leg(
                        Model.accountKey(al.get("account").asText(), al.get("subAccount").asText(), symbol),
                        id, sign * al.get("quantity").asLong()));
            }
        }
    }

    /**
     * The running position for one key level, published on every applied input,
     * and its market value at most once per interval, on interval boundaries,
     * when the position changed since the last one published.
     */
    static final class PositionAndValue
            extends KeyedBroadcastProcessFunction<String, Leg, byte[], Tuple2<String, String>> {
        private final boolean account;
        private final int partitions;
        private final long interval;
        private transient ValueState<PosState> state;
        private transient ObjectMapper om;
        private transient Counter distinctKeys;
        private transient Counter quantityApplied;
        private transient Counter repeatsDropped;

        PositionAndValue(boolean account, int partitions, long interval) {
            this.account = account;
            this.partitions = partitions;
            this.interval = interval;
        }

        @Override
        public void open(Configuration parameters) {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("position", PosState.class));
            om = new ObjectMapper();
            distinctKeys = getRuntimeContext().getMetricGroup().counter("distinctKeys");
            quantityApplied = getRuntimeContext().getMetricGroup().counter("quantityApplied");
            repeatsDropped = getRuntimeContext().getMetricGroup().counter("repeatsDropped");
        }

        @Override
        public void processElement(Leg leg, ReadOnlyContext ctx, Collector<Tuple2<String, String>> out)
                throws Exception {
            PosState st = state.value();
            if (st == null) {
                st = new PosState();
                st.maxId = new long[partitions];
                java.util.Arrays.fill(st.maxId, -1L);
                distinctKeys.inc();
            }
            int p = (int) (leg.id >>> Model.PARTITION_SHIFT);
            if (leg.id <= st.maxId[p]) {
                repeatsDropped.inc();   // a repeat the producer wrote twice: counted once
                return;
            }
            st.maxId[p] = leg.id;
            st.position += leg.qty;
            st.n++;
            quantityApplied.inc(leg.qty);
            out.collect(Tuple2.of(leg.key, positionJson(leg.key, st)));
            if (st.timerAt == 0) {
                long now = ctx.timerService().currentProcessingTime();
                st.timerAt = (now / interval + 1) * interval;
                ctx.timerService().registerProcessingTimeTimer(st.timerAt);
            }
            state.update(st);
        }

        @Override
        public void onTimer(long ts, OnTimerContext ctx, Collector<Tuple2<String, String>> out) throws Exception {
            PosState st = state.value();
            if (st == null) {
                return;
            }
            st.timerAt = 0;
            if (st.n != st.lastMvN) {
                String key = ctx.getCurrentKey();
                Tuple2<Long, Long> price = ctx.getBroadcastState(PRICES).get(Model.symbolOfKey(key));
                if (price == null) {
                    // no price yet for this symbol: try again at the next boundary
                    st.timerAt = (ts / interval + 1) * interval;
                    ctx.timerService().registerProcessingTimeTimer(st.timerAt);
                } else {
                    ctx.output(MARKET_VALUES, Tuple2.of(key, marketValueJson(key, st, price.f1, price.f0)));
                    st.lastMvN = st.n;
                }
            }
            state.update(st);
        }

        @Override
        public void processBroadcastElement(byte[] value, Context ctx, Collector<Tuple2<String, String>> out)
                throws Exception {
            JsonNode p = om.readTree(value);
            String symbol = p.get("symbol").asText();
            long ts = p.get("ts").asLong();
            long cents = p.get("priceCents").asLong();
            BroadcastState<String, Tuple2<Long, Long>> bs = ctx.getBroadcastState(PRICES);
            Tuple2<Long, Long> have = bs.get(symbol);
            if (have == null || ts >= have.f0) {
                bs.put(symbol, Tuple2.of(ts, cents));
            }
        }

        private String keyFields(String key) {
            if (!account) {
                return "\"symbol\":\"" + key + "\"";
            }
            int a = key.indexOf('/');
            int b = key.lastIndexOf('/');
            return "\"account\":\"" + key.substring(0, a) + "\",\"subAccount\":\"" + key.substring(a + 1, b)
                    + "\",\"symbol\":\"" + key.substring(b + 1) + "\"";
        }

        private String positionJson(String key, PosState st) {
            return "{" + keyFields(key) + ",\"position\":" + st.position + ",\"n\":" + st.n + "}";
        }

        private String marketValueJson(String key, PosState st, long cents, long priceTs) {
            return "{" + keyFields(key) + ",\"position\":" + st.position + ",\"priceCents\":" + cents
                    + ",\"priceTs\":" + priceTs + ",\"marketValueCents\":" + Math.multiplyExact(st.position, cents)
                    + ",\"n\":" + st.n + "}";
        }
    }
}
