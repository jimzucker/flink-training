package st52;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
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
 * Positions by symbol and by account / sub-account / symbol, published per
 * order, plus a market value for each, throttled to an interval.
 *
 * orders -> parse once -> (main) keyBy symbol            -> position-by-symbol  -> positions-by-symbol
 *                       -> (side) 4 allocations, keyBy acct/sub/symbol -> position-by-account -> positions-by-account
 * prices -> broadcast to both aggregations; each emits position x latest price on a processing-time tick.
 */
public class PositionsJob {

    /** One change to one key's position. key is the aggregation key. */
    public static class Delta {
        public long id;
        public String key;
        public String sym;
        public String acct;
        public String sub;
        public long qty;

        public Delta() {}

        public Delta(long id, String key, String sym, String acct, String sub, long qty) {
            this.id = id; this.key = key; this.sym = sym; this.acct = acct; this.sub = sub; this.qty = qty;
        }
    }

    /** A Kafka record, already serialised. */
    public static class OutRec {
        public byte[] key;
        public byte[] value;

        public OutRec() {}

        public OutRec(byte[] key, byte[] value) { this.key = key; this.value = value; }
    }

    public static class Price {
        public String sym;
        public long ts;
        public long price;

        public Price() {}
    }

    static final OutputTag<Delta> ALLOCATIONS = new OutputTag<>("allocations", TypeInformation.of(Delta.class));
    static final OutputTag<OutRec> MARKET_VALUES = new OutputTag<>("market-values", TypeInformation.of(OutRec.class));
    static final MapStateDescriptor<String, long[]> PRICES =
            new MapStateDescriptor<>("latest-price", Types.STRING, PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO);

    public static void main(String[] args) throws Exception {
        // --key=value, the form pipeline.json passes; ParameterTool wants --key value
        java.util.Map<String, String> kv = new java.util.HashMap<>();
        for (String arg : args) {
            int eq = arg.indexOf('=');
            if (arg.startsWith("--") && eq > 2) kv.put(arg.substring(2, eq), arg.substring(eq + 1));
        }
        ParameterTool p = ParameterTool.fromMap(kv);
        String bootstrap = p.getRequired("bootstrap");
        String in = p.getRequired("in");
        String prices = p.get("prices", "prices");
        String outSym = p.getRequired("outSym");
        String outAcct = p.getRequired("outAcct");
        String mvSym = p.getRequired("mvSym");
        String mvAcct = p.getRequired("mvAcct");
        String group = p.getRequired("group");
        int par = p.getInt("parallelism");
        long ckptMs = p.getLong("checkpointMs", 10000);
        long mvIntervalMs = p.getLong("mvIntervalMs", 10000);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(par);
        env.enableCheckpointing(ckptMs, CheckpointingMode.EXACTLY_ONCE);
        env.getConfig().enableObjectReuse();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, Time.seconds(2)));

        KafkaSource<byte[]> orderSource = KafkaSource.<byte[]>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(in)
                .setGroupId(group)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(ByteArrayDeserializer.class))
                .setProperty("commit.offsets.on.checkpoint", "true")
                .setProperty("partition.discovery.interval.ms", "-1")
                .build();
        KafkaSource<byte[]> priceSource = KafkaSource.<byte[]>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(prices)
                .setGroupId(group + "-prices")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(ByteArrayDeserializer.class))
                .setProperty("partition.discovery.interval.ms", "-1")
                .build();

        SingleOutputStreamOperator<Delta> bySymbolIn = env
                .fromSource(orderSource, WatermarkStrategy.noWatermarks(), "orders-source").uid("orders-source")
                .process(new ParseOrders()).name("parse-orders").uid("parse-orders");
        DataStream<Delta> byAccountIn = bySymbolIn.getSideOutput(ALLOCATIONS);

        BroadcastStream<Price> priceStream = env
                .fromSource(priceSource, WatermarkStrategy.noWatermarks(), "prices-source").uid("prices-source")
                .map(new ParsePrice()).name("parse-prices").uid("parse-prices")
                .broadcast(PRICES);

        SingleOutputStreamOperator<OutRec> bySymbol = bySymbolIn
                .keyBy(new ByKey(), Types.STRING)
                .connect(priceStream)
                .process(new PositionFn(false, mvIntervalMs), TypeInformation.of(OutRec.class))
                .name("position-by-symbol").uid("position-by-symbol");
        bySymbol.sinkTo(sink(bootstrap, outSym)).name("sink-positions-by-symbol").uid("sink-positions-by-symbol");
        bySymbol.getSideOutput(MARKET_VALUES).sinkTo(sink(bootstrap, mvSym))
                .name("sink-market-values-by-symbol").uid("sink-market-values-by-symbol");

        SingleOutputStreamOperator<OutRec> byAccount = byAccountIn
                .keyBy(new ByKey(), Types.STRING)
                .connect(priceStream)
                .process(new PositionFn(true, mvIntervalMs), TypeInformation.of(OutRec.class))
                .name("position-by-account").uid("position-by-account");
        byAccount.sinkTo(sink(bootstrap, outAcct)).name("sink-positions-by-account").uid("sink-positions-by-account");
        byAccount.getSideOutput(MARKET_VALUES).sinkTo(sink(bootstrap, mvAcct))
                .name("sink-market-values-by-account").uid("sink-market-values-by-account");

        env.execute("st52 positions and market values");
    }

    static KafkaSink<OutRec> sink(String bootstrap, String topic) {
        Properties props = new Properties();
        props.setProperty("compression.type", "lz4");
        props.setProperty("linger.ms", "10");
        props.setProperty("batch.size", "262144");
        props.setProperty("acks", "1");
        props.setProperty("enable.idempotence", "false");
        return KafkaSink.<OutRec>builder()
                .setBootstrapServers(bootstrap)
                .setKafkaProducerConfig(props)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer(new Serializer(topic))
                .build();
    }

    static final class Serializer implements KafkaRecordSerializationSchema<OutRec> {
        private final String topic;

        Serializer(String topic) { this.topic = topic; }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(OutRec r, KafkaSinkContext ctx, Long ts) {
            return new ProducerRecord<>(topic, null, r.key, r.value);
        }
    }

    static final class ByKey implements KeySelector<Delta, String> {
        @Override
        public String getKey(Delta d) { return d.key; }
    }

    /** Parse an order once; the symbol change goes on, the allocations go out a side output. */
    static final class ParseOrders extends ProcessFunction<byte[], Delta> {
        private transient JsonFactory json;

        @Override
        public void open(Configuration parameters) { json = new JsonFactory(); }

        @Override
        public void processElement(byte[] value, Context ctx, Collector<Delta> out) throws Exception {
            long id = 0, qty = 0;
            String sym = null;
            String[] accts = new String[4], subs = new String[4];
            long[] aq = new long[4];
            int n = 0;
            try (JsonParser jp = json.createParser(value)) {
                jp.nextToken();
                while (jp.nextToken() == JsonToken.FIELD_NAME) {
                    String f = jp.getCurrentName();
                    jp.nextToken();
                    switch (f) {
                        case "id": id = jp.getLongValue(); break;
                        case "sym": sym = jp.getText(); break;
                        case "qty": qty = jp.getLongValue(); break;
                        case "allocs":
                            while (jp.nextToken() == JsonToken.START_OBJECT) {
                                String a = null, s = null;
                                long q = 0;
                                while (jp.nextToken() == JsonToken.FIELD_NAME) {
                                    String g = jp.getCurrentName();
                                    jp.nextToken();
                                    if (g.equals("acct")) a = jp.getText();
                                    else if (g.equals("sub")) s = jp.getText();
                                    else if (g.equals("qty")) q = jp.getLongValue();
                                    else jp.skipChildren();
                                }
                                if (n == accts.length) {
                                    accts = java.util.Arrays.copyOf(accts, n * 2);
                                    subs = java.util.Arrays.copyOf(subs, n * 2);
                                    aq = java.util.Arrays.copyOf(aq, n * 2);
                                }
                                accts[n] = a; subs[n] = s; aq[n] = q; n++;
                            }
                            break;
                        default: jp.skipChildren();
                    }
                }
            }
            out.collect(new Delta(id, sym, sym, null, null, qty));
            for (int i = 0; i < n; i++) {
                ctx.output(ALLOCATIONS, new Delta(id, accts[i] + "|" + subs[i] + "|" + sym, sym, accts[i], subs[i], aq[i]));
            }
        }
    }

    static final class ParsePrice implements MapFunction<byte[], Price> {
        private transient JsonFactory json;

        @Override
        public Price map(byte[] value) throws Exception {
            if (json == null) json = new JsonFactory();
            Price p = new Price();
            try (JsonParser jp = json.createParser(value)) {
                jp.nextToken();
                while (jp.nextToken() == JsonToken.FIELD_NAME) {
                    String f = jp.getCurrentName();
                    jp.nextToken();
                    switch (f) {
                        case "sym": p.sym = jp.getText(); break;
                        case "ts": p.ts = jp.getLongValue(); break;
                        case "price": p.price = jp.getLongValue(); break;
                        default: jp.skipChildren();
                    }
                }
            }
            return p;
        }
    }

    /**
     * The running position for one key. State per key, as one long[]:
     * 0 position, 1 sequence (orders applied), 2 last order id applied,
     * 3 sequence last published as a market value, 4 price timestamp last published,
     * 5 attempt (+1) that last counted this key, 6 tick timer registered.
     */
    static final class PositionFn extends KeyedBroadcastProcessFunction<String, Delta, Price, OutRec> {
        private final boolean account;
        private final long intervalMs;
        private transient ValueState<long[]> state;
        private transient Counter duplicatesDropped;
        private transient long distinctKeys;
        private transient long absQuantity;
        private transient long attempt;

        PositionFn(boolean account, long intervalMs) { this.account = account; this.intervalMs = intervalMs; }

        @Override
        public void open(Configuration parameters) {
            state = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("position", PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO));
            duplicatesDropped = getRuntimeContext().getMetricGroup().counter("duplicatesDropped");
            getRuntimeContext().getMetricGroup().gauge("distinctKeys", () -> distinctKeys);
            getRuntimeContext().getMetricGroup().gauge("absQuantityApplied", () -> absQuantity);
            attempt = getRuntimeContext().getTaskInfo().getAttemptNumber() + 1;
        }

        @Override
        public void processElement(Delta d, ReadOnlyContext ctx, Collector<OutRec> out) throws Exception {
            long[] s = state.value();
            if (s == null) {
                s = new long[] {0, 0, Long.MIN_VALUE, -1, -1, 0, 0};
            }
            if (s[5] != attempt) {
                s[5] = attempt;
                distinctKeys++;
            }
            if (d.id <= s[2]) {
                // A repeat of an order this key has already applied: ids rise within a
                // partition and every key lives in one partition.
                duplicatesDropped.inc();
                state.update(s);
                return;
            }
            s[0] += d.qty;
            s[1] += 1;
            s[2] = d.id;
            absQuantity += Math.abs(d.qty);
            if (s[6] == 0) {
                long now = ctx.timerService().currentProcessingTime();
                ctx.timerService().registerProcessingTimeTimer((now / intervalMs + 1) * intervalMs);
                s[6] = 1;
            }
            state.update(s);
            out.collect(new OutRec(d.key.getBytes(StandardCharsets.UTF_8), position(d, s)));
        }

        private byte[] position(Delta d, long[] s) {
            StringBuilder b = new StringBuilder(96).append('{');
            if (account) {
                b.append("\"acct\":\"").append(d.acct).append("\",\"sub\":\"").append(d.sub).append("\",");
            }
            b.append("\"sym\":\"").append(d.sym).append("\",\"pos\":").append(s[0])
                    .append(",\"seq\":").append(s[1]).append(",\"oid\":").append(s[2]).append('}');
            return b.toString().getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public void onTimer(long ts, OnTimerContext ctx, Collector<OutRec> out) throws Exception {
            String key = ctx.getCurrentKey();
            long[] s = state.value();
            ctx.timerService().registerProcessingTimeTimer(ts + intervalMs);
            if (s == null) {
                return;
            }
            String sym;
            String acct = null, sub = null;
            if (account) {
                int a = key.indexOf('|'), b = key.indexOf('|', a + 1);
                acct = key.substring(0, a);
                sub = key.substring(a + 1, b);
                sym = key.substring(b + 1);
            } else {
                sym = key;
            }
            long[] price = ctx.getBroadcastState(PRICES).get(sym);
            if (price == null) {
                return;                       // no price yet: try again on the next tick
            }
            if (s[3] == s[1] && s[4] == price[0]) {
                return;                       // nothing changed since the last market value
            }
            s[3] = s[1];
            s[4] = price[0];
            state.update(s);
            StringBuilder b = new StringBuilder(128).append('{');
            if (account) {
                b.append("\"acct\":\"").append(acct).append("\",\"sub\":\"").append(sub).append("\",");
            }
            b.append("\"sym\":\"").append(sym).append("\",\"pos\":").append(s[0])
                    .append(",\"price\":").append(price[1]).append(",\"mv\":").append(s[0] * price[1])
                    .append(",\"seq\":").append(s[1]).append(",\"pts\":").append(price[0]).append('}');
            ctx.output(MARKET_VALUES, new OutRec(key.getBytes(StandardCharsets.UTF_8),
                    b.toString().getBytes(StandardCharsets.UTF_8)));
        }

        @Override
        public void processBroadcastElement(Price p, Context ctx, Collector<OutRec> out) throws Exception {
            long[] have = ctx.getBroadcastState(PRICES).get(p.sym);
            if (have == null || p.ts > have[0]) {
                ctx.getBroadcastState(PRICES).put(p.sym, new long[] {p.ts, p.price});
            }
        }
    }
}
