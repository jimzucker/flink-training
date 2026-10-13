package st53;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * Positions by symbol and by account / sub-account / symbol, each published as it changes, and a
 * market value for each key every 10 s from a broadcast price feed. See README.md.
 */
public final class PositionsJob {

    static final MapStateDescriptor<String, Recs.Price> PRICES =
            new MapStateDescriptor<>("prices", Types.STRING, TypeInformation.of(Recs.Price.class));
    static final OutputTag<Recs.AcctUpdate> ALLOCATIONS =
            new OutputTag<>("allocations", TypeInformation.of(Recs.AcctUpdate.class));
    static final OutputTag<Recs.KV> MARKET_VALUES =
            new OutputTag<>("market-values", TypeInformation.of(Recs.KV.class));

    public static void main(String[] argv) throws Exception {
        Args a = new Args(argv);
        String bootstrap = a.get("bootstrap");
        String group = a.get("group");
        int par = a.getInt("parallelism");
        long ckptMs = a.getLong("checkpointMs");
        int partitions = a.getInt("partitions");
        long mvIntervalMs = a.getLong("mvIntervalMs", 10_000L);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(par);
        env.enableCheckpointing(ckptMs, CheckpointingMode.EXACTLY_ONCE);
        env.getConfig().enableObjectReuse();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1000, Time.seconds(2)));

        KafkaSource<byte[]> orders = KafkaSource.<byte[]>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(a.get("in"))
                .setGroupId(group)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new RawBytes())
                .setProperty("commit.offsets.on.checkpoint", "true")
                .build();
        KafkaSource<byte[]> prices = KafkaSource.<byte[]>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(a.get("prices"))
                .setGroupId(group + "-prices")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new RawBytes())
                .build();

        SingleOutputStreamOperator<Recs.SymUpdate> parsed = env
                .fromSource(orders, WatermarkStrategy.noWatermarks(), "order-source").uid("order-source")
                .process(new Parse()).name("parse").uid("parse");
        DataStream<Recs.AcctUpdate> allocations = parsed.getSideOutput(ALLOCATIONS);

        BroadcastStream<Recs.Price> priceFeed = env
                .fromSource(prices, WatermarkStrategy.noWatermarks(), "prices-source").uid("prices-source")
                .map(PositionsJob::parsePrice).returns(Recs.Price.class).name("parse-prices").uid("parse-prices")
                .broadcast(PRICES);

        SingleOutputStreamOperator<Recs.KV> bySymbol = parsed
                .keyBy(u -> u.symbol, Types.STRING)
                .connect(priceFeed)
                .process(new SymbolPositions(partitions, mvIntervalMs))
                .returns(Recs.KV.class)
                .name("position-mv-by-symbol").uid("position-mv-by-symbol");
        SingleOutputStreamOperator<Recs.KV> byAccount = allocations
                .keyBy(u -> u.key, Types.STRING)
                .connect(priceFeed)
                .process(new AccountPositions(partitions, mvIntervalMs))
                .returns(Recs.KV.class)
                .name("position-mv-by-account").uid("position-mv-by-account");

        bySymbol.sinkTo(sink(bootstrap, a.get("outSym"))).name("sink-" + a.get("outSym")).uid("sink-pos-sym");
        byAccount.sinkTo(sink(bootstrap, a.get("outAcct"))).name("sink-" + a.get("outAcct")).uid("sink-pos-acct");
        bySymbol.getSideOutput(MARKET_VALUES).sinkTo(sink(bootstrap, a.get("mvSym")))
                .name("sink-" + a.get("mvSym")).uid("sink-mv-sym");
        byAccount.getSideOutput(MARKET_VALUES).sinkTo(sink(bootstrap, a.get("mvAcct")))
                .name("sink-" + a.get("mvAcct")).uid("sink-mv-acct");

        env.execute("st53 positions and market values");
    }

    /** The source hands over the raw value; parsing is its own operator, done once. */
    public static final class RawBytes implements DeserializationSchema<byte[]> {
        @Override public byte[] deserialize(byte[] message) { return message; }
        @Override public boolean isEndOfStream(byte[] nextElement) { return false; }
        @Override public TypeInformation<byte[]> getProducedType() { return PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO; }
    }

    static KafkaSink<Recs.KV> sink(String bootstrap, String topic) {
        Properties p = new Properties();
        p.put("compression.type", "lz4");
        p.put("linger.ms", "20");
        p.put("batch.size", "262144");
        return KafkaSink.<Recs.KV>builder()
                .setBootstrapServers(bootstrap)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setKafkaProducerConfig(p)
                .setRecordSerializer(new KVSerializer(topic))
                .build();
    }

    /** Keyed by the aggregation key, so each key lands in exactly one partition. */
    public static final class KVSerializer implements KafkaRecordSerializationSchema<Recs.KV> {
        private final String topic;
        KVSerializer(String topic) { this.topic = topic; }
        @Override
        public ProducerRecord<byte[], byte[]> serialize(Recs.KV e, KafkaSinkContext ctx, Long ts) {
            return new ProducerRecord<>(topic, e.key.getBytes(StandardCharsets.UTF_8),
                    e.value.getBytes(StandardCharsets.UTF_8));
        }
    }

    static final JsonFactory JSON = new JsonFactory();

    static Recs.Price parsePrice(byte[] v) throws IOException {
        Recs.Price p = new Recs.Price();
        try (JsonParser jp = JSON.createParser(v)) {
            expect(jp.nextToken(), JsonToken.START_OBJECT);
            while (jp.nextToken() == JsonToken.FIELD_NAME) {
                String f = jp.getCurrentName();
                jp.nextToken();
                switch (f) {
                    case "symbol": p.symbol = jp.getText(); break;
                    case "ts": p.ts = jp.getLongValue(); break;
                    case "price": p.cents = jp.getLongValue(); break;
                    default: jp.skipChildren();
                }
            }
        }
        return p;
    }

    static void expect(JsonToken got, JsonToken want) throws IOException {
        if (got != want) throw new IOException("expected " + want + ", got " + got);
    }

    /** Parse each order once: the order to the main output, its allocations to a side output. */
    public static final class Parse extends ProcessFunction<byte[], Recs.SymUpdate> {
        @Override
        public void processElement(byte[] v, Context ctx, Collector<Recs.SymUpdate> out) throws Exception {
            long id = -1, qty = 0;
            String symbol = null;
            String[] acc = new String[Model.ALLOCS], sub = new String[Model.ALLOCS];
            long[] aq = new long[Model.ALLOCS];
            int n = 0;
            try (JsonParser jp = JSON.createParser(v)) {
                expect(jp.nextToken(), JsonToken.START_OBJECT);
                while (jp.nextToken() == JsonToken.FIELD_NAME) {
                    String f = jp.getCurrentName();
                    JsonToken t = jp.nextToken();
                    switch (f) {
                        case "id": id = jp.getLongValue(); break;
                        case "symbol": symbol = jp.getText(); break;
                        case "qty": qty = jp.getLongValue(); break;
                        case "allocs":
                            expect(t, JsonToken.START_ARRAY);
                            while (jp.nextToken() == JsonToken.START_OBJECT) {
                                if (n == acc.length) {
                                    acc = java.util.Arrays.copyOf(acc, n * 2);
                                    sub = java.util.Arrays.copyOf(sub, n * 2);
                                    aq = java.util.Arrays.copyOf(aq, n * 2);
                                }
                                while (jp.nextToken() == JsonToken.FIELD_NAME) {
                                    String g = jp.getCurrentName();
                                    jp.nextToken();
                                    switch (g) {
                                        case "account": acc[n] = jp.getText(); break;
                                        case "sub": sub[n] = jp.getText(); break;
                                        case "qty": aq[n] = jp.getLongValue(); break;
                                        default: jp.skipChildren();
                                    }
                                }
                                n++;
                            }
                            break;
                        default: jp.skipChildren();
                    }
                }
            }
            if (id < 0 || symbol == null) throw new IOException("order without id or symbol: " + new String(v, StandardCharsets.UTF_8));
            out.collect(new Recs.SymUpdate(id, symbol, qty));
            for (int i = 0; i < n; i++) {
                ctx.output(ALLOCATIONS, new Recs.AcctUpdate(id, acc[i] + "|" + sub[i] + "|" + symbol, symbol, aq[i]));
            }
        }
    }

    /**
     * Per key: [0] position, [1] orders applied, [2] orders at the last market value (-1 none),
     * [3] price timestamp at the last market value (-1 none), [4..4+P) highest id applied from each
     * input partition -- an id at or below it is a repeat (ASSUMPTIONS.md A10).
     */
    abstract static class Positions<I> extends KeyedBroadcastProcessFunction<String, I, Recs.Price, Recs.KV>
            implements CheckpointedFunction {
        final int partitions;
        final long intervalMs;
        transient ValueState<long[]> state;
        transient ListState<Long> keysState;
        transient long keys;
        transient Counter quantity, repeats;
        final StringBuilder sb = new StringBuilder(160);

        Positions(int partitions, long intervalMs) { this.partitions = partitions; this.intervalMs = intervalMs; }

        @Override
        public void open(Configuration conf) {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("position",
                    PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO));
            getRuntimeContext().getMetricGroup().gauge("distinctKeys", () -> keys);
            quantity = getRuntimeContext().getMetricGroup().counter("quantityApplied");
            repeats = getRuntimeContext().getMetricGroup().counter("repeatsDropped");
        }

        @Override
        public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
            keysState.update(java.util.Collections.singletonList(keys));
        }

        @Override
        public void initializeState(FunctionInitializationContext ctx) throws Exception {
            keysState = ctx.getOperatorStateStore().getListState(new ListStateDescriptor<>("distinct-keys", Types.LONG));
            keys = 0;
            for (Long k : keysState.get()) keys += k;
        }

        /** Applies one update; returns the key's state, or null when it was a repeat. */
        long[] apply(long id, long qty, ReadOnlyContext ts) throws Exception {
            long[] s = state.value();
            if (s == null) {
                s = new long[4 + partitions];
                java.util.Arrays.fill(s, -1L);
                s[0] = 0; s[1] = 0;
                keys++;
                long now = ts.currentProcessingTime();
                ts.timerService().registerProcessingTimeTimer((now / intervalMs + 1) * intervalMs);
            }
            int p = (int) (id % partitions);
            if (id <= s[4 + p]) {
                repeats.inc();
                return null;
            }
            s[4 + p] = id;
            s[0] += qty;
            s[1]++;
            state.update(s);
            quantity.inc(Math.abs(qty));
            return s;
        }

        abstract String symbolOf(String key);
        abstract void head(StringBuilder sb, String key);

        @Override
        public void processBroadcastElement(Recs.Price p, Context ctx, Collector<Recs.KV> out) throws Exception {
            BroadcastState<String, Recs.Price> bs = ctx.getBroadcastState(PRICES);
            Recs.Price cur = bs.get(p.symbol);
            if (cur == null || p.ts > cur.ts) {
                bs.put(p.symbol, new Recs.Price(p.symbol, p.ts, p.cents));
            }
        }

        @Override
        public void onTimer(long t, OnTimerContext ctx, Collector<Recs.KV> out) throws Exception {
            String key = ctx.getCurrentKey();
            long[] s = state.value();
            ReadOnlyBroadcastState<String, Recs.Price> bs = ctx.getBroadcastState(PRICES);
            Recs.Price p = bs.get(symbolOf(key));
            if (s != null && p != null && (s[2] != s[1] || s[3] != p.ts)) {
                sb.setLength(0);
                head(sb, key);
                sb.append(",\"position\":").append(s[0]).append(",\"price\":").append(p.cents)
                  .append(",\"priceTs\":").append(p.ts).append(",\"marketValue\":").append(s[0] * p.cents)
                  .append(",\"orders\":").append(s[1]).append('}');
                ctx.output(MARKET_VALUES, new Recs.KV(key, sb.toString()));
                s[2] = s[1];
                s[3] = p.ts;
                state.update(s);
            }
            ctx.timerService().registerProcessingTimeTimer(t + intervalMs);
        }

        void emitPosition(String key, long[] s, Collector<Recs.KV> out) {
            sb.setLength(0);
            head(sb, key);
            sb.append(",\"position\":").append(s[0]).append(",\"orders\":").append(s[1]).append('}');
            out.collect(new Recs.KV(key, sb.toString()));
        }
    }

    public static final class SymbolPositions extends Positions<Recs.SymUpdate> {
        SymbolPositions(int partitions, long intervalMs) { super(partitions, intervalMs); }

        @Override
        public void processElement(Recs.SymUpdate u, ReadOnlyContext ctx, Collector<Recs.KV> out) throws Exception {
            long[] s = apply(u.id, u.qty, ctx);
            if (s != null) emitPosition(u.symbol, s, out);
        }

        @Override String symbolOf(String key) { return key; }
        @Override void head(StringBuilder sb, String key) { sb.append("{\"symbol\":\"").append(key).append('"'); }
    }

    public static final class AccountPositions extends Positions<Recs.AcctUpdate> {
        AccountPositions(int partitions, long intervalMs) { super(partitions, intervalMs); }

        @Override
        public void processElement(Recs.AcctUpdate u, ReadOnlyContext ctx, Collector<Recs.KV> out) throws Exception {
            long[] s = apply(u.id, u.qty, ctx);
            if (s != null) emitPosition(u.key, s, out);
        }

        // key = "A1|S1|SYM0001"
        @Override String symbolOf(String key) { return key.substring(6); }
        @Override void head(StringBuilder sb, String key) {
            sb.append("{\"account\":\"").append(key, 0, 2).append("\",\"sub\":\"").append(key, 3, 5)
              .append("\",\"symbol\":\"").append(key, 6, key.length()).append('"');
        }
    }

}
