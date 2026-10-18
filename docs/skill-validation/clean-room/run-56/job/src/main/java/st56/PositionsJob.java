package st56;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;

/**
 * Orders in, positions by symbol and by account / sub-account / symbol out, one
 * record per input at each level; prices broadcast to both aggregations, and a
 * market value per key at most once per interval (default 10 s) at both levels.
 */
public final class PositionsJob {

    /** One change to one position: the symbol level, or one allocation at the account level. */
    public static class Delta {
        public String key;
        public String account;
        public String sub;
        public String symbol;
        public long id;
        public long qty;

        public Delta() {}
    }

    public static class Price {
        public String symbol;
        public long ts;
        public long priceCents;

        public Price() {}
    }

    /** A record bound for Kafka: its key and its JSON value. */
    public static class Out {
        public String key;
        public String value;

        public Out() {}

        Out(String k, String v) {
            key = k;
            value = v;
        }
    }

    public static class PosState {
        public long pos;
        public long n;
        public long lastId = -1;
        public long timerAt;

        public PosState() {}
    }

    static final OutputTag<Delta> ALLOCATIONS = new OutputTag<>("allocations", TypeInformation.of(Delta.class));
    static final OutputTag<Out> MARKET_VALUES = new OutputTag<>("market-values", TypeInformation.of(Out.class));
    static final MapStateDescriptor<String, Price> PRICES =
            new MapStateDescriptor<>("latest-price", Types.STRING, TypeInformation.of(Price.class));

    public static void main(String[] argv) throws Exception {
        Map<String, String> a = Args.parse(argv);
        String bootstrap = Args.need(a, "bootstrap");
        String in = Args.need(a, "in");
        String prices = Args.need(a, "prices");
        String outSym = Args.need(a, "outSym");
        String outAcct = Args.need(a, "outAcct");
        String mvSym = Args.need(a, "mvSym");
        String mvAcct = Args.need(a, "mvAcct");
        String group = Args.need(a, "group");
        int par = Integer.parseInt(Args.need(a, "parallelism"));
        long ckptMs = Long.parseLong(Args.need(a, "checkpointMs"));
        long mvIntervalMs = Long.parseLong(a.getOrDefault("mvIntervalMs", "10000"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(par);
        env.enableCheckpointing(ckptMs, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, Time.seconds(2)));
        env.getConfig().enableObjectReuse();

        KafkaSource<byte[]> orders = KafkaSource.<byte[]>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(in)
                .setGroupId(group)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(ByteArrayDeserializer.class))
                .setProperty("commit.offsets.on.checkpoint", "true")
                .setProperty("max.partition.fetch.bytes", String.valueOf(4 << 20))
                .setProperty("fetch.max.bytes", String.valueOf(32 << 20))
                .build();

        KafkaSource<Price> priceSource = KafkaSource.<Price>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(prices)
                .setGroupId(group + "-prices")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new PriceDeserializer())
                .setProperty("commit.offsets.on.checkpoint", "false")
                .build();

        BroadcastStream<Price> priceBroadcast = env
                .fromSource(priceSource, WatermarkStrategy.noWatermarks(), "kafka-source-prices")
                .uid("kafka-source-prices")
                .broadcast(PRICES);

        SingleOutputStreamOperator<Delta> parsed = env
                .fromSource(orders, WatermarkStrategy.noWatermarks(), "kafka-source-orders")
                .uid("kafka-source-orders")
                .process(new ParseOrders()).name("parse-orders").uid("parse-orders");

        SingleOutputStreamOperator<Out> bySymbol = parsed
                .keyBy(d -> d.key, Types.STRING)
                .connect(priceBroadcast)
                .process(new PositionAndMarketValue(true, mvIntervalMs), TypeInformation.of(Out.class))
                .name("position-and-market-value-by-symbol").uid("position-and-market-value-by-symbol");

        SingleOutputStreamOperator<Out> byAccount = parsed.getSideOutput(ALLOCATIONS)
                .keyBy(d -> d.key, Types.STRING)
                .connect(priceBroadcast)
                .process(new PositionAndMarketValue(false, mvIntervalMs), TypeInformation.of(Out.class))
                .name("position-and-market-value-by-account").uid("position-and-market-value-by-account");

        sink(bySymbol, bootstrap, outSym);
        sink(byAccount, bootstrap, outAcct);
        sink(bySymbol.getSideOutput(MARKET_VALUES), bootstrap, mvSym);
        sink(byAccount.getSideOutput(MARKET_VALUES), bootstrap, mvAcct);

        env.execute("positions-st56");
    }

    static void sink(DataStream<Out> s, String bootstrap, String topic) {
        Properties pp = new Properties();
        pp.put("compression.type", "lz4");
        pp.put("linger.ms", "10");
        pp.put("batch.size", String.valueOf(256 * 1024));
        KafkaSink<Out> k = KafkaSink.<Out>builder()
                .setBootstrapServers(bootstrap)
                .setKafkaProducerConfig(pp)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer(new OutSerializer(topic))
                .build();
        s.sinkTo(k).name("sink-" + topic).uid("sink-" + topic);
    }

    static final class OutSerializer implements KafkaRecordSerializationSchema<Out> {
        private final String topic;

        OutSerializer(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(Out o, KafkaSinkContext ctx, Long ts) {
            return new ProducerRecord<>(topic, o.key.getBytes(StandardCharsets.UTF_8),
                    o.value.getBytes(StandardCharsets.UTF_8));
        }
    }

    static final class PriceDeserializer implements KafkaRecordDeserializationSchema<Price> {
        private transient JsonFactory f;

        @Override
        public void deserialize(ConsumerRecord<byte[], byte[]> r, Collector<Price> out) throws IOException {
            if (f == null) f = new JsonFactory();
            Price p = new Price();
            try (JsonParser jp = f.createParser(r.value())) {
                jp.nextToken();
                while (jp.nextToken() == JsonToken.FIELD_NAME) {
                    String name = jp.getCurrentName();
                    jp.nextToken();
                    switch (name) {
                        case "symbol": p.symbol = jp.getText(); break;
                        case "ts": p.ts = jp.getLongValue(); break;
                        case "priceCents": p.priceCents = jp.getLongValue(); break;
                        default: jp.skipChildren();
                    }
                }
            }
            out.collect(p);
        }

        @Override
        public TypeInformation<Price> getProducedType() {
            return TypeInformation.of(Price.class);
        }
    }

    /** Parse each order once; the symbol change on the main output, its four allocations on a side output. */
    static final class ParseOrders extends ProcessFunction<byte[], Delta> {
        private transient JsonFactory f;
        private transient Counter parsedOrders;

        @Override
        public void open(Configuration parameters) {
            f = new JsonFactory();
            parsedOrders = getRuntimeContext().getMetricGroup().counter("ordersParsed");
        }

        @Override
        public void processElement(byte[] value, Context ctx, Collector<Delta> out) throws Exception {
            long id = 0, qty = 0;
            String symbol = null;
            String[] acc = new String[Model.ALLOCATIONS], sub = new String[Model.ALLOCATIONS];
            long[] aq = new long[Model.ALLOCATIONS];
            int na = 0;
            try (JsonParser jp = f.createParser(value)) {
                if (jp.nextToken() != JsonToken.START_OBJECT) throw new IOException("order is not an object");
                while (jp.nextToken() == JsonToken.FIELD_NAME) {
                    String name = jp.getCurrentName();
                    JsonToken t = jp.nextToken();
                    switch (name) {
                        case "id": id = jp.getLongValue(); break;
                        case "symbol": symbol = jp.getText(); break;
                        case "qty": qty = jp.getLongValue(); break;
                        case "allocations":
                            if (t != JsonToken.START_ARRAY) throw new IOException("allocations is not an array");
                            while (jp.nextToken() == JsonToken.START_OBJECT) {
                                if (na == acc.length) {
                                    acc = java.util.Arrays.copyOf(acc, na * 2);
                                    sub = java.util.Arrays.copyOf(sub, na * 2);
                                    aq = java.util.Arrays.copyOf(aq, na * 2);
                                }
                                while (jp.nextToken() == JsonToken.FIELD_NAME) {
                                    String an = jp.getCurrentName();
                                    jp.nextToken();
                                    switch (an) {
                                        case "account": acc[na] = jp.getText(); break;
                                        case "sub": sub[na] = jp.getText(); break;
                                        case "qty": aq[na] = jp.getLongValue(); break;
                                        default: jp.skipChildren();
                                    }
                                }
                                na++;
                            }
                            break;
                        default: jp.skipChildren();
                    }
                }
            }
            Delta d = new Delta();
            d.key = symbol;
            d.symbol = symbol;
            d.id = id;
            d.qty = qty;
            out.collect(d);
            for (int i = 0; i < na; i++) {
                Delta x = new Delta();
                x.account = acc[i];
                x.sub = sub[i];
                x.symbol = symbol;
                x.key = Model.accountKey(acc[i], sub[i], symbol);
                x.id = id;
                x.qty = aq[i];
                ctx.output(ALLOCATIONS, x);
            }
            parsedOrders.inc();
        }
    }

    /**
     * The running position for one key, published on every change, and its
     * market value published at most once per interval. Prices arrive by
     * broadcast; the latest is the one with the highest timestamp.
     *
     * Repeats are dropped by order id: every key lives in one input partition
     * and ids rise within a partition, so an id at or below the last one
     * applied to this key is a repeat. One number per key, not a set of ids.
     */
    static final class PositionAndMarketValue extends KeyedBroadcastProcessFunction<String, Delta, Price, Out> {
        private final boolean symbolLevel;
        private final long intervalMs;
        private transient ValueState<PosState> state;
        private transient Counter repeatsDropped, marketValues;

        PositionAndMarketValue(boolean symbolLevel, long intervalMs) {
            this.symbolLevel = symbolLevel;
            this.intervalMs = intervalMs;
        }

        @Override
        public void open(Configuration parameters) {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("position", TypeInformation.of(PosState.class)));
            repeatsDropped = getRuntimeContext().getMetricGroup().counter("repeatsDropped");
            marketValues = getRuntimeContext().getMetricGroup().counter("marketValuesEmitted");
        }

        @Override
        public void processElement(Delta d, ReadOnlyContext ctx, Collector<Out> out) throws Exception {
            PosState st = state.value();
            if (st == null) st = new PosState();
            if (d.id <= st.lastId) {
                repeatsDropped.inc();
                return;
            }
            st.pos += d.qty;
            st.n++;
            st.lastId = d.id;
            StringBuilder b = new StringBuilder(128).append('{');
            if (!symbolLevel) {
                b.append("\"account\":\"").append(d.account).append("\",\"sub\":\"").append(d.sub).append("\",");
            }
            b.append("\"symbol\":\"").append(d.symbol).append("\",\"position\":").append(st.pos)
             .append(",\"n\":").append(st.n).append(",\"orderId\":").append(d.id).append('}');
            out.collect(new Out(d.key, b.toString()));
            if (st.timerAt == 0) {
                long now = ctx.timerService().currentProcessingTime();
                st.timerAt = (now / intervalMs + 1) * intervalMs;
                ctx.timerService().registerProcessingTimeTimer(st.timerAt);
            }
            state.update(st);
        }

        @Override
        public void processBroadcastElement(Price p, Context ctx, Collector<Out> out) throws Exception {
            Price have = ctx.getBroadcastState(PRICES).get(p.symbol);
            if (have == null || p.ts > have.ts) {
                Price copy = new Price();
                copy.symbol = p.symbol;
                copy.ts = p.ts;
                copy.priceCents = p.priceCents;
                ctx.getBroadcastState(PRICES).put(p.symbol, copy);
            }
        }

        @Override
        public void onTimer(long ts, OnTimerContext ctx, Collector<Out> out) throws Exception {
            PosState st = state.value();
            if (st == null) return;
            String key = ctx.getCurrentKey();
            String symbol = symbolLevel ? key : key.substring(key.lastIndexOf('/') + 1);
            Price p = ctx.getBroadcastState(PRICES).get(symbol);
            if (p == null) {
                // no price yet: try again next interval
                st.timerAt = (ts / intervalMs + 1) * intervalMs;
                ctx.timerService().registerProcessingTimeTimer(st.timerAt);
                state.update(st);
                return;
            }
            st.timerAt = 0;
            state.update(st);
            StringBuilder b = new StringBuilder(160).append('{');
            if (!symbolLevel) {
                int s1 = key.indexOf('/'), s2 = key.lastIndexOf('/');
                b.append("\"account\":\"").append(key, 0, s1).append("\",\"sub\":\"").append(key, s1 + 1, s2).append("\",");
            }
            b.append("\"symbol\":\"").append(symbol).append("\",\"position\":").append(st.pos)
             .append(",\"priceCents\":").append(p.priceCents)
             .append(",\"marketValueCents\":").append(Math.multiplyExact(st.pos, p.priceCents))
             .append(",\"priceTs\":").append(p.ts)
             .append(",\"n\":").append(st.n).append('}');
            ctx.output(MARKET_VALUES, new Out(key, b.toString()));
            marketValues.inc();
        }
    }
}
