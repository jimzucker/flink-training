package st44;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;

/**
 * The pipeline of ANSWERS.md.
 *
 *   readings (Kafka) -> parse reading -> keyBy location
 *                    -> hourly average by location -> publish hourly average (Kafka)
 *
 * One row per location per one-hour bucket, carrying the TOTAL and the COUNT
 * (question 4: not a rounded average). The bucket comes from the timestamp
 * inside the reading; the machine clock is never consulted, there are no
 * watermarks and no timers.
 *
 * A location's open hour is published the moment a reading for a LATER hour
 * arrives for that location, and a reading for an hour at or before the one
 * already published is dropped -- question 4's "late readings are ignored; a
 * reading whose hour has already been published does not reopen it". The
 * consequence, written down in ASSUMPTIONS.md B3, is that the newest hour of
 * each location is never published, because nothing arrives to close it.
 *
 * Guarantee: exactly-once checkpointing for the state, and an at-least-once
 * sink made idempotent by publishing the absolute total and count for each
 * (location, hour) -- a replayed row overwrites itself with the same bytes.
 */
public final class TempJob {

    public static void main(String[] args) throws Exception {
        String bootstrap = Spec.arg(args, "bootstrap", null);
        String in = Spec.arg(args, "in", null);
        String out = Spec.arg(args, "out", null);
        String group = Spec.arg(args, "group", null);
        int par = Integer.parseInt(Spec.arg(args, "parallelism", null));
        long ckptMs = Long.parseLong(Spec.arg(args, "checkpointMs", null));

        Configuration conf = new Configuration();
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 20);
        conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(3));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(par);
        env.enableCheckpointing(ckptMs, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(5);
        env.getConfig().enableObjectReuse();

        KafkaSource<byte[]> source = KafkaSource.<byte[]>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(in)
                .setGroupId(group)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(ByteArrayDeserializer.class))
                // the harness reads the rate from COMMITTED offsets, so the
                // source must commit them on every checkpoint
                .setProperty("commit.offsets.on.checkpoint", "true")
                .setProperty("partition.discovery.interval.ms", "0")
                .setProperty("fetch.max.bytes", "67108864")
                .setProperty("max.partition.fetch.bytes", "8388608")
                .setProperty("fetch.min.bytes", "131072")
                .build();

        KafkaSink<HourlyRow> sink = KafkaSink.<HourlyRow>builder()
                .setBootstrapServers(bootstrap)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer(new RowSerializer(out))
                // idempotent producer: retries cannot reorder a key's rows, and
                // per-key order is the one ordering a keyed stream promises
                .setProperty("enable.idempotence", "true")
                .setProperty("acks", "all")
                .setProperty("max.in.flight.requests.per.connection", "5")
                .setProperty("linger.ms", "5")
                .setProperty("compression.type", "lz4")
                .build();

        DataStream<byte[]> raw = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "read temperature readings")
                .uid("source-readings");

        DataStream<Reading> readings = raw
                .map(new ParseReading())
                .name("parse reading")
                .uid("parse-reading");

        DataStream<HourlyRow> hourly = readings
                .keyBy((org.apache.flink.api.java.functions.KeySelector<Reading, String>) r -> r.location)
                .process(new HourlyAverageByLocation())
                .name("hourly average by location")
                .uid("hourly-average-by-location");

        hourly.sinkTo(sink)
                .name("publish hourly average")
                .uid("publish-hourly-average");

        env.execute("temperature hourly average by location");
    }

    // ------------------------------------------------------------- record types

    /** One reading, already parsed: which location, which hour, how many tenths of a degree. */
    public static final class Reading {
        public String location;
        public long hour;
        public int tenths;

        public Reading() {
        }
    }

    /** One published row: the absolute total and count for one (location, hour). */
    public static final class HourlyRow {
        public String location;
        public long hour;
        public long totalTenths;
        public long count;

        public HourlyRow() {
        }

        public HourlyRow(String location, long hour, long totalTenths, long count) {
            this.location = location;
            this.hour = hour;
            this.totalTenths = totalTenths;
            this.count = count;
        }
    }

    /** The open hour for one location. */
    public static final class Acc {
        public long hour;
        public long totalTenths;
        public long count;

        public Acc() {
        }
    }

    // ----------------------------------------------------------------- operators

    /** JSON in, reading out. The temperature never touches a double. */
    public static final class ParseReading extends RichMapFunction<byte[], Reading> {
        private transient JsonFactory factory;

        @Override
        public void open(OpenContext ctx) {
            factory = new JsonFactory();
        }

        @Override
        public Reading map(byte[] value) throws Exception {
            String location = null;
            long eventMs = Long.MIN_VALUE;
            long tenths = Long.MIN_VALUE;
            try (JsonParser p = factory.createParser(value)) {
                if (p.nextToken() != JsonToken.START_OBJECT) {
                    throw new IllegalArgumentException("not a JSON object: "
                            + new String(value, StandardCharsets.UTF_8));
                }
                while (p.nextToken() == JsonToken.FIELD_NAME) {
                    String field = p.currentName();
                    p.nextToken();
                    switch (field) {
                        case "location":
                            location = p.getText();
                            break;
                        case "readingTime":
                            eventMs = Instant.parse(p.getText()).toEpochMilli();
                            break;
                        case "temperatureC":
                            tenths = Spec.parseTenths(p.getText());
                            break;
                        default:
                            break;
                    }
                }
            }
            if (location == null || eventMs == Long.MIN_VALUE || tenths == Long.MIN_VALUE) {
                throw new IllegalArgumentException("incomplete reading: "
                        + new String(value, StandardCharsets.UTF_8));
            }
            Reading r = new Reading();
            r.location = location;
            r.hour = Spec.hourOf(eventMs);
            r.tenths = (int) tenths;
            return r;
        }
    }

    /**
     * One open hour per location, closed by the arrival of a later hour for the
     * same location. No watermarks, no timers, no wall clock: the only thing
     * that moves an hour along is the data of that location.
     */
    public static final class HourlyAverageByLocation
            extends KeyedProcessFunction<String, Reading, HourlyRow> {

        private transient ValueState<Acc> open;

        @Override
        public void open(OpenContext ctx) {
            open = getRuntimeContext().getState(new ValueStateDescriptor<>("open-hour", Acc.class));
        }

        @Override
        public void processElement(Reading r, Context ctx, Collector<HourlyRow> out) throws Exception {
            Acc a = open.value();
            if (a == null) {
                a = new Acc();
                a.hour = r.hour;
            } else if (r.hour > a.hour) {
                out.collect(new HourlyRow(ctx.getCurrentKey(), a.hour, a.totalTenths, a.count));
                a.hour = r.hour;
                a.totalTenths = 0;
                a.count = 0;
            } else if (r.hour < a.hour) {
                // question 4: a reading whose hour has already been published
                // does not reopen it
                return;
            }
            a.totalTenths += r.tenths;
            a.count++;
            open.update(a);
        }
    }

    /** The published row, keyed by location so Kafka keeps one location in one partition. */
    public static final class RowSerializer implements KafkaRecordSerializationSchema<HourlyRow> {
        private static final long serialVersionUID = 1L;
        private final String topic;

        RowSerializer(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(HourlyRow r, KafkaRecordSerializationSchema.KafkaSinkContext ctx, Long ts) {
            String json = "{\"location\":\"" + r.location
                    + "\",\"hourStart\":\"" + Spec.hourStart(r.hour)
                    + "\",\"totalC\":" + Spec.decimal(r.totalTenths)
                    + ",\"count\":" + r.count + "}";
            return new ProducerRecord<>(topic, null,
                    r.location.getBytes(StandardCharsets.UTF_8),
                    json.getBytes(StandardCharsets.UTF_8));
        }
    }

    private TempJob() {
    }
}
