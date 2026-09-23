package st44;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * The deterministic generator, and the manifest that says what the right answer
 * is.
 *
 * Two modes over one walk of the same stream, so the expected answer can never
 * drift from the data:
 *
 *   --mode=produce   write the readings to Kafka and write the manifest
 *   --mode=manifest  write only the manifest (the determinism preflight runs
 *                    this twice with one seed and compares the bytes)
 *
 * Reading i of the stream belongs to sensor (i mod 1000) in simulated second
 * (i div 1000). Everything else -- the location, the event time, the
 * temperature -- is a pure function of those two and the seed (Spec).
 *
 * The manifest holds the expected total and count for every (location, hour)
 * the pipeline is required to publish. A location's NEWEST hour is not in it:
 * nothing arrives after it to close it, so the pipeline never publishes it.
 * That is a consequence of ANSWERS.md question 4 -- a published hour is never
 * reopened -- and is written down in ASSUMPTIONS.md B3.
 */
public final class Gen {

    // Fixed-width record. Every field has a constant length, so the bytes are
    // patched in place rather than rebuilt, and the record size is exactly
    // known before anything is measured.
    private static final byte[] TEMPLATE =
            ("{\"sensorId\":\"sensor-0000\",\"location\":\"loc-00\","
                    + "\"readingTime\":\"2026-01-01T00:00:00Z\",\"temperatureC\":15.0}")
                    .getBytes(StandardCharsets.US_ASCII);
    private static final int SENSOR_OFF = 13;   // 11 bytes: sensor-NNNN
    private static final int LOC_OFF = 38;      // 6 bytes:  loc-NN
    private static final int TIME_OFF = 61;     // 20 bytes: 2026-01-01T00:00:00Z
    private static final int TEMP_OFF = 98;     // 4 bytes:  NN.N
    static final int RECORD_BYTES = TEMPLATE.length;   // 103

    public static void main(String[] args) throws Exception {
        String mode = Spec.arg(args, "mode", "produce");
        long count = Long.parseLong(Spec.arg(args, "count", null));
        long seed = Long.parseLong(Spec.arg(args, "seed", null));
        String manifestPath = Spec.arg(args, "manifest", null);
        boolean produce = mode.equals("produce");
        String topic = Spec.arg(args, "topic", produce ? null : "unused");
        String bootstrap = Spec.arg(args, "bootstrap", produce ? null : "unused");
        int partitions = Integer.parseInt(Spec.arg(args, "partitions", "8"));

        if (RECORD_BYTES != 103) {
            throw new IllegalStateException("record template is " + RECORD_BYTES + " bytes, expected 103");
        }

        Walk walk = new Walk(seed, count, partitions);
        long t0 = System.nanoTime();
        if (produce) {
            Properties p = new Properties();
            p.put("bootstrap.servers", bootstrap);
            p.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
            p.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
            // ASSUMPTIONS.md B7: lz4 on the way in. Four times as much of the
            // backlog fits the broker's page cache, and the decompression cost
            // lands on the component under test.
            p.put("compression.type", "lz4");
            p.put("batch.size", "262144");
            p.put("linger.ms", "50");
            p.put("acks", "1");
            p.put("buffer.memory", "134217728");
            p.put("max.request.size", "8388608");
            try (Producer<byte[], byte[]> producer = new KafkaProducer<>(p)) {
                walk.run(producer, topic);
                producer.flush();
            }
        } else {
            walk.run(null, null);
        }
        double secs = (System.nanoTime() - t0) / 1e9;
        walk.writeManifest(manifestPath, seed, count);
        System.out.printf("%s %,d readings in %.1fs (%,.0f rec/s), %,d closed windows, %,d readings in them%n",
                produce ? "produced" : "walked", count, secs, count / Math.max(secs, 1e-9),
                walk.closedWindows, walk.closedReadings);
    }

    /** One walk of the stream. Produces, accumulates, or both. */
    static final class Walk {
        private final long seed;
        private final long count;
        private final int partitions;
        private final byte[] buf = TEMPLATE.clone();
        private final byte[][] keyBytes = new byte[Spec.LOCATIONS][];
        private final byte[][] locBytes = new byte[Spec.LOCATIONS][];
        private final byte[][] sensorBytes = new byte[Spec.SENSORS][];
        // per location: the hour currently open, its total and count
        private final long[] openHour = new long[Spec.LOCATIONS];
        private final long[] openTenths = new long[Spec.LOCATIONS];
        private final long[] openCount = new long[Spec.LOCATIONS];
        private final boolean[] started = new boolean[Spec.LOCATIONS];
        // per location: every hour already closed, in order
        private final List<List<long[]>> closed = new ArrayList<>(Spec.LOCATIONS);
        long closedWindows;
        long closedReadings;
        // cached civil date for the timestamp formatter
        private long cachedDay = Long.MIN_VALUE;
        private final byte[] dayBytes = new byte[10];

        Walk(long seed, long count, int partitions) {
            this.seed = seed;
            this.count = count;
            this.partitions = partitions;
            for (int l = 0; l < Spec.LOCATIONS; l++) {
                String name = Spec.locationName(l);
                keyBytes[l] = name.getBytes(StandardCharsets.US_ASCII);
                locBytes[l] = keyBytes[l];
                closed.add(new ArrayList<>());
            }
            for (int s = 0; s < Spec.SENSORS; s++) {
                sensorBytes[s] = Spec.sensorName(s).getBytes(StandardCharsets.US_ASCII);
            }
        }

        void run(Producer<byte[], byte[]> producer, String topic) {
            long produced = 0;
            long second = 0;
            while (produced < count) {
                for (int sensor = 0; sensor < Spec.SENSORS && produced < count; sensor++) {
                    int loc = Spec.locationOfSensor(sensor);
                    long eventMs = Spec.eventMillis(sensor, second);
                    long hour = Spec.hourOf(eventMs);
                    int tenths = Spec.tenths(seed, sensor, second);

                    if (!started[loc]) {
                        started[loc] = true;
                        openHour[loc] = hour;
                    } else if (hour > openHour[loc]) {
                        closed.get(loc).add(new long[]{openHour[loc], openTenths[loc], openCount[loc]});
                        closedWindows++;
                        closedReadings += openCount[loc];
                        openHour[loc] = hour;
                        openTenths[loc] = 0;
                        openCount[loc] = 0;
                    }
                    openTenths[loc] += tenths;
                    openCount[loc]++;

                    if (producer != null) {
                        System.arraycopy(sensorBytes[sensor], 0, buf, SENSOR_OFF, 11);
                        System.arraycopy(locBytes[loc], 0, buf, LOC_OFF, 6);
                        writeIso(eventMs);
                        writeTemp(tenths);
                        producer.send(new ProducerRecord<>(topic, loc % partitions,
                                keyBytes[loc], buf.clone()));
                    }
                    produced++;
                    if (producer != null && (produced % 50_000_000L) == 0) {
                        System.out.printf("  %,d of %,d readings%n", produced, count);
                    }
                }
                second++;
            }
        }

        private void writeTemp(int tenths) {
            int whole = tenths / 10;
            buf[TEMP_OFF] = (byte) ('0' + whole / 10);
            buf[TEMP_OFF + 1] = (byte) ('0' + whole % 10);
            buf[TEMP_OFF + 2] = '.';
            buf[TEMP_OFF + 3] = (byte) ('0' + tenths % 10);
        }

        /** yyyy-MM-ddTHH:mm:ssZ, 20 bytes, written straight into the template. */
        private void writeIso(long eventMs) {
            long epochSec = eventMs / 1000L;
            long day = Math.floorDiv(epochSec, 86400L);
            int sod = (int) Math.floorMod(epochSec, 86400L);
            if (day != cachedDay) {
                civil(day, dayBytes);
                cachedDay = day;
            }
            System.arraycopy(dayBytes, 0, buf, TIME_OFF, 10);
            buf[TIME_OFF + 10] = 'T';
            two(TIME_OFF + 11, sod / 3600);
            buf[TIME_OFF + 13] = ':';
            two(TIME_OFF + 14, (sod / 60) % 60);
            buf[TIME_OFF + 16] = ':';
            two(TIME_OFF + 17, sod % 60);
            buf[TIME_OFF + 19] = 'Z';
        }

        private void two(int off, int v) {
            buf[off] = (byte) ('0' + v / 10);
            buf[off + 1] = (byte) ('0' + v % 10);
        }

        /** days-since-epoch to yyyy-MM-dd (Howard Hinnant's civil_from_days). */
        static void civil(long z, byte[] out) {
            z += 719468;
            long era = Math.floorDiv(z, 146097L);
            long doe = z - era * 146097L;
            long yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
            long y = yoe + era * 400;
            long doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
            long mp = (5 * doy + 2) / 153;
            long d = doy - (153 * mp + 2) / 5 + 1;
            long m = mp + (mp < 10 ? 3 : -9);
            if (m <= 2) {
                y++;
            }
            out[0] = (byte) ('0' + (int) (y / 1000) % 10);
            out[1] = (byte) ('0' + (int) (y / 100) % 10);
            out[2] = (byte) ('0' + (int) (y / 10) % 10);
            out[3] = (byte) ('0' + (int) y % 10);
            out[4] = '-';
            out[5] = (byte) ('0' + (int) (m / 10));
            out[6] = (byte) ('0' + (int) (m % 10));
            out[7] = '-';
            out[8] = (byte) ('0' + (int) (d / 10));
            out[9] = (byte) ('0' + (int) (d % 10));
        }

        void writeManifest(String path, long seed, long count) throws Exception {
            try (Writer w = new BufferedWriter(new FileWriter(path), 1 << 20)) {
                w.write("{\n");
                w.write("  \"readingCount\": " + count + ",\n");
                w.write("  \"seed\": " + seed + ",\n");
                w.write("  \"locationCount\": " + Spec.LOCATIONS + ",\n");
                w.write("  \"sensorCount\": " + Spec.SENSORS + ",\n");
                w.write("  \"sensorsPerLocation\": " + Spec.SENSORS_PER_LOCATION + ",\n");
                w.write("  \"recordBytes\": " + RECORD_BYTES + ",\n");
                w.write("  \"hourMs\": " + Spec.HOUR_MS + ",\n");
                w.write("  \"locationStaggerSeconds\": " + Spec.LOCATION_STAGGER_S + ",\n");
                w.write("  \"partitions\": " + partitions + ",\n");
                w.write("  \"closedWindowCount\": " + closedWindows + ",\n");
                w.write("  \"readingsInClosedWindows\": " + closedReadings + ",\n");
                w.write("  \"locationKeys\": [");
                for (int l = 0; l < Spec.LOCATIONS; l++) {
                    w.write((l == 0 ? "" : ", ") + "\"" + Spec.locationName(l) + "\"");
                }
                w.write("],\n");
                w.write("  \"expected\": [\n");
                boolean first = true;
                for (int l = 0; l < Spec.LOCATIONS; l++) {
                    for (long[] e : closed.get(l)) {
                        if (!first) {
                            w.write(",\n");
                        }
                        first = false;
                        w.write("    {\"location\": \"" + Spec.locationName(l)
                                + "\", \"hourStart\": \"" + Spec.hourStart(e[0])
                                + "\", \"totalC\": " + Spec.decimal(e[1])
                                + ", \"count\": " + e[2] + "}");
                    }
                }
                w.write("\n  ]\n}\n");
            }
        }
    }

    private Gen() {
    }
}
