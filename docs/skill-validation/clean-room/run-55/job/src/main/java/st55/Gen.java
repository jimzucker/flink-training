package st55;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Deterministic generator of orders (and, once, of prices).
 *
 * --mode=produce writes {count} records to {topic} and the manifest;
 * --mode=manifest writes only the manifest (same content, no Kafka).
 *
 * Partition p holds the orders for the symbols with index % partitions == p, so
 * every key lives in one partition. Order ids rise within a partition
 * (id = seq * partitions + p). A share of records (dupRate) is written twice on
 * purpose, right after the original and to the same partition, as a retrying
 * producer would. {count} counts every record written, repeats included; the
 * manifest's totals count each order once.
 */
public class Gen {
    static final long T0 = 1_760_000_000_000L;
    static final long PRICE_SEED = 777L;
    static final int PRICE_UPDATES = 3;

    public static void main(String[] argv) throws Exception {
        Args a = new Args(argv);
        String mode = a.get("mode");
        long count = a.getLong("count");
        long seed = a.getLong("seed");
        int parts = (int) a.getLong("partitions", 8);
        double dupRate = a.getDouble("dupRate", 0.005);
        String manifest = a.get("manifest");
        if (Keys.SYMBOLS % parts != 0) throw new IllegalArgumentException("partitions must divide " + Keys.SYMBOLS);

        boolean produce = mode.equals("produce");
        KafkaProducer<byte[], byte[]> producer = null;
        String topic = null;
        if (produce) {
            String bootstrap = a.get("bootstrap");
            topic = a.get("topic");
            Properties p = new Properties();
            p.put("bootstrap.servers", bootstrap);
            p.put("acks", "all");
            p.put("enable.idempotence", "true");
            p.put("compression.type", "lz4");
            p.put("linger.ms", "50");
            p.put("batch.size", String.valueOf(512 * 1024));
            p.put("buffer.memory", String.valueOf(256L * 1024 * 1024));
            p.put("max.request.size", String.valueOf(8 * 1024 * 1024));
            producer = new KafkaProducer<>(p, new ByteArraySerializer(), new ByteArraySerializer());
            writePricesIfEmpty(bootstrap, a.get("pricesTopic"), parts, producer);
        }

        // totals: [symbol], and [pair][symbol]
        long[] symTotal = new long[Keys.SYMBOLS];
        long[] symCount = new long[Keys.SYMBOLS];
        long[][] acctTotal = new long[4][Keys.SYMBOLS];
        AtomicLong dups = new AtomicLong();
        AtomicLong sendErrors = new AtomicLong();
        AtomicReference<Exception> firstError = new AtomicReference<>();

        Thread[] threads = new Thread[parts];
        final KafkaProducer<byte[], byte[]> prod = producer;
        final String tp = topic;
        long t0 = System.currentTimeMillis();
        for (int p = 0; p < parts; p++) {
            final int part = p;
            final long n = count / parts + (p < count % parts ? 1 : 0);
            threads[p] = new Thread(() -> {
                SplittableRandom r = new SplittableRandom(seed * 1_000_003L + part);
                int perPart = Keys.SYMBOLS / parts;
                long seq = 0;
                byte[] prev = null;
                byte[] prevKey = null;
                long localDups = 0;
                StringBuilder b = new StringBuilder(256);
                for (long i = 0; i < n; i++) {
                    byte[] key, val;
                    if (prev != null && r.nextDouble() < dupRate) {
                        key = prevKey; val = prev; prev = null; localDups++;
                    } else {
                        long id = seq * parts + part;
                        seq++;
                        int si = part + parts * r.nextInt(perPart);
                        String sym = Keys.sym(si);
                        long qty = r.nextLong(1, 1001) * (r.nextBoolean() ? 1 : -1);
                        long[] q = new long[4];
                        long rest = qty;
                        for (int k = 0; k < 3; k++) { q[k] = r.nextLong(-400, 401); rest -= q[k]; }
                        q[3] = rest;
                        b.setLength(0);
                        b.append("{\"id\":").append(id).append(",\"sym\":\"").append(sym)
                         .append("\",\"qty\":").append(qty).append(",\"ts\":").append(T0 + id)
                         .append(",\"alloc\":[");
                        for (int k = 0; k < 4; k++) {
                            if (k > 0) b.append(',');
                            b.append("{\"acct\":\"").append(Keys.PAIRS[k][0]).append("\",\"sub\":\"")
                             .append(Keys.PAIRS[k][1]).append("\",\"qty\":").append(q[k]).append('}');
                            acctTotal[k][si] += q[k];
                        }
                        b.append("]}");
                        symTotal[si] += qty;
                        symCount[si]++;
                        key = sym.getBytes(StandardCharsets.UTF_8);
                        val = b.toString().getBytes(StandardCharsets.UTF_8);
                        prev = val; prevKey = key;
                    }
                    if (prod != null) {
                        prod.send(new ProducerRecord<>(tp, part, key, val), (md, e) -> {
                            if (e != null) { sendErrors.incrementAndGet(); firstError.compareAndSet(null, e); }
                        });
                    }
                }
                dups.addAndGet(localDups);
            });
            threads[p].start();
        }
        for (Thread t : threads) t.join();
        if (producer != null) {
            producer.flush();
            producer.close();
        }
        if (sendErrors.get() > 0) {
            System.err.println("STOPPED: " + sendErrors.get() + " records were not written: " + firstError.get());
            System.exit(2);
        }

        // ---- manifest
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("recordCount", count);
        m.put("uniqueOrders", count - dups.get());
        m.put("duplicateRecords", dups.get());
        m.put("dupRate", dupRate);
        m.put("seed", seed);
        m.put("partitions", parts);
        m.put("fanOut", 5);
        m.put("distinctSymbols", Keys.SYMBOLS);
        m.put("distinctAccountKeys", Keys.SYMBOLS * 4);
        m.put("accountSubAccountPairs", 4);
        List<String> symKeys = new ArrayList<>();
        List<String> acctKeys = new ArrayList<>();
        Map<String, Long> totalBySymbol = new LinkedHashMap<>();
        Map<String, Long> countBySymbol = new LinkedHashMap<>();
        Map<String, Long> totalByAccount = new LinkedHashMap<>();
        for (int s = 0; s < Keys.SYMBOLS; s++) {
            String sym = Keys.sym(s);
            symKeys.add(sym);
            totalBySymbol.put(sym, symTotal[s]);
            countBySymbol.put(sym, symCount[s]);
            for (int k = 0; k < 4; k++) {
                String ak = Keys.acctKey(Keys.PAIRS[k][0], Keys.PAIRS[k][1], sym);
                acctKeys.add(ak);
                totalByAccount.put(ak, acctTotal[k][s]);
            }
        }
        m.put("symbolKeys", symKeys);
        m.put("accountKeys", acctKeys);
        m.put("totalBySymbol", totalBySymbol);
        m.put("countBySymbol", countBySymbol);
        m.put("totalByAccount", totalByAccount);
        Map<String, Long> latestPrice = new LinkedHashMap<>();
        for (Price p : prices()) {
            latestPrice.merge(p.sym, p.cents, (o, nw) -> nw);  // prices() yields ts ascending per symbol
        }
        m.put("latestPriceCents", latestPrice);
        ObjectMapper om = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        om.writeValue(new File(manifest), m);
        System.out.printf("%s: %,d records (%,d unique orders, %,d written twice on purpose) in %.1f s%n",
                produce ? "produced" : "manifest only", count, count - dups.get(), dups.get(),
                (System.currentTimeMillis() - t0) / 1000.0);
    }

    /** The price feed: PRICE_UPDATES prices per symbol with rising timestamps,
     *  from a fixed seed, so every fill and every manifest see the same prices.
     *  Returned in timestamp order per symbol. */
    static List<Price> prices() {
        SplittableRandom r = new SplittableRandom(PRICE_SEED);
        List<Price> out = new ArrayList<>();
        for (int s = 0; s < Keys.SYMBOLS; s++) {
            long base = r.nextLong(500, 500_001);
            for (int k = 0; k < PRICE_UPDATES; k++) {
                long cents = Math.max(1, base + r.nextLong(-200, 201) * k);
                out.add(new Price(Keys.sym(s), T0 + k * 1000L, cents));
            }
        }
        return out;
    }

    /** Write the price feed once: only when the topic is new or empty. Written
     *  newest first, so the job has to take the latest by timestamp, not by
     *  arrival. */
    static void writePricesIfEmpty(String bootstrap, String topic, int parts,
                                   KafkaProducer<byte[], byte[]> producer) throws Exception {
        Properties ap = new Properties();
        ap.put("bootstrap.servers", bootstrap);
        try (AdminClient admin = AdminClient.create(ap)) {
            if (!admin.listTopics().names().get().contains(topic)) {
                NewTopic nt = new NewTopic(topic, parts, (short) 1)
                        .configs(Map.of("retention.bytes", String.valueOf(1L << 30)));
                admin.createTopics(List.of(nt)).all().get();
                System.out.println("created " + topic + " with " + parts + " partitions");
            }
            Map<TopicPartition, OffsetSpec> q = new HashMap<>();
            for (int p = 0; p < parts; p++) q.put(new TopicPartition(topic, p), OffsetSpec.latest());
            long end = 0;
            for (var e : admin.listOffsets(q).all().get().values()) end += e.offset();
            if (end > 0) {
                System.out.println(topic + " already holds " + end + " price records; not written again");
                return;
            }
        }
        List<Price> ps = prices();
        Collections.reverse(ps);
        for (Price p : ps) {
            int si = Integer.parseInt(p.sym.substring(1));
            String v = "{\"sym\":\"" + p.sym + "\",\"ts\":" + p.ts + ",\"cents\":" + p.cents + "}";
            producer.send(new ProducerRecord<>(topic, si % parts, p.sym.getBytes(StandardCharsets.UTF_8),
                    v.getBytes(StandardCharsets.UTF_8)));
        }
        producer.flush();
        System.out.println("wrote " + ps.size() + " price records to " + topic);
    }
}
