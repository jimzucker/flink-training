package st56;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Deterministic order generator.
 *
 * --mode=produce writes {count} records to {topic} and the manifest;
 * --mode=manifest writes only the manifest (same content, nothing sent).
 *
 * Slot i lives in partition i mod P at local position i / P. A slot is either
 * an original order (id = i) or, for about 0.5% of slots, a repeat of the
 * original just before it in the same partition, the way a retrying producer
 * would write it twice. {count} counts every record written, repeats included,
 * because the harness checks the topic's log end against it; the manifest's
 * totals count each order once.
 *
 * Also, on produce: creates the prices topic and the two market-value topics if
 * they do not exist (8 partitions, retention.bytes 1 GiB), and fills prices
 * once -- if the prices topic already holds every price it writes nothing, so
 * repeated fills do not pile prices up.
 */
public final class GenerateBacklog {

    static final long RETENTION = 1L << 30;

    public static void main(String[] argv) throws Exception {
        Map<String, String> a = Args.parse(argv);
        String mode = a.getOrDefault("mode", "manifest");
        long count = Long.parseLong(a.get("count"));
        long seed = Long.parseLong(a.get("seed"));
        int partitions = Integer.parseInt(a.getOrDefault("partitions", "8"));
        String manifestPath = a.get("manifest");
        if (Model.SYMBOLS % partitions != 0) throw new IllegalArgumentException("4096 symbols must divide by partitions");
        boolean produce = mode.equals("produce");
        String bootstrap = a.get("bootstrap");
        String topic = a.get("topic");

        if (produce) {
            String prices = a.getOrDefault("prices", "prices");
            List<String> mvTopics = Arrays.asList(a.getOrDefault("mvTopics", "").split(","));
            ensureTopicsAndPrices(bootstrap, prices, mvTopics, partitions);
        }

        long t0 = System.currentTimeMillis();
        Part[] parts = new Part[partitions];
        Thread[] threads = new Thread[partitions];
        AtomicReference<Throwable> failure = new AtomicReference<>();
        for (int p = 0; p < partitions; p++) {
            final int pp = p;
            parts[p] = new Part();
            threads[p] = new Thread(() -> {
                try {
                    runPartition(pp, partitions, count, seed, produce ? bootstrap : null, topic, parts[pp]);
                } catch (Throwable e) {
                    failure.compareAndSet(null, e);
                }
            }, "gen-p" + p);
            threads[p].start();
        }
        for (Thread t : threads) t.join();
        if (failure.get() != null) {
            failure.get().printStackTrace();
            System.exit(1);
        }

        // merge
        long written = 0, dups = 0;
        long[] posSym = new long[Model.SYMBOLS], nSym = new long[Model.SYMBOLS];
        long[][] posAcc = new long[Model.ALLOCATIONS][Model.SYMBOLS];
        for (Part pt : parts) {
            written += pt.written;
            dups += pt.dups;
            for (int s = 0; s < Model.SYMBOLS; s++) {
                posSym[s] += pt.posSym[s];
                nSym[s] += pt.nSym[s];
                for (int k = 0; k < Model.ALLOCATIONS; k++) posAcc[k][s] += pt.posAcc[k][s];
            }
        }
        if (written != count) throw new IllegalStateException("wrote " + written + " of " + count);

        StringBuilder m = new StringBuilder(4 << 20);
        m.append("{\n \"recordCount\": ").append(count)
         .append(",\n \"duplicateRecords\": ").append(dups)
         .append(",\n \"uniqueOrders\": ").append(count - dups)
         .append(",\n \"seed\": ").append(seed)
         .append(",\n \"partitions\": ").append(partitions)
         .append(",\n \"allocationsPerOrder\": ").append(Model.ALLOCATIONS)
         .append(",\n \"symbolKeyCount\": ").append(countNonZero(nSym))
         .append(",\n \"accountKeyCount\": ").append(countNonZero(nSym) * Model.ALLOCATIONS)
         .append(",\n \"priceRecords\": ").append(Model.SYMBOLS * Model.PRICE_TIMESTAMPS);
        // per-key tables: symbol level
        StringBuilder ps = new StringBuilder(), ns = new StringBuilder(), lp = new StringBuilder();
        StringBuilder pa = new StringBuilder(), na = new StringBuilder();
        for (int s = 0; s < Model.SYMBOLS; s++) {
            if (nSym[s] == 0) continue;
            String sym = Model.symbol(s);
            sep(ps).append('"').append(sym).append("\":").append(posSym[s]);
            sep(ns).append('"').append(sym).append("\":").append(nSym[s]);
            sep(lp).append('"').append(sym).append("\":").append(Model.latestPriceCents(s));
            int k = 0;
            for (String acc : Model.ACCOUNTS) {
                for (String sub : Model.SUBS) {
                    String key = Model.accountKey(acc, sub, sym);
                    sep(pa).append('"').append(key).append("\":").append(posAcc[k][s]);
                    sep(na).append('"').append(key).append("\":").append(nSym[s]);
                    k++;
                }
            }
        }
        m.append(",\n \"positionBySymbol\": {").append(ps).append('}')
         .append(",\n \"ordersBySymbol\": {").append(ns).append('}')
         .append(",\n \"latestPriceCents\": {").append(lp).append('}')
         .append(",\n \"positionByAccount\": {").append(pa).append('}')
         .append(",\n \"ordersByAccount\": {").append(na).append('}')
         .append("\n}\n");
        try (FileWriter w = new FileWriter(manifestPath, StandardCharsets.UTF_8)) {
            w.write(m.toString());
        }
        long ms = System.currentTimeMillis() - t0;
        System.out.printf("%s: %,d records (%,d orders, %,d repeats) in %.1f s, %,.0f records/s; manifest %s%n",
                produce ? "produced" : "manifest only", count, count - dups, dups, ms / 1000.0,
                count * 1000.0 / Math.max(1, ms), manifestPath);
    }

    static int countNonZero(long[] n) {
        int c = 0;
        for (long v : n) if (v != 0) c++;
        return c;
    }

    static StringBuilder sep(StringBuilder b) {
        if (b.length() > 0) b.append(',');
        return b;
    }

    static final class Part {
        long written, dups;
        final long[] posSym = new long[Model.SYMBOLS], nSym = new long[Model.SYMBOLS];
        final long[][] posAcc = new long[Model.ALLOCATIONS][Model.SYMBOLS];
    }

    static void runPartition(int p, int partitions, long count, long seed, String bootstrap, String topic, Part out)
            throws Exception {
        KafkaProducer<byte[], byte[]> prod = bootstrap == null ? null : producer(bootstrap);
        AtomicReference<Exception> sendErr = new AtomicReference<>();
        Model.Order o = new Model.Order();
        byte[] lastKey = null, lastVal = null;
        try {
            for (long i = p, j = 0; i < count; i += partitions, j++) {
                boolean repeat = Model.isRepeatSlot(seed, p, j) && !Model.isRepeatSlot(seed, p, j - 1) && lastVal != null;
                byte[] key, val;
                if (repeat) {
                    key = lastKey;
                    val = lastVal;
                    out.dups++;
                } else {
                    Model.order(seed, i, p, partitions, o);
                    out.posSym[o.symbolIdx] += o.qty;
                    out.nSym[o.symbolIdx]++;
                    for (int k = 0; k < Model.ALLOCATIONS; k++) out.posAcc[k][o.symbolIdx] += o.alloc[k];
                    key = Model.symbol(o.symbolIdx).getBytes(StandardCharsets.UTF_8);
                    val = o.json().getBytes(StandardCharsets.UTF_8);
                    lastKey = key;
                    lastVal = val;
                }
                out.written++;
                if (prod != null) {
                    prod.send(new ProducerRecord<>(topic, p, key, val), (md, e) -> {
                        if (e != null) sendErr.compareAndSet(null, e);
                    });
                    if (sendErr.get() != null) throw sendErr.get();
                }
            }
            if (prod != null) prod.flush();
            if (sendErr.get() != null) throw sendErr.get();
        } finally {
            if (prod != null) prod.close();
        }
    }

    static KafkaProducer<byte[], byte[]> producer(String bootstrap) {
        Properties pr = new Properties();
        pr.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        pr.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        pr.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        pr.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        pr.put(ProducerConfig.LINGER_MS_CONFIG, "20");
        pr.put(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(512 * 1024));
        pr.put(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(64L << 20));
        pr.put(ProducerConfig.ACKS_CONFIG, "all");
        pr.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        return new KafkaProducer<>(pr);
    }

    static void ensureTopicsAndPrices(String bootstrap, String prices, List<String> mvTopics, int partitions)
            throws Exception {
        Properties ap = new Properties();
        ap.put("bootstrap.servers", bootstrap);
        try (Admin admin = Admin.create(ap)) {
            Set<String> have = admin.listTopics().names().get();
            List<NewTopic> create = new ArrayList<>();
            List<String> all = new ArrayList<>();
            all.add(prices);
            for (String t : mvTopics) if (!t.isBlank()) all.add(t.trim());
            for (String t : all) {
                if (!have.contains(t)) {
                    create.add(new NewTopic(t, partitions, (short) 1)
                            .configs(Map.of("retention.bytes", String.valueOf(RETENTION))));
                }
            }
            if (!create.isEmpty()) {
                admin.createTopics(create).all().get();
                System.out.println("created topics: " + create.stream().map(NewTopic::name).toList());
            }
        }
        long expected = (long) Model.SYMBOLS * Model.PRICE_TIMESTAMPS;
        long have = endOffsets(bootstrap, prices, partitions);
        if (have == expected) {
            System.out.printf("prices: %s already holds all %,d prices, nothing written%n", prices, expected);
            return;
        }
        if (have != 0) {
            throw new IllegalStateException("prices topic " + prices + " holds " + have + " records, expected 0 or "
                    + expected + "; delete it and fill again");
        }
        // every (symbol, timestamp) once, in a fixed shuffled order
        List<int[]> rows = new ArrayList<>();
        for (int s = 0; s < Model.SYMBOLS; s++)
            for (int k = 0; k < Model.PRICE_TIMESTAMPS; k++) rows.add(new int[]{s, k});
        Collections.shuffle(rows, new Random(Model.PRICE_SEED));
        // asynchronous sends, one flush: a get() per send waited out linger.ms
        // 16,384 times (5.5 minutes on the first fill of run 56)
        AtomicReference<Exception> err = new AtomicReference<>();
        try (KafkaProducer<byte[], byte[]> prod = producer(bootstrap)) {
            for (int[] r : rows) {
                String sym = Model.symbol(r[0]);
                String v = "{\"symbol\":\"" + sym + "\",\"ts\":" + Model.priceTs(r[0], r[1])
                        + ",\"priceCents\":" + Model.priceCents(r[0], r[1]) + "}";
                prod.send(new ProducerRecord<>(prices, Model.partitionOf(r[0], partitions),
                        sym.getBytes(StandardCharsets.UTF_8), v.getBytes(StandardCharsets.UTF_8)),
                        (md, e) -> { if (e != null) err.compareAndSet(null, e); });
            }
            prod.flush();
        }
        if (err.get() != null) throw err.get();
        long after = endOffsets(bootstrap, prices, partitions);
        if (after != expected) throw new IllegalStateException("prices topic holds " + after + " after writing, expected " + expected);
        System.out.printf("prices: wrote %,d prices to %s (read back %,d)%n", expected, prices, after);
    }

    static long endOffsets(String bootstrap, String topic, int partitions) {
        Properties cp = new Properties();
        cp.put("bootstrap.servers", bootstrap);
        cp.put("key.deserializer", ByteArrayDeserializer.class.getName());
        cp.put("value.deserializer", ByteArrayDeserializer.class.getName());
        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(cp)) {
            List<TopicPartition> tps = new ArrayList<>();
            for (int p = 0; p < partitions; p++) tps.add(new TopicPartition(topic, p));
            long tot = 0;
            for (long v : c.endOffsets(tps).values()) tot += v;
            return tot;
        }
    }
}
