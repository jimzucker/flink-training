package st53;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Deterministic order backlog, plus the price feed, plus the manifest the verifier checks against.
 *
 * Order u (0-based, unique) goes to partition u mod P with id u, so ids increase within every
 * partition -- the property the job's repeat check relies on (ASSUMPTIONS.md A10). One order in
 * 200 is written twice, back to back, in the same partition. The manifest counts each order once.
 *
 *   --mode=produce|manifest --count=N --seed=S --partitions=P --bootstrap=host:port
 *   --topic=orders --prices=prices --manifest=path
 *
 * count is records written, repeats included, which is what the harness reads back off the log.
 */
public final class GenerateBacklog {

    public static void main(String[] argv) throws Exception {
        Args a = new Args(argv);
        String mode = a.get("mode");
        long count = a.getLong("count");
        long seed = a.getLong("seed");
        int partitions = a.getInt("partitions", 8);
        String manifestPath = a.get("manifest");

        // How many unique orders make exactly `count` records once repeats are added.
        long total = 0, u = 0;
        while (total < count) {
            total += Model.isDup(seed, u) ? 2 : 1;
            u++;
        }
        final long uniques = u;
        final boolean lastRepeatDropped = total > count;   // the last order would overshoot by one
        final long repeats = count - uniques;

        boolean produce = mode.equals("produce");
        if (!produce && !mode.equals("manifest")) throw new IllegalArgumentException("mode " + mode);
        String bootstrap = produce ? a.get("bootstrap") : null;
        String topic = produce ? a.get("topic") : null;
        if (produce) {
            ensurePrices(bootstrap, a.get("prices"), partitions);
        }

        int threads = produce ? 4 : Math.min(4, Runtime.getRuntime().availableProcessors());
        if (produce && partitions % threads != 0) {
            // one producer per partition is what keeps ids increasing within it
            throw new IllegalArgumentException(partitions + " partitions do not divide among " + threads + " producers");
        }
        long[][] symPos = new long[threads][Model.SYMBOLS];
        long[][] symOrd = new long[threads][Model.SYMBOLS];
        long[][] accPos = new long[threads][Model.SYMBOLS * Model.ALLOCS];
        long[][] accOrd = new long[threads][Model.SYMBOLS * Model.ALLOCS];
        AtomicLong written = new AtomicLong();
        AtomicReference<Throwable> err = new AtomicReference<>();
        List<Thread> ts = new ArrayList<>();
        long t0 = System.currentTimeMillis();
        for (int t = 0; t < threads; t++) {
            final int me = t;
            Thread th = new Thread(() -> {
                try {
                    run(me, threads, seed, uniques, lastRepeatDropped, partitions, produce, bootstrap, topic,
                        symPos[me], symOrd[me], accPos[me], accOrd[me], written, count);
                } catch (Throwable e) {
                    err.set(e);
                }
            });
            th.start();
            ts.add(th);
        }
        for (Thread th : ts) th.join();
        if (err.get() != null) throw new RuntimeException("generator thread failed", err.get());

        long[] sp = new long[Model.SYMBOLS], so = new long[Model.SYMBOLS];
        long[] ap = new long[Model.SYMBOLS * Model.ALLOCS], ao = new long[Model.SYMBOLS * Model.ALLOCS];
        for (int t = 0; t < threads; t++) {
            for (int i = 0; i < sp.length; i++) { sp[i] += symPos[t][i]; so[i] += symOrd[t][i]; }
            for (int i = 0; i < ap.length; i++) { ap[i] += accPos[t][i]; ao[i] += accOrd[t][i]; }
        }
        writeManifest(manifestPath, count, uniques, repeats, seed, partitions, sp, so, ap, ao);
        System.out.printf("done: %,d records (%,d orders, %,d written twice on purpose) in %.1f s%s%n",
                count, uniques, repeats, (System.currentTimeMillis() - t0) / 1000.0,
                produce ? " to " + topic : " (manifest only)");
    }

    /** Thread `me` handles orders u = me, me+threads, ... -- with threads dividing P, that is a fixed set of partitions. */
    static void run(int me, int threads, long seed, long uniques, boolean lastRepeatDropped, int partitions,
                    boolean produce, String bootstrap, String topic,
                    long[] symPos, long[] symOrd, long[] accPos, long[] accOrd, AtomicLong written, long count)
            throws Exception {
        KafkaProducer<byte[], byte[]> p = null;
        if (produce) {
            Properties pr = new Properties();
            pr.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            pr.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            pr.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            pr.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
            pr.put(ProducerConfig.BATCH_SIZE_CONFIG, 1 << 20);
            pr.put(ProducerConfig.LINGER_MS_CONFIG, 50);
            pr.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 256L << 20);
            pr.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);     // keeps order within a partition
            pr.put(ProducerConfig.ACKS_CONFIG, "all");
            p = new KafkaProducer<>(pr);
        }
        int[] sym = new int[1];
        long[] alloc = new long[Model.ALLOCS];
        StringBuilder sb = new StringBuilder(256);
        AtomicReference<Exception> sendErr = new AtomicReference<>();
        long local = 0;
        for (long u = me; u < uniques; u += threads) {
            Model.order(seed, u, sym, alloc);
            String symbol = Model.symbol(sym[0]);
            long qty = 0;
            for (long q : alloc) qty += q;
            symPos[sym[0]] += qty;
            symOrd[sym[0]]++;
            for (int k = 0; k < Model.ALLOCS; k++) {
                int idx = k * Model.SYMBOLS + sym[0];
                accPos[idx] += alloc[k];
                accOrd[idx]++;
            }
            boolean twice = Model.isDup(seed, u) && !(lastRepeatDropped && u == uniques - 1);
            if (p != null) {
                sb.setLength(0);
                sb.append("{\"id\":").append(u).append(",\"symbol\":\"").append(symbol)
                  .append("\",\"qty\":").append(qty).append(",\"allocs\":[");
                for (int k = 0; k < Model.ALLOCS; k++) {
                    if (k > 0) sb.append(',');
                    sb.append("{\"account\":\"").append(Model.ACCOUNTS[k / Model.SUBS.length])
                      .append("\",\"sub\":\"").append(Model.SUBS[k % Model.SUBS.length])
                      .append("\",\"qty\":").append(alloc[k]).append('}');
                }
                sb.append("]}");
                byte[] v = sb.toString().getBytes(StandardCharsets.UTF_8);
                int part = (int) (u % partitions);
                for (int n = twice ? 2 : 1; n > 0; n--) {
                    p.send(new ProducerRecord<>(topic, part, null, v), (md, e) -> { if (e != null) sendErr.set(e); });
                }
                if (sendErr.get() != null) throw sendErr.get();
            }
            local += twice ? 2 : 1;
            if (local >= 1_000_000) {
                long w = written.addAndGet(local);
                local = 0;
                if (me == 0 && produce) System.out.printf("written about %,d of %,d%n", w, count);
            }
        }
        written.addAndGet(local);
        if (p != null) {
            p.flush();
            p.close();
            if (sendErr.get() != null) throw sendErr.get();
        }
    }

    /** The second input. Written once with a fixed seed; skipped when the topic already holds it. */
    static void ensurePrices(String bootstrap, String topic, int partitions) throws Exception {
        long want = (long) Model.SYMBOLS * Model.PRICES_PER_SYMBOL;
        Properties ap = new Properties();
        ap.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        try (Admin admin = Admin.create(ap)) {
            if (!admin.listTopics().names().get().contains(topic)) {
                NewTopic nt = new NewTopic(topic, partitions, (short) 1)
                        .configs(Map.of("retention.bytes", "1073741824"));
                admin.createTopics(Collections.singleton(nt)).all().get();
                System.out.println("created " + topic + " with " + partitions + " partitions");
            }
        }
        long have = logEnd(bootstrap, topic, partitions);
        if (have >= want) {
            System.out.printf("%s already holds %,d prices (want %,d); not written again%n", topic, have, want);
            return;
        }
        Properties pr = new Properties();
        pr.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        pr.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        pr.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        try (KafkaProducer<byte[], byte[]> p = new KafkaProducer<>(pr)) {
            for (int k = 0; k < Model.PRICES_PER_SYMBOL; k++) {
                for (int s = 0; s < Model.SYMBOLS; s++) {
                    long[] px = Model.price(s, k);
                    String sym = Model.symbol(s);
                    String v = "{\"symbol\":\"" + sym + "\",\"ts\":" + px[0] + ",\"price\":" + px[1] + "}";
                    p.send(new ProducerRecord<>(topic, sym.getBytes(StandardCharsets.UTF_8),
                            v.getBytes(StandardCharsets.UTF_8))).get();
                }
            }
        }
        long after = logEnd(bootstrap, topic, partitions);
        if (after < want) throw new IllegalStateException(topic + " holds " + after + " after writing " + want);
        System.out.printf("wrote %,d prices to %s (log end now %,d)%n", want, topic, after);
    }

    static long logEnd(String bootstrap, String topic, int partitions) {
        Properties cp = new Properties();
        cp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        cp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        cp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(cp)) {
            List<TopicPartition> tps = new ArrayList<>();
            c.partitionsFor(topic).forEach(pi -> tps.add(new TopicPartition(topic, pi.partition())));
            long sum = 0;
            for (long v : c.endOffsets(tps).values()) sum += v;
            return sum;
        }
    }

    static void writeManifest(String path, long count, long uniques, long repeats, long seed, int partitions,
                              long[] sp, long[] so, long[] ap, long[] ao) throws Exception {
        int symWith = 0, accWith = 0;
        for (long o : so) if (o > 0) symWith++;
        for (long o : ao) if (o > 0) accWith++;
        try (Writer w = new OutputStreamWriter(new FileOutputStream(path), StandardCharsets.UTF_8)) {
            w.write("{\n");
            w.write("  \"records\": " + count + ",\n");
            w.write("  \"uniqueOrders\": " + uniques + ",\n");
            w.write("  \"duplicateRecords\": " + repeats + ",\n");
            w.write("  \"seed\": " + seed + ",\n");
            w.write("  \"partitions\": " + partitions + ",\n");
            w.write("  \"outputsPerUniqueOrder\": " + (1 + Model.ALLOCS) + ",\n");
            w.write("  \"symbolKeysPredicted\": " + Model.SYMBOLS + ",\n");
            w.write("  \"accountKeysPredicted\": " + Model.SYMBOLS * Model.ALLOCS + ",\n");
            w.write("  \"symbolKeysWithOrders\": " + symWith + ",\n");
            w.write("  \"accountKeysWithOrders\": " + accWith + ",\n");
            w.write("  \"priceSeed\": " + Model.PRICE_SEED + ",\n");
            w.write("  \"pricesPerSymbol\": " + Model.PRICES_PER_SYMBOL + ",\n");
            w.write("  \"positionBySymbol\": {");
            for (int s = 0; s < Model.SYMBOLS; s++) {
                w.write((s == 0 ? "\n" : ",\n") + "    \"" + Model.symbol(s) + "\": [" + sp[s] + ", " + so[s] + "]");
            }
            w.write("\n  },\n  \"positionByAccount\": {");
            boolean first = true;
            for (int k = 0; k < Model.ALLOCS; k++) {
                for (int s = 0; s < Model.SYMBOLS; s++) {
                    int idx = k * Model.SYMBOLS + s;
                    String key = Model.accountKey(k / Model.SUBS.length, k % Model.SUBS.length, Model.symbol(s));
                    w.write((first ? "\n" : ",\n") + "    \"" + key + "\": [" + ap[idx] + ", " + ao[idx] + "]");
                    first = false;
                }
            }
            w.write("\n  },\n  \"latestPrice\": {");
            for (int s = 0; s < Model.SYMBOLS; s++) {
                long[] px = Model.latestPrice(s);
                w.write((s == 0 ? "\n" : ",\n") + "    \"" + Model.symbol(s) + "\": [" + px[0] + ", " + px[1] + "]");
            }
            w.write("\n  }\n}\n");
        }
    }
}
