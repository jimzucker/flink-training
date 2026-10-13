package st54;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Deterministic order backlog, and the prices the job joins to it.
 *
 *   --mode=produce   writes the prices topic (only if it holds fewer than it should), then
 *                    {count} orders over {partitions} partitions, and the manifest
 *   --mode=manifest  the same manifest, producing nothing
 *
 * Each partition is generated on its own from (seed, partition), so the records and the
 * manifest do not depend on thread timing. Order ids rise within a partition. A small share
 * of orders is written twice in a row, same partition, same id, as a retrying producer would;
 * tradeCount counts every record on the topic, every total counts each order once.
 */
public final class Generate {

    static final int REPEAT_PER_MILLE = 2;   // 0.2%

    /** what one partition contributes to the manifest */
    static final class Part {
        long records;
        long unique;
        long repeats;
        final long[] symTotal = new long[Model.SYMBOLS];
        final long[] symCount = new long[Model.SYMBOLS];
        final long[][] acctTotal = new long[Model.ALLOCS][Model.SYMBOLS];
    }

    interface Sink {
        void send(int partition, byte[] value) throws Exception;
    }

    static Part generate(long seed, int partition, long quota, Sink sink) throws Exception {
        Part pt = new Part();
        SplittableRandom r = new SplittableRandom(seed * 1_000_003L + partition);
        long seq = 0;
        StringBuilder sb = new StringBuilder(256);
        long[] alloc = new long[Model.ALLOCS];
        while (pt.records < quota) {
            long id = ((long) partition << Model.PARTITION_SHIFT) | seq++;
            int s = r.nextInt(Model.SYMBOLS);
            boolean sell = r.nextBoolean();
            long qty = 0;
            for (int i = 0; i < Model.ALLOCS; i++) {
                alloc[i] = 1 + r.nextInt(1000);
                qty += alloc[i];
            }
            sb.setLength(0);
            sb.append("{\"id\":").append(id).append(",\"symbol\":\"").append(Model.symbol(s))
              .append("\",\"side\":\"").append(sell ? "SELL" : "BUY").append("\",\"quantity\":").append(qty)
              .append(",\"allocations\":[");
            int i = 0;
            for (String acc : Model.ACCOUNTS) {
                for (String sub : Model.SUBS) {
                    if (i > 0) {
                        sb.append(',');
                    }
                    sb.append("{\"account\":\"").append(acc).append("\",\"subAccount\":\"").append(sub)
                      .append("\",\"quantity\":").append(alloc[i]).append('}');
                    i++;
                }
            }
            sb.append("]}");
            byte[] v = sb.toString().getBytes(StandardCharsets.UTF_8);
            long sign = sell ? -1 : 1;
            pt.unique++;
            pt.symTotal[s] += sign * qty;
            pt.symCount[s]++;
            for (int k = 0; k < Model.ALLOCS; k++) {
                pt.acctTotal[k][s] += sign * alloc[k];
            }
            if (sink != null) {
                sink.send(partition, v);
            }
            pt.records++;
            // the repeat is drawn every time, so the stream of draws does not depend on the quota
            boolean repeat = r.nextInt(1000) < REPEAT_PER_MILLE;
            if (repeat && pt.records < quota) {
                if (sink != null) {
                    sink.send(partition, v);
                }
                pt.records++;
                pt.repeats++;
            }
        }
        return pt;
    }

    public static void main(String[] argv) throws Exception {
        Args a = new Args(argv);
        String mode = a.get("mode");
        long count = a.getLong("count");
        long seed = a.getLong("seed");
        int partitions = Integer.parseInt(a.get("partitions"));
        String manifestPath = a.get("manifest");
        boolean produce = mode.equals("produce");

        KafkaProducer<byte[], byte[]> producer = null;
        String topic = null;
        if (produce) {
            String bootstrap = a.get("bootstrap");
            topic = a.get("topic");
            writePrices(bootstrap, a.get("prices"), partitions);
            Properties p = new Properties();
            p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
            p.put(ProducerConfig.LINGER_MS_CONFIG, "20");
            p.put(ProducerConfig.BATCH_SIZE_CONFIG, "524288");
            p.put(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(256L << 20));
            p.put(ProducerConfig.ACKS_CONFIG, "all");
            p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
            producer = new KafkaProducer<>(p);
        }

        final KafkaProducer<byte[], byte[]> prod = producer;
        final String t = topic;
        final AtomicReference<Exception> failed = new AtomicReference<>();
        final AtomicLong sent = new AtomicLong();
        Part[] parts = new Part[partitions];
        List<Thread> threads = new ArrayList<>();
        long started = System.currentTimeMillis();
        for (int pi = 0; pi < partitions; pi++) {
            final int part = pi;
            final long quota = count / partitions + (part < count % partitions ? 1 : 0);
            Thread th = new Thread(() -> {
                try {
                    Sink sink = prod == null ? null : (partition, v) -> {
                        if (failed.get() != null) {
                            throw failed.get();
                        }
                        prod.send(new ProducerRecord<>(t, partition, null, v), (md, e) -> {
                            if (e != null) {
                                failed.compareAndSet(null, e);
                            }
                        });
                        long n = sent.incrementAndGet();
                        if (n % 10_000_000 == 0) {
                            long secs = Math.max(1, (System.currentTimeMillis() - started) / 1000);
                            System.out.printf("sent %,d of %,d (%,d a second)%n", n, count, n / secs);
                        }
                    };
                    parts[part] = generate(seed, part, quota, sink);
                } catch (Exception e) {
                    failed.compareAndSet(null, e);
                }
            });
            th.start();
            threads.add(th);
        }
        for (Thread th : threads) {
            th.join();
        }
        if (producer != null) {
            producer.flush();
            producer.close();
        }
        if (failed.get() != null) {
            System.err.println("generator stopped: " + failed.get());
            failed.get().printStackTrace();
            System.exit(1);
        }
        writeManifest(manifestPath, count, seed, partitions, parts);
        long secs = Math.max(1, (System.currentTimeMillis() - started) / 1000);
        System.out.printf("done: %,d records (%s) in %d s; manifest %s%n", count,
                produce ? "written to " + topic : "manifest only", secs, manifestPath);
    }

    static void writeManifest(String path, long count, long seed, int partitions, Part[] parts) throws Exception {
        long records = 0, unique = 0, repeats = 0, grand = 0;
        long[] symTotal = new long[Model.SYMBOLS];
        long[] symCount = new long[Model.SYMBOLS];
        long[][] acctTotal = new long[Model.ALLOCS][Model.SYMBOLS];
        for (Part p : parts) {
            records += p.records;
            unique += p.unique;
            repeats += p.repeats;
            for (int s = 0; s < Model.SYMBOLS; s++) {
                symTotal[s] += p.symTotal[s];
                symCount[s] += p.symCount[s];
                for (int k = 0; k < Model.ALLOCS; k++) {
                    acctTotal[k][s] += p.acctTotal[k][s];
                }
            }
        }
        if (records != count) {
            throw new IllegalStateException("generated " + records + " records, asked for " + count);
        }
        Map<String, Long> bySym = new LinkedHashMap<>();
        Map<String, Long> bySymN = new LinkedHashMap<>();
        Map<String, Long> byAcct = new LinkedHashMap<>();
        Map<String, Long> byAcctN = new LinkedHashMap<>();
        Map<String, Long> latest = new LinkedHashMap<>();
        for (int s = 0; s < Model.SYMBOLS; s++) {
            if (symCount[s] == 0) {
                continue;
            }
            String sym = Model.symbol(s);
            bySym.put(sym, symTotal[s]);
            bySymN.put(sym, symCount[s]);
            grand += symTotal[s];
            latest.put(sym, Model.priceCents(s, Model.PRICES_PER_SYMBOL - 1));
        }
        int k = 0;
        for (String acc : Model.ACCOUNTS) {
            for (String sub : Model.SUBS) {
                for (int s = 0; s < Model.SYMBOLS; s++) {
                    if (symCount[s] == 0) {
                        continue;
                    }
                    String key = Model.accountKey(acc, sub, Model.symbol(s));
                    byAcct.put(key, acctTotal[k][s]);
                    byAcctN.put(key, symCount[s]);
                }
                k++;
            }
        }
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("tradeCount", records);
        m.put("uniqueOrders", unique);
        m.put("duplicateRecords", repeats);
        m.put("seed", seed);
        m.put("partitions", partitions);
        m.put("allocationsPerOrder", Model.ALLOCS);
        m.put("outputsPerUniqueOrder", 1 + Model.ALLOCS);
        m.put("symbolKeys", bySym.size());
        m.put("accountKeys", byAcct.size());
        m.put("grandTotal", grand);
        m.put("pricesPerSymbol", Model.PRICES_PER_SYMBOL);
        m.put("totalBySymbol", bySym);
        m.put("countBySymbol", bySymN);
        m.put("totalByAccount", byAcct);
        m.put("countByAccount", byAcctN);
        m.put("latestPriceCentsBySymbol", latest);
        new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).writeValue(new File(path), m);
    }

    /** 3 prices per symbol, rising timestamps, the same every time. Written only if the topic is short. */
    static void writePrices(String bootstrap, String topic, int partitions) throws Exception {
        long want = (long) Model.SYMBOLS * Model.PRICES_PER_SYMBOL;
        Properties ap = new Properties();
        ap.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        try (Admin admin = Admin.create(ap)) {
            if (!admin.listTopics().names().get().contains(topic)) {
                Map<String, String> cfg = new HashMap<>();
                cfg.put("retention.bytes", String.valueOf(64L << 20));
                cfg.put("retention.ms", "-1");
                admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1).configs(cfg))).all().get();
                System.out.println("created topic " + topic);
            }
            int n = admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic).partitions().size();
            Map<TopicPartition, OffsetSpec> q = new HashMap<>();
            for (int i = 0; i < n; i++) {
                q.put(new TopicPartition(topic, i), OffsetSpec.latest());
            }
            long have = 0;
            for (ListOffsetsResult.ListOffsetsResultInfo i : admin.listOffsets(q).all().get().values()) {
                have += i.offset();
            }
            if (have >= want) {
                System.out.printf("prices: %s already holds %,d records; nothing written%n", topic, have);
                return;
            }
        }
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        List<java.util.concurrent.Future<?>> acks = new ArrayList<>();
        try (KafkaProducer<byte[], byte[]> prod = new KafkaProducer<>(p)) {
            for (int k = 0; k < Model.PRICES_PER_SYMBOL; k++) {
                for (int s = 0; s < Model.SYMBOLS; s++) {
                    String sym = Model.symbol(s);
                    String v = "{\"symbol\":\"" + sym + "\",\"ts\":" + Model.priceTs(k) + ",\"priceCents\":"
                            + Model.priceCents(s, k) + "}";
                    acks.add(prod.send(new ProducerRecord<>(topic, sym.getBytes(StandardCharsets.UTF_8),
                            v.getBytes(StandardCharsets.UTF_8))));
                }
            }
            prod.flush();
            for (java.util.concurrent.Future<?> f : acks) {
                f.get();   // throws if the broker did not take it
            }
        }
        System.out.printf("prices: wrote %,d records to %s%n", want, topic);
    }
}
