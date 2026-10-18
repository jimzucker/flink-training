package io.github.jimzucker.flinktraining.metrics;

import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * What the dashboard knows about Kafka, read from the broker itself.
 *
 * <p>Every topic: records written (the sum of its partitions' end offsets) and
 * the bytes it holds on disk. Every consumer group: its committed position and
 * its lag on every topic it has committed on -- the jobs' groups on the inputs,
 * the market-value job's on the position topics, the store loader's when it
 * runs. Lag is required on every topic, and the market-value outputs have no
 * consumer of their own, so this also runs one reader over the outputs it is
 * given (group {@code dashboard-reader}), which counts their records and
 * distinct keys and commits its position, so its lag is read the same way.
 *
 * <p>Counts taken from offsets include one transaction marker per partition
 * per committed transaction, so on these exactly-once topics they read
 * slightly high. The reader counts records actually read.
 *
 * <p>The same service the scalable-flink-skill harness runs beside every
 * pipeline it measures (harness/dashboard/KafkaMetrics.java there); kept in
 * step by hand.
 *
 * <p>Environment: BOOTSTRAP_SERVERS; METRICS_PORT (9500); READ_TOPICS,
 * comma-separated; READER_GROUP; INTERVAL_MS (2000).
 */
public final class KafkaMetrics {

    /** Distinct keys are counted up to this many per topic, then reported as capped. */
    static final int KEY_CAP = 2_000_000;

    static volatile String brokerText = "";
    static final Map<String, AtomicLong> READ = new ConcurrentHashMap<>();
    static final Map<String, LongSet> KEYS = new ConcurrentHashMap<>();
    static final Map<String, AtomicLong> NO_KEY = new ConcurrentHashMap<>();
    static final AtomicLong ERRORS = new AtomicLong();

    public static void main(String[] args) throws Exception {
        String bootstrap = env("BOOTSTRAP_SERVERS", null);
        if (bootstrap == null) {
            throw new IllegalArgumentException("BOOTSTRAP_SERVERS is not set");
        }
        int port = Integer.parseInt(env("METRICS_PORT", "9500"));
        long interval = Long.parseLong(env("INTERVAL_MS", "2000"));
        List<String> readTopics = new ArrayList<>();
        for (String t : env("READ_TOPICS", "").split(",")) {
            if (!t.isBlank()) {
                readTopics.add(t.trim());
            }
        }
        String group = env("READER_GROUP", "dashboard-reader");
        Path extra = Path.of(env("EXTRA_FILE", "/nonexistent"));

        Properties ap = new Properties();
        ap.put("bootstrap.servers", bootstrap);
        Admin admin = Admin.create(ap);

        HttpServer http = HttpServer.create(new InetSocketAddress(port), 0);
        http.createContext("/metrics", ex -> {
            StringBuilder sb = new StringBuilder(brokerText);
            readerText(sb, readTopics);
            try {
                if (Files.exists(extra)) {
                    sb.append(Files.readString(extra));
                }
            } catch (Exception e) {
                System.out.println("could not read " + extra + ": " + e);
            }
            byte[] body = sb.toString().getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().add("Content-Type", "text/plain; version=0.0.4");
            ex.sendResponseHeaders(200, body.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(body);
            }
        });
        http.start();

        if (!readTopics.isEmpty()) {
            Thread reader = new Thread(() -> read(bootstrap, admin, readTopics, group), "reader");
            reader.setDaemon(true);
            reader.start();
        }
        System.out.println("{\"kafkametrics\":\"up\",\"port\":" + port + ",\"reading\":\"" + readTopics + "\"}");
        System.out.flush();
        while (true) {
            long t = System.currentTimeMillis();
            try {
                brokerText = collect(admin);
            } catch (Exception e) {
                ERRORS.incrementAndGet();
                System.out.println("reading the broker did not work: " + e);
            }
            long sleep = interval - (System.currentTimeMillis() - t);
            if (sleep > 0) {
                Thread.sleep(sleep);
            }
        }
    }

    static String env(String k, String d) {
        String v = System.getenv(k);
        return v == null || v.isBlank() ? d : v;
    }

    /** One pass over the broker: topics, their end offsets and disk, every group's lag. */
    static String collect(Admin admin) throws Exception {
        Set<String> topics = new HashSet<>();
        for (String t : admin.listTopics().names().get()) {
            if (!t.startsWith("__")) {
                topics.add(t);
            }
        }
        Map<TopicPartition, OffsetSpec> latest = new HashMap<>();
        Map<TopicPartition, OffsetSpec> earliest = new HashMap<>();
        if (!topics.isEmpty()) {
            for (TopicDescription d : admin.describeTopics(topics).allTopicNames().get().values()) {
                for (TopicPartitionInfo p : d.partitions()) {
                    TopicPartition tp = new TopicPartition(d.name(), p.partition());
                    latest.put(tp, OffsetSpec.latest());
                    earliest.put(tp, OffsetSpec.earliest());
                }
            }
        }
        Map<TopicPartition, Long> end = new HashMap<>();
        Map<TopicPartition, Long> start = new HashMap<>();
        if (!latest.isEmpty()) {
            admin.listOffsets(latest).all().get().forEach((tp, r) -> end.put(tp, r.offset()));
            admin.listOffsets(earliest).all().get().forEach((tp, r) -> start.put(tp, r.offset()));
        }

        Map<String, Long> endByTopic = new TreeMap<>();
        for (Map.Entry<TopicPartition, Long> e : end.entrySet()) {
            endByTopic.merge(e.getKey().topic(), e.getValue(), Long::sum);
        }
        Map<String, Long> bytesByTopic = new TreeMap<>();
        List<Integer> brokers = new ArrayList<>();
        for (Node n : admin.describeCluster().nodes().get()) {
            brokers.add(n.id());
        }
        for (Map<String, LogDirDescription> dirs : admin.describeLogDirs(brokers).allDescriptions().get().values()) {
            for (LogDirDescription dir : dirs.values()) {
                for (Map.Entry<TopicPartition, ReplicaInfo> r : dir.replicaInfos().entrySet()) {
                    if (!r.getKey().topic().startsWith("__")) {
                        bytesByTopic.merge(r.getKey().topic(), r.getValue().size(), Long::sum);
                    }
                }
            }
        }

        // group -> topic -> {committed, lag}
        Map<String, Map<String, long[]>> groups = new TreeMap<>();
        for (ConsumerGroupListing g : admin.listConsumerGroups().all().get()) {
            Map<TopicPartition, OffsetAndMetadata> offs;
            try {
                offs = admin.listConsumerGroupOffsets(g.groupId()).partitionsToOffsetAndMetadata().get();
            } catch (Exception e) {
                continue;                               // deleted between the list and the read
            }
            Map<String, long[]> byTopic = new TreeMap<>();
            for (Map.Entry<TopicPartition, OffsetAndMetadata> e : offs.entrySet()) {
                if (e.getValue() == null || !end.containsKey(e.getKey())) {
                    continue;
                }
                long committed = e.getValue().offset();
                long lag = Math.max(0, end.get(e.getKey()) - committed);
                long[] acc = byTopic.computeIfAbsent(e.getKey().topic(), t -> new long[2]);
                acc[0] += committed;
                acc[1] += lag;
            }
            // A partition the group has not committed on yet is all lag, from its first record.
            for (Map.Entry<String, long[]> t : byTopic.entrySet()) {
                for (TopicPartition tp : end.keySet()) {
                    if (tp.topic().equals(t.getKey()) && !offs.containsKey(tp)) {
                        t.getValue()[1] += Math.max(0, end.get(tp) - start.getOrDefault(tp, 0L));
                    }
                }
            }
            if (!byTopic.isEmpty()) {
                groups.put(g.groupId(), byTopic);
            }
        }

        StringBuilder sb = new StringBuilder();
        sb.append("# HELP kafka_topic_records_written_total Records written to the topic, the sum of its partitions' end offsets; starts again at 0 when the topic is recreated\n");
        sb.append("# TYPE kafka_topic_records_written_total counter\n");
        endByTopic.forEach((t, v) -> sb.append("kafka_topic_records_written_total{topic=\"").append(esc(t)).append("\"} ").append(v).append('\n'));
        sb.append("# HELP kafka_topic_log_bytes Bytes the topic holds on the broker's disk\n");
        sb.append("# TYPE kafka_topic_log_bytes gauge\n");
        bytesByTopic.forEach((t, v) -> sb.append("kafka_topic_log_bytes{topic=\"").append(esc(t)).append("\"} ").append(v).append('\n'));
        sb.append("# HELP kafka_group_records_committed_total The group's committed position, summed over the topic's partitions\n");
        sb.append("# TYPE kafka_group_records_committed_total counter\n");
        sb.append("# HELP kafka_group_lag_records Records written to the topic that the group has not committed past\n");
        sb.append("# TYPE kafka_group_lag_records gauge\n");
        groups.forEach((g, byTopic) -> byTopic.forEach((t, v) -> {
            String labels = "{group=\"" + esc(g) + "\",topic=\"" + esc(t) + "\"} ";
            sb.append("kafka_group_records_committed_total").append(labels).append(v[0]).append('\n');
            sb.append("kafka_group_lag_records").append(labels).append(v[1]).append('\n');
        }));
        sb.append("# HELP kafka_metrics_errors_total Passes over the broker that did not complete\n");
        sb.append("# TYPE kafka_metrics_errors_total counter\n");
        sb.append("kafka_metrics_errors_total ").append(ERRORS.get()).append('\n');
        return sb.toString();
    }

    static void readerText(StringBuilder sb, List<String> topics) {
        if (topics.isEmpty()) {
            return;
        }
        sb.append("# HELP kafka_reader_records_total Committed records the dashboard's reader has read from the topic\n");
        sb.append("# TYPE kafka_reader_records_total counter\n");
        for (String t : topics) {
            sb.append("kafka_reader_records_total{topic=\"").append(esc(t)).append("\"} ")
              .append(READ.computeIfAbsent(t, k -> new AtomicLong()).get()).append('\n');
        }
        sb.append("# HELP kafka_reader_unique_keys Distinct keys the reader has seen on the topic since it was created\n");
        sb.append("# TYPE kafka_reader_unique_keys gauge\n");
        for (String t : topics) {
            LongSet s = KEYS.computeIfAbsent(t, k -> new LongSet());
            sb.append("kafka_reader_unique_keys{topic=\"").append(esc(t)).append("\"} ").append(s.size()).append('\n');
        }
        sb.append("# HELP kafka_reader_unique_keys_capped 1 when the topic has more distinct keys than the reader counts\n");
        sb.append("# TYPE kafka_reader_unique_keys_capped gauge\n");
        for (String t : topics) {
            LongSet s = KEYS.computeIfAbsent(t, k -> new LongSet());
            sb.append("kafka_reader_unique_keys_capped{topic=\"").append(esc(t)).append("\"} ")
              .append(s.capped ? 1 : 0).append('\n');
        }
        sb.append("# HELP kafka_reader_records_without_key_total Records read with no key\n");
        sb.append("# TYPE kafka_reader_records_without_key_total counter\n");
        for (String t : topics) {
            sb.append("kafka_reader_records_without_key_total{topic=\"").append(esc(t)).append("\"} ")
              .append(NO_KEY.computeIfAbsent(t, k -> new AtomicLong()).get()).append('\n');
        }
    }

    /**
     * Reads the outputs to their end, committed records only, and commits its
     * position every two seconds so its lag is on the dashboard like any other
     * group's. The harness deletes and recreates the outputs between cases;
     * the topic id tells a new topic from the old one, and a new one is read
     * from its first record with its key count started again.
     */
    static void read(String bootstrap, Admin admin, List<String> topics, String group) {
        Properties cp = new Properties();
        cp.put("bootstrap.servers", bootstrap);
        cp.put("group.id", group);
        cp.put("enable.auto.commit", "false");
        cp.put("auto.offset.reset", "earliest");
        cp.put("isolation.level", "read_committed");
        cp.put("allow.auto.create.topics", "false");
        cp.put("check.crcs", "false");
        cp.put("fetch.max.bytes", Integer.toString(64 << 20));
        cp.put("max.partition.fetch.bytes", Integer.toString(8 << 20));
        cp.put("max.poll.records", "20000");
        cp.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        cp.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        Map<String, String> ids = new HashMap<>();
        List<TopicPartition> assigned = new ArrayList<>();
        long lastCheck = 0;
        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(cp)) {
            while (true) {
                try {
                    if (System.currentTimeMillis() - lastCheck > 2000) {
                        lastCheck = System.currentTimeMillis();
                        commit(c, assigned);
                        Set<String> exist = admin.listTopics().names().get();
                        List<String> present = new ArrayList<>();
                        for (String t : topics) {
                            if (exist.contains(t)) {
                                present.add(t);
                            }
                        }
                        Map<String, TopicDescription> desc = present.isEmpty() ? Map.of()
                                : admin.describeTopics(present).allTopicNames().get();
                        List<TopicPartition> want = new ArrayList<>();
                        List<TopicPartition> fresh = new ArrayList<>();
                        for (String t : topics) {
                            TopicDescription d = desc.get(t);
                            String id = d == null ? null : d.topicId().toString();
                            if (!java.util.Objects.equals(id, ids.get(t))) {
                                KEYS.put(t, new LongSet());
                                if (id == null) {
                                    ids.remove(t);
                                } else {
                                    ids.put(t, id);
                                }
                            }
                            if (d == null) {
                                continue;
                            }
                            for (TopicPartitionInfo p : d.partitions()) {
                                TopicPartition tp = new TopicPartition(t, p.partition());
                                want.add(tp);
                                if (!assigned.contains(tp) || !id.equals(idAtAssign.get(tp))) {
                                    fresh.add(tp);
                                    idAtAssign.put(tp, id);
                                }
                            }
                        }
                        if (!want.equals(assigned) || !fresh.isEmpty()) {
                            c.assign(want);
                            assigned = want;
                            if (!fresh.isEmpty()) {
                                c.seekToBeginning(fresh);
                            }
                        }
                    }
                    if (assigned.isEmpty()) {
                        Thread.sleep(500);
                        continue;
                    }
                    for (ConsumerRecord<byte[], byte[]> r : c.poll(Duration.ofMillis(500))) {
                        READ.computeIfAbsent(r.topic(), k -> new AtomicLong()).incrementAndGet();
                        if (r.key() == null) {
                            NO_KEY.computeIfAbsent(r.topic(), k -> new AtomicLong()).incrementAndGet();
                        } else {
                            KEYS.computeIfAbsent(r.topic(), k -> new LongSet()).add(hash(r.key()));
                        }
                    }
                } catch (Exception e) {
                    ERRORS.incrementAndGet();
                    System.out.println("the reader did not get a batch: " + e);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        return;
                    }
                }
            }
        }
    }

    static final Map<TopicPartition, String> idAtAssign = new HashMap<>();

    static void commit(KafkaConsumer<byte[], byte[]> c, List<TopicPartition> assigned) {
        if (assigned.isEmpty()) {
            return;
        }
        Map<TopicPartition, OffsetAndMetadata> pos = new HashMap<>();
        for (TopicPartition tp : assigned) {
            try {
                pos.put(tp, new OffsetAndMetadata(c.position(tp, Duration.ofSeconds(2))));
            } catch (Exception e) {
                // a partition whose topic was just deleted has no position; leave it out
            }
        }
        if (!pos.isEmpty()) {
            try {
                c.commitSync(pos, Duration.ofSeconds(5));
            } catch (Exception e) {
                System.out.println("the reader's commit did not go through: " + e);
            }
        }
    }

    /** 64-bit FNV-1a: two keys share a hash about once in 10^7 at the cap. */
    static long hash(byte[] b) {
        long h = 0xcbf29ce484222325L;
        for (byte x : b) {
            h ^= (x & 0xff);
            h *= 0x100000001b3L;
        }
        return h == 0 ? 1 : h;
    }

    static String esc(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    }

    /** An open-addressing set of longs: 16 bytes a key at the cap, not the 60 a HashSet costs. */
    static final class LongSet {
        long[] slots = new long[1 << 12];
        int size;
        volatile boolean capped;

        synchronized void add(long v) {
            if (size >= KEY_CAP) {
                if (!contains(v)) {
                    capped = true;
                }
                return;
            }
            if (size * 2 >= slots.length) {
                long[] old = slots;
                slots = new long[old.length * 2];
                size = 0;
                for (long o : old) {
                    if (o != 0) {
                        insert(o);
                    }
                }
            }
            insert(v);
        }

        private void insert(long v) {
            int m = slots.length - 1;
            int i = (int) (v ^ (v >>> 32)) & m;
            while (slots[i] != 0) {
                if (slots[i] == v) {
                    return;
                }
                i = (i + 1) & m;
            }
            slots[i] = v;
            size++;
        }

        private boolean contains(long v) {
            int m = slots.length - 1;
            int i = (int) (v ^ (v >>> 32)) & m;
            while (slots[i] != 0) {
                if (slots[i] == v) {
                    return true;
                }
                i = (i + 1) & m;
            }
            return false;
        }

        synchronized int size() {
            return size;
        }
    }

    private KafkaMetrics() {
    }
}
