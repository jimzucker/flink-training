package io.github.jimzucker.flinktraining.store;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Proves Redis holds exactly the latest committed value of every key, and
 * nothing else.
 *
 * <p>Waits for the output topics to stop growing, reads each one to its end
 * under {@code read_committed}, and takes the last record per key -- what the
 * store should hold. Then waits for the loader to catch up and compares every
 * key and every field. No tolerances: one missing key, one extra key, or one
 * field that differs, and it exits non-zero naming it.
 *
 * <p>Usage: {@code VerifyStore <bootstrap> <redisHost> <redisPort> [quietSeconds]}
 */
public final class VerifyStore {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("usage: VerifyStore <bootstrap> <redisHost> <redisPort> [quietSeconds]");
            System.exit(2);
        }
        String bootstrap = args[0];
        String redisHost = args[1];
        int redisPort = Integer.parseInt(args[2]);
        int quietSeconds = args.length > 3 ? Integer.parseInt(args[3]) : 15;
        Map<String, String> topics = StoreConfig.parseTopics(
                System.getenv().getOrDefault("STORE_TOPICS", StoreConfig.DEFAULT_TOPICS));

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 20000);

        int failures = 0;
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
             Jedis jedis = new Jedis(redisHost, redisPort);
             TopicIds ids = new TopicIds(bootstrap, topics.keySet())) {
            List<TopicPartition> partitions = new ArrayList<>();
            for (String topic : topics.keySet()) {
                for (PartitionInfo p : consumer.partitionsFor(topic)) {
                    partitions.add(new TopicPartition(topic, p.partition()));
                }
            }
            Map<TopicPartition, Long> end = waitUntilQuiet(consumer, partitions, quietSeconds);
            ids.refresh();

            Map<String, LatestPerKey> expected = readToEnd(consumer, partitions, end);
            for (Map.Entry<String, String> t : topics.entrySet()) {
                String topic = t.getKey();
                String prefix = t.getValue();
                Map<String, Map<String, String>> want = wanted(expected.get(topic), prefix, ids.get(topic));
                Map<String, Map<String, String>> have = waitForLoader(jedis, prefix, want, Duration.ofSeconds(120));

                List<String> problems = new ArrayList<>();
                for (Map.Entry<String, Map<String, String>> w : want.entrySet()) {
                    Map<String, String> h = have.get(w.getKey());
                    if (h == null) {
                        problems.add("missing " + w.getKey());
                    } else if (!h.equals(w.getValue())) {
                        problems.add("differs " + w.getKey() + ": want " + w.getValue() + ", have " + h);
                    }
                }
                for (String key : have.keySet()) {
                    if (!want.containsKey(key)) {
                        problems.add("extra   " + key + " (no committed record has this key)");
                    }
                }
                long records = expected.containsKey(topic) ? expected.get(topic).seen() : 0;
                System.out.printf("%-22s %,12d committed records read  %,7d keys expected  %,7d in Redis  %s%n",
                        topic, records, want.size(), have.size(),
                        problems.isEmpty() ? "match" : problems.size() + " PROBLEMS");
                problems.stream().limit(10).forEach(p -> System.out.println("    " + p));
                failures += problems.size();
            }
        }
        if (failures > 0) {
            System.out.println("STOPPED: Redis does not hold the latest committed value of every key ("
                    + failures + " problems above)");
            System.exit(1);
        }
        System.out.println("Redis holds the latest committed value of every key, and no other keys");
    }

    /** End offsets, once they have not moved for {@code quietSeconds}. */
    private static Map<TopicPartition, Long> waitUntilQuiet(
            KafkaConsumer<byte[], byte[]> consumer, List<TopicPartition> partitions, int quietSeconds)
            throws InterruptedException {
        Map<TopicPartition, Long> before = consumer.endOffsets(partitions);
        for (int attempt = 0; attempt < 8; attempt++) {
            Thread.sleep(quietSeconds * 1000L);
            Map<TopicPartition, Long> after = consumer.endOffsets(partitions);
            if (after.equals(before)) {
                return after;
            }
            System.out.println("the output topics are still growing; waiting " + quietSeconds + "s more");
            before = after;
        }
        System.out.println("STOPPED: the output topics kept growing for " + 8 * quietSeconds
                + "s. Stop the generators first: docker stop ft-generators");
        System.exit(3);
        return before;
    }

    private static Map<String, LatestPerKey> readToEnd(
            KafkaConsumer<byte[], byte[]> consumer, List<TopicPartition> partitions,
            Map<TopicPartition, Long> end) {
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);
        Map<String, LatestPerKey> byTopic = new HashMap<>();
        Set<TopicPartition> open = new HashSet<>();
        for (TopicPartition tp : partitions) {
            if (end.get(tp) > 0) {
                open.add(tp);
            }
        }
        while (!open.isEmpty()) {
            for (ConsumerRecord<byte[], byte[]> r : consumer.poll(Duration.ofMillis(500))) {
                TopicPartition tp = new TopicPartition(r.topic(), r.partition());
                if (r.offset() < end.get(tp)) {
                    byTopic.computeIfAbsent(r.topic(), t -> new LatestPerKey()).add(r);
                }
            }
            open.removeIf(tp -> consumer.position(tp) >= end.get(tp));
        }
        return byTopic;
    }

    /** Each Redis key and the hash it should hold, built the way the loader builds it. */
    private static Map<String, Map<String, String>> wanted(LatestPerKey latest, String prefix, String topicId) {
        Map<String, Map<String, String>> want = new LinkedHashMap<>();
        if (latest == null) {
            return want;
        }
        for (LatestPerKey.Latest l : latest.latest()) {
            List<String> pairs = new ArrayList<>();
            RedisStore.fields(l.value(), pairs);
            Map<String, String> hash = new HashMap<>();
            for (int i = 0; i < pairs.size(); i += 2) {
                hash.put(pairs.get(i), pairs.get(i + 1));
            }
            hash.put(RedisStore.TOPIC_ID, topicId);
            hash.put(RedisStore.OFFSET, Long.toString(l.offset()));
            hash.put(RedisStore.PARTITION, Integer.toString(l.partition()));
            want.put(prefix + ":" + l.key(), hash);
        }
        return want;
    }

    /** Everything under the prefix, once every expected key is at its expected offset. */
    private static Map<String, Map<String, String>> waitForLoader(
            Jedis jedis, String prefix, Map<String, Map<String, String>> want, Duration within)
            throws InterruptedException {
        long deadline = System.nanoTime() + within.toNanos();
        while (true) {
            Map<String, Map<String, String>> have = readPrefix(jedis, prefix);
            boolean caughtUp = want.entrySet().stream().allMatch(w -> {
                Map<String, String> h = have.get(w.getKey());
                return h != null && w.getValue().get(RedisStore.OFFSET).equals(h.get(RedisStore.OFFSET));
            });
            if (caughtUp || System.nanoTime() > deadline) {
                return have;
            }
            Thread.sleep(1000);
        }
    }

    private static Map<String, Map<String, String>> readPrefix(Jedis jedis, String prefix) {
        Map<String, Map<String, String>> out = new HashMap<>();
        String cursor = ScanParams.SCAN_POINTER_START;
        ScanParams params = new ScanParams().match(prefix + ":*").count(1000);
        do {
            ScanResult<String> page = jedis.scan(cursor, params);
            cursor = page.getCursor();
            Pipeline pipeline = jedis.pipelined();
            Map<String, Response<Map<String, String>>> responses = new HashMap<>();
            for (String key : page.getResult()) {
                responses.put(key, pipeline.hgetAll(key));
            }
            pipeline.sync();
            responses.forEach((k, v) -> out.put(k, v.get()));
        } while (!ScanParams.SCAN_POINTER_START.equals(cursor));
        return out;
    }

    private VerifyStore() {
    }
}
