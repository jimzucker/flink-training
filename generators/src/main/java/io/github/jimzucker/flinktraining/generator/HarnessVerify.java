package io.github.jimzucker.flinktraining.generator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/**
 * Asserts the position topics against the manifest, with no tolerances.
 *
 * <p>The counterpart to {@link HarnessBacklog}: it exists so this repository's
 * own job can pass the same completeness gate the skill's pipelines do. The
 * expected values come from the manifest, which came from the generator, which
 * never saw the pipeline's output. A sink is at-least-once, so a key may be
 * written more than once; the last write per key is the answer and repeats are
 * counted, not tolerated.
 */
public final class HarnessVerify {

    private HarnessVerify() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> a = new LinkedHashMap<>();
        for (String arg : args) {
            int eq = arg.indexOf('=');
            if (arg.startsWith("--") && eq > 0) {
                a.put(arg.substring(2, eq), arg.substring(eq + 1));
            }
        }
        String bootstrap = a.getOrDefault("bootstrap", "localhost:9092");
        String symbolTopic = a.getOrDefault("symbolTopic", "positions-by-symbol");
        String accountTopic = a.getOrDefault("accountTopic", "positions-by-account");
        Path manifestPath = Path.of(a.getOrDefault("manifest", "manifest.json"));

        ObjectMapper mapper = new ObjectMapper();
        JsonNode manifest = mapper.readTree(Files.readString(manifestPath));
        Map<String, Long> wantSymbol = longs(manifest.get("netBySymbol"));
        Map<String, Long> wantAccount = longs(manifest.get("netByAccount"));

        Map<String, Long> gotSymbol = lastQuantityPerKey(bootstrap, symbolTopic, mapper, false);
        Map<String, Long> gotAccount = lastQuantityPerKey(bootstrap, accountTopic, mapper, true);

        List<String> failures = new ArrayList<>();
        compare("positions-by-symbol", wantSymbol, gotSymbol, failures);
        compare("positions-by-account", wantAccount, gotAccount, failures);

        long wantTotal = wantSymbol.values().stream().mapToLong(Long::longValue).sum();
        long gotTotal = gotSymbol.values().stream().mapToLong(Long::longValue).sum();
        if (wantTotal != gotTotal) {
            failures.add(String.format("net quantity across all symbols: expected %,d, got %,d",
                    wantTotal, gotTotal));
        }
        long accountTotal = gotAccount.values().stream().mapToLong(Long::longValue).sum();
        if (accountTotal != gotTotal) {
            failures.add(String.format("the two paths disagree: symbols net %,d, accounts net %,d",
                    gotTotal, accountTotal));
        }

        if (failures.isEmpty()) {
            System.out.printf("  OK   positions-by-symbol: %,d keys, every net quantity exact%n",
                    wantSymbol.size());
            System.out.printf("  OK   positions-by-account: %,d keys, every net quantity exact%n",
                    wantAccount.size());
            System.out.printf("  OK   both paths net to %,d, as the manifest predicts%n", wantTotal);
            System.out.println("COMPLETENESS OK: every assertion held with no tolerance");
            return;
        }
        failures.forEach(f -> System.out.println("  FAIL " + f));
        System.exit(1);
    }

    private static void compare(String label, Map<String, Long> want, Map<String, Long> got,
                                List<String> failures) {
        if (want.size() != got.size()) {
            failures.add(String.format("%s: expected %,d keys, got %,d", label, want.size(), got.size()));
        }
        int wrong = 0;
        String first = null;
        for (Map.Entry<String, Long> e : want.entrySet()) {
            Long actual = got.get(e.getKey());
            if (actual == null || !actual.equals(e.getValue())) {
                wrong++;
                if (first == null) {
                    first = String.format("%s: key %s expected %,d, got %s",
                            label, e.getKey(), e.getValue(), actual);
                }
            }
        }
        if (wrong > 0) {
            failures.add(first + String.format("  (%,d key(s) wrong)", wrong));
        }
    }

    private static Map<String, Long> longs(JsonNode node) {
        Map<String, Long> out = new TreeMap<>();
        node.fields().forEachRemaining(e -> out.put(e.getKey(), e.getValue().asLong()));
        return out;
    }

    /** Drains a topic and keeps the last record per key, which is what an idempotent sink means. */
    private static Map<String, Long> lastQuantityPerKey(String bootstrap, String topic,
                                                        ObjectMapper mapper, boolean accountKey) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "harness-verify-" + System.nanoTime());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10_000);

        Map<String, Long> last = new HashMap<>();
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> parts = new ArrayList<>();
            for (PartitionInfo p : consumer.partitionsFor(topic)) {
                parts.add(new TopicPartition(topic, p.partition()));
            }
            consumer.assign(parts);
            Map<TopicPartition, Long> ends = consumer.endOffsets(parts);
            consumer.seekToBeginning(parts);
            long remaining = ends.values().stream().mapToLong(Long::longValue).sum();
            while (remaining > 0) {
                ConsumerRecords<String, byte[]> batch = consumer.poll(Duration.ofSeconds(5));
                if (batch.isEmpty()) {
                    break;
                }
                for (ConsumerRecord<String, byte[]> r : batch) {
                    JsonNode n = readTree(mapper, r.value());
                    // the demo keys account positions as account/subAccount/symbol
                    // and symbol positions by the symbol itself
                    String key = n.get("key").asText();
                    last.put(key, n.get("quantity").asLong());
                    remaining--;
                }
            }
        }
        return last;
    }

    private static JsonNode readTree(ObjectMapper mapper, byte[] bytes) {
        try {
            return mapper.readTree(bytes);
        } catch (Exception e) {
            throw new IllegalStateException("unreadable sink record", e);
        }
    }
}
