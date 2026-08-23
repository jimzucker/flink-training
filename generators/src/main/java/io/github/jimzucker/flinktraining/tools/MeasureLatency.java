package io.github.jimzucker.flinktraining.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Measures what a consumer actually waits for: the age of a record at the moment
 * it becomes readable.
 *
 * <p>The operators publish their own latency, but they cannot see the whole
 * story. Under exactly-once a record is not readable until the checkpoint that
 * produced it commits, so a consumer waits longer than the pipeline took. That
 * difference is a deliberate cost of not publishing numbers that a failure could
 * make wrong, and it is better measured and explained than left to be noticed.
 *
 * <p>Reads from the end of the topic, so it measures records produced while it is
 * watching rather than the backlog.
 */
public final class MeasureLatency {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("usage: MeasureLatency <bootstrap> <topic> <seconds> [timestampField]");
            System.exit(2);
        }
        String bootstrap = args[0];
        String topic = args[1];
        long seconds = Long.parseLong(args[2]);
        String field = args.length > 3 ? args[3] : "eventTime";

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "latency-" + System.nanoTime());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        List<Long> ages = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            long deadline = System.currentTimeMillis() + seconds * 1_000L;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> batch = consumer.poll(Duration.ofMillis(200));
                long readAt = System.currentTimeMillis();
                for (ConsumerRecord<String, String> record : batch) {
                    JsonNode node = MAPPER.readTree(record.value());
                    if (node.hasNonNull(field)) {
                        ages.add(readAt - node.get(field).asLong());
                    }
                }
            }
        }

        if (ages.isEmpty()) {
            System.out.printf("%-22s no records seen in %ds%n", topic, seconds);
            System.exit(1);
        }
        Collections.sort(ages);
        System.out.printf("%-22s n=%-6d p50=%-6d p95=%-6d p99=%-6d max=%-6d  (ms)%n",
                topic, ages.size(),
                percentile(ages, 50), percentile(ages, 95),
                percentile(ages, 99), ages.get(ages.size() - 1));
    }

    private static long percentile(List<Long> sorted, int p) {
        int index = (int) Math.ceil(p / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1)));
    }

    private MeasureLatency() {
    }
}
