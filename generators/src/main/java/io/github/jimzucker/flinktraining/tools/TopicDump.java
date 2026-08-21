package io.github.jimzucker.flinktraining.tools;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Reads whole topics to files, and says so when it could not.
 *
 * <p>Replaces a console consumer per topic. That approach could not distinguish
 * "the topic is empty" from "the read did not finish in time": both come back as
 * zero records, and the verification then reports missing data when the data was
 * there. On a loaded machine the timeout is reached routinely, so the failure
 * looked like a pipeline bug and was not one.
 *
 * <p>Here the end offsets are read first, so completion is a fact rather than a
 * guess: the dump finishes when every partition has been consumed to its end
 * offset, and exits non-zero naming the topics that fell short. Under
 * {@code read_committed} the end offset is the last stable offset, so
 * transaction markers are accounted for without having to reason about them.
 *
 * <p>One process reads every topic, which also removes a JVM start per topic.
 */
public final class TopicDump {

    private static final Duration POLL = Duration.ofMillis(250);

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("usage: TopicDump <bootstrap> <outDir> <topic>... [--deadline-seconds N]");
            System.exit(2);
        }

        String bootstrap = args[0];
        Path outDir = Path.of(args[1]);
        long deadlineSeconds = 120;
        List<String> topics = new ArrayList<>();
        for (int i = 2; i < args.length; i++) {
            if ("--deadline-seconds".equals(args[i]) && i + 1 < args.length) {
                deadlineSeconds = Long.parseLong(args[++i]);
            } else {
                topics.add(args[i]);
            }
        }
        Files.createDirectories(outDir);

        int incomplete = 0;
        for (String topic : topics) {
            incomplete += dump(bootstrap, outDir, topic, deadlineSeconds) ? 0 : 1;
        }
        System.exit(incomplete == 0 ? 0 : 1);
    }

    private static boolean dump(String bootstrap, Path outDir, String topic, long deadlineSeconds)
            throws IOException {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dump-" + topic + "-" + System.nanoTime());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // The position sinks write transactionally; an uncommitted read would
        // report records belonging to transactions that may still abort.
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        Path out = outDir.resolve(topic + ".jsonl");
        long read = 0;
        long expected;
        boolean complete;

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
             BufferedWriter writer = Files.newBufferedWriter(out, StandardCharsets.UTF_8)) {

            List<TopicPartition> partitions = new ArrayList<>();
            List<PartitionInfo> info = consumer.partitionsFor(topic, Duration.ofSeconds(30));
            if (info == null) {
                System.out.printf("%-22s NO SUCH TOPIC%n", topic);
                return false;
            }
            info.forEach(p -> partitions.add(new TopicPartition(topic, p.partition())));
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            Map<TopicPartition, Long> ends = new HashMap<>(consumer.endOffsets(partitions));
            Map<TopicPartition, Long> starts = new HashMap<>(consumer.beginningOffsets(partitions));
            expected = ends.entrySet().stream()
                    .mapToLong(e -> e.getValue() - starts.get(e.getKey())).sum();

            long deadline = System.currentTimeMillis() + deadlineSeconds * 1_000L;
            while (System.currentTimeMillis() < deadline && !atEnd(consumer, partitions, ends)) {
                ConsumerRecords<String, String> batch = consumer.poll(POLL);
                for (ConsumerRecord<String, String> record : batch) {
                    writer.write(record.value());
                    writer.newLine();
                    read++;
                }
            }
            complete = atEnd(consumer, partitions, ends);
        }

        System.out.printf("%-22s %6d records%s%n", topic, read,
                complete ? "" : "  INCOMPLETE (offsets said " + expected + ")");
        return complete;
    }

    /** True once every partition has been consumed up to the end offset seen at assignment. */
    private static boolean atEnd(KafkaConsumer<String, String> consumer,
                                 List<TopicPartition> partitions,
                                 Map<TopicPartition, Long> ends) {
        for (TopicPartition partition : partitions) {
            if (consumer.position(partition) < ends.get(partition)) {
                return false;
            }
        }
        return true;
    }

    private TopicDump() {
    }
}
