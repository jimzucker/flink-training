package st44;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * Read one topic from the beginning to the end offsets it had when the read
 * started, once. Used by the verifier and by the progress command.
 *
 * No consumer group, no committed offsets: this must not be able to disturb the
 * thing being measured, and the harness's first vantage point is exactly the
 * committed offsets of the pipeline's own group.
 */
public final class TopicReader {

    public static long readAll(String bootstrap, String topic,
                               Consumer<ConsumerRecord<byte[], byte[]>> sink) {
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrap);
        p.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        p.put("enable.auto.commit", "false");
        p.put("max.poll.records", "10000");
        p.put("fetch.max.bytes", "33554432");
        p.put("auto.offset.reset", "earliest");
        long seen = 0;
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(p)) {
            List<PartitionInfo> infos = consumer.partitionsFor(topic);
            if (infos == null || infos.isEmpty()) {
                throw new IllegalStateException("topic " + topic + " has no partitions");
            }
            List<TopicPartition> tps = new ArrayList<>();
            for (PartitionInfo i : infos) {
                tps.add(new TopicPartition(topic, i.partition()));
            }
            consumer.assign(tps);
            Map<TopicPartition, Long> ends = new HashMap<>(consumer.endOffsets(tps));
            consumer.seekToBeginning(tps);
            long target = 0;
            for (TopicPartition tp : tps) {
                target += ends.get(tp) - consumer.position(tp);
            }
            long idle = 0;
            while (seen < target) {
                ConsumerRecords<byte[], byte[]> batch = consumer.poll(Duration.ofMillis(500));
                if (batch.isEmpty()) {
                    if (++idle > 60) {
                        throw new IllegalStateException("stalled after " + seen + " of " + target
                                + " records on " + topic);
                    }
                    continue;
                }
                idle = 0;
                for (ConsumerRecord<byte[], byte[]> r : batch) {
                    Long end = ends.get(new TopicPartition(r.topic(), r.partition()));
                    if (end != null && r.offset() >= end) {
                        continue;
                    }
                    sink.accept(r);
                    seen++;
                }
            }
        }
        return seen;
    }

    private TopicReader() {
    }
}
