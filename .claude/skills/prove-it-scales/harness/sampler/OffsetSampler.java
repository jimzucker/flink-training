import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * The transport vantage point. Throughput is read from committed broker offsets,
 * never from the engine's metric service, which starves at exactly the load the
 * measurement cares about.
 *
 * Compiled by the harness against the broker image's own kafka-clients jar and
 * run as a container on the stack network, so it is reaped with the stack and
 * does not live in the pipeline's jar. Prints one JSON line per tick.
 */
public final class OffsetSampler {
    public static void main(String[] args) throws Exception {
        Map<String, String> a = new HashMap<>();
        for (String s : args) {
            if (s.startsWith("--") && s.contains("=")) {
                int i = s.indexOf('=');
                a.put(s.substring(2, i), s.substring(i + 1));
            }
        }
        String bootstrap = a.get("bootstrap");
        String group = a.get("group");
        String inTopic = a.get("inTopic");
        String[] outTopics = a.getOrDefault("outTopics", "").isEmpty() ? new String[0] : a.get("outTopics").split(",");
        long interval = Long.parseLong(a.getOrDefault("intervalMs", "500"));
        if (bootstrap == null || group == null || inTopic == null)
            throw new IllegalArgumentException("need --bootstrap --group --inTopic");

        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrap);
        Admin admin = Admin.create(p);
        Properties cp = new Properties();
        cp.put("bootstrap.servers", bootstrap);
        cp.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        cp.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        cp.put("group.id", "sampler-" + UUID.randomUUID());
        KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(cp);

        List<TopicPartition> in = tps(c, inTopic);
        Map<String, List<TopicPartition>> outs = new LinkedHashMap<>();
        for (String t : outTopics) outs.put(t, tps(c, t));

        System.out.println("{\"sampler\":\"up\",\"group\":\"" + group + "\",\"inPartitions\":" + in.size() + "}");
        System.out.flush();
        while (true) {
            long ts = System.currentTimeMillis();
            long committed = 0;
            try {
                Map<TopicPartition, OffsetAndMetadata> m =
                        admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get();
                for (Map.Entry<TopicPartition, OffsetAndMetadata> e : m.entrySet())
                    if (e.getKey().topic().equals(inTopic) && e.getValue() != null)
                        committed += e.getValue().offset();
            } catch (Exception e) {
                committed = -1;
            }
            StringBuilder sb = new StringBuilder();
            sb.append("{\"ts\":").append(ts).append(",\"committed\":").append(committed)
              .append(",\"endIn\":").append(sum(c.endOffsets(in)));
            for (Map.Entry<String, List<TopicPartition>> e : outs.entrySet())
                sb.append(",\"end_").append(e.getKey()).append("\":").append(sum(c.endOffsets(e.getValue())));
            sb.append('}');
            System.out.println(sb);
            System.out.flush();
            long sleep = interval - (System.currentTimeMillis() - ts);
            if (sleep > 0) Thread.sleep(sleep);
        }
    }

    static List<TopicPartition> tps(KafkaConsumer<byte[], byte[]> c, String topic) {
        List<TopicPartition> l = new ArrayList<>();
        List<PartitionInfo> pis = c.partitionsFor(topic);
        if (pis == null || pis.isEmpty()) throw new IllegalStateException("topic has no partitions: " + topic);
        for (PartitionInfo pi : pis) l.add(new TopicPartition(topic, pi.partition()));
        return l;
    }

    static long sum(Map<TopicPartition, Long> m) {
        long s = 0;
        for (Long v : m.values()) s += v;
        return s;
    }
}
