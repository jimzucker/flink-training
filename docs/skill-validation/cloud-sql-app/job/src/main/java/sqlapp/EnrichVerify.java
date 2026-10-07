package sqlapp;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Reads every committed record of the output and compares, per account, the
 * count and the total quantity with the manifest -- no tolerance. The same
 * comparison Confluent Cloud's completeness makes with verifySql. The sink is
 * exactly-once, so the killed arm must match exactly too.
 */
public final class EnrichVerify {
    static final Pattern ACC = Pattern.compile("\"account\":(\\d+)"), QTY = Pattern.compile("\"qty\":(\\d+)");

    public static void main(String[] args) throws Exception {
        var a = EnrichJob.parse(args);
        String topic = a.getOrDefault("topic", "enriched");
        String man = Files.readString(Path.of(a.get("manifest")));
        Properties pr = new Properties();
        pr.put("bootstrap.servers", a.get("bootstrap"));
        pr.put("isolation.level", "read_committed");
        pr.put("enable.auto.commit", "false");
        pr.put("max.poll.records", "20000");
        long[] n = new long[OrdersGenerator.ACCOUNTS], qty = new long[OrdersGenerator.ACCOUNTS];
        long total = 0;
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(pr, new StringDeserializer(), new StringDeserializer())) {
            List<TopicPartition> parts = new ArrayList<>();
            c.partitionsFor(topic).forEach(pi -> parts.add(new TopicPartition(topic, pi.partition())));
            c.assign(parts);
            c.seekToBeginning(parts);
            Map<TopicPartition, Long> end = c.endOffsets(parts);
            while (parts.stream().anyMatch(tp -> c.position(tp) < end.get(tp))) {
                for (ConsumerRecord<String, String> r : c.poll(Duration.ofMillis(500))) {
                    Matcher ma = ACC.matcher(r.value()), mq = QTY.matcher(r.value());
                    if (!ma.find() || !mq.find()) { System.out.println("unreadable record: " + r.value()); System.exit(1); }
                    int k = Integer.parseInt(ma.group(1));
                    n[k]++; qty[k] += Long.parseLong(mq.group(1)); total++;
                }
            }
        }
        boolean ok = true;
        for (int k = 0; k < OrdersGenerator.ACCOUNTS; k++) {
            Matcher row = Pattern.compile("\\{\"account\":" + k + ",\"n\":(\\d+),\"qty\":(\\d+)\\}").matcher(man);
            if (!row.find()) { System.out.println("manifest has no row for account " + k); System.exit(1); }
            long en = Long.parseLong(row.group(1)), eq = Long.parseLong(row.group(2));
            boolean same = en == n[k] && eq == qty[k];
            ok &= same;
            System.out.println("account " + k + ": records " + n[k] + " (manifest " + en + "), quantity " + qty[k]
                    + " (manifest " + eq + ")" + (same ? "" : "  <-- DIFFERS"));
        }
        System.out.println((ok ? "VERIFY PASS" : "VERIFY: the outputs do not match the manifest") + " (" + a.getOrDefault("arm", "clean")
                + " arm, " + total + " records)");
        System.exit(ok ? 0 : 1);
    }
}
