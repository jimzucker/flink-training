package sqlapp;

import java.io.FileWriter;
import java.util.Properties;
import java.util.SplittableRandom;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Orders from a seed: the same seed gives the same orders, byte for byte, and
 * the manifest is computed from them -- per account, the count and the total
 * quantity, the two things completeness compares on both platforms.
 * --mode=manifest writes the manifest without producing (the determinism check).
 */
public final class OrdersGenerator {
    static final int ACCOUNTS = 4, SYMBOLS = 4096;

    public static void main(String[] args) throws Exception {
        var a = EnrichJob.parse(args);
        long count = Long.parseLong(a.get("count"));
        long seed = Long.parseLong(a.get("seed"));
        boolean produce = !"manifest".equals(a.get("mode"));
        int partitions = Integer.parseInt(a.getOrDefault("partitions", "1"));
        long[] n = new long[ACCOUNTS], qty = new long[ACCOUNTS];
        KafkaProducer<String, String> p = null;
        if (produce) {
            Properties pr = new Properties();
            pr.put("bootstrap.servers", a.get("bootstrap"));
            pr.put("acks", "all");
            pr.put("linger.ms", "20");
            pr.put("batch.size", "524288");
            pr.put("compression.type", "lz4");
            p = new KafkaProducer<>(pr, new StringSerializer(), new StringSerializer());
        }
        SplittableRandom r = new SplittableRandom(seed);
        for (long i = 0; i < count; i++) {
            int account = r.nextInt(ACCOUNTS), symbol = r.nextInt(SYMBOLS), q = 1 + r.nextInt(99);
            double price = 1 + r.nextInt(49900) / 100.0;
            n[account]++;
            qty[account] += q;
            if (p != null) {
                String v = "{\"order_id\":\"" + seed + "-" + i + "\",\"symbol\":" + symbol + ",\"account\":" + account
                        + ",\"qty\":" + q + ",\"price\":" + price + "}";
                p.send(new ProducerRecord<>(a.get("topic"), (int) (i % partitions), null, v));
            }
            if (p != null && i % 5_000_000 == 0 && i > 0) System.out.println("produced " + i);
        }
        if (p != null) { p.flush(); p.close(); }
        StringBuilder rows = new StringBuilder();
        for (int k = 0; k < ACCOUNTS; k++) {
            if (k > 0) rows.append(",");
            rows.append("{\"account\":").append(k).append(",\"n\":").append(n[k]).append(",\"qty\":").append(qty[k]).append("}");
        }
        try (FileWriter w = new FileWriter(a.get("manifest"))) {
            w.write("{\"orderCount\":" + count + ",\"seed\":" + seed + ",\"accounts\":" + ACCOUNTS + ",\"rows\":[" + rows + "]}");
        }
        System.out.println("orders " + count + " written; manifest " + a.get("manifest"));
    }
}
