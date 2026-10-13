package st54;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Completeness, with no tolerances, against the generator's manifest (SKILL section 4).
 * Exits 0 only if every assertion holds.
 *
 * Both arms: final position and input count per key equal the manifest; distinct keys equal
 * the interview's numbers; each key sits in exactly one partition; the two paths' grand totals
 * agree with each other and the manifest; every market value is position x price, and each
 * key's last one is final position x latest price.
 * Ordering, judged on n (inputs applied, only rises per key): clean arm, never backwards, and
 * for positions exactly one step at a time; killed arm, at most one backward step per key.
 * Clean arm also: position rows = unique orders (symbol) and 4 x unique orders (account).
 */
public final class Verify {

    static final class KeyTrack {
        int partition = -1;
        long lastN = Long.MIN_VALUE;
        int backward;
        int gaps;
        long rows;
        JsonNode last;
    }

    static final class TopicResult {
        final Map<String, KeyTrack> keys = new HashMap<>();
        long rows;
        long splitKeys;
    }

    static final List<String> failures = new ArrayList<>();

    static long failureCount;

    static void check(boolean ok, String what) {
        if (!ok) {
            failureCount++;
            if (failures.size() < 1000) {
                failures.add(what);
            }
        }
    }

    public static void main(String[] argv) throws Exception {
        Args a = new Args(argv);
        String bootstrap = a.get("bootstrap");
        String arm = a.get("arm", "clean");
        boolean killed = arm.equals("killed");
        JsonNode m = new ObjectMapper().readTree(new File(a.get("manifest")));
        long unique = m.get("uniqueOrders").asLong();
        int symKeys = m.get("symbolKeys").asInt();
        int acctKeys = m.get("accountKeys").asInt();

        TopicResult ps = read(bootstrap, a.get("posSym"), true, killed);
        TopicResult pa = read(bootstrap, a.get("posAcct"), true, killed);
        TopicResult ms = read(bootstrap, a.get("mvSym"), false, killed);
        TopicResult ma = read(bootstrap, a.get("mvAcct"), false, killed);

        // the interview's key counts: 4,096 symbols; 2 accounts x 2 sub-accounts x 4,096
        check(symKeys == Model.SYMBOLS, "manifest has " + symKeys + " symbols, the interview said " + Model.SYMBOLS);
        check(acctKeys == Model.SYMBOLS * Model.ALLOCS, "manifest has " + acctKeys + " account keys, the interview said "
                + Model.SYMBOLS * Model.ALLOCS);
        check(ps.keys.size() == symKeys, "positions-by-symbol has " + ps.keys.size() + " keys, expected " + symKeys);
        check(pa.keys.size() == acctKeys, "positions-by-account has " + pa.keys.size() + " keys, expected " + acctKeys);
        check(ms.keys.size() == symKeys, "market-values-by-symbol has " + ms.keys.size() + " keys, expected " + symKeys);
        check(ma.keys.size() == acctKeys, "market-values-by-account has " + ma.keys.size() + " keys, expected " + acctKeys);

        long symSum = finals("symbol", ps, m.get("totalBySymbol"), m.get("countBySymbol"));
        long acctSum = finals("account", pa, m.get("totalByAccount"), m.get("countByAccount"));
        long grand = m.get("grandTotal").asLong();
        check(symSum == grand, "sum of symbol positions " + symSum + " != manifest grand total " + grand);
        check(acctSum == grand, "sum of account positions " + acctSum + " != manifest grand total " + grand);
        check(symSum == acctSum, "the two paths disagree: symbol " + symSum + ", account " + acctSum);

        marketValues("symbol", ms, m.get("totalBySymbol"), m.get("countBySymbol"), m.get("latestPriceCentsBySymbol"));
        marketValues("account", ma, m.get("totalByAccount"), m.get("countByAccount"), m.get("latestPriceCentsBySymbol"));

        if (!killed) {
            check(ps.rows == unique, "positions-by-symbol has " + ps.rows + " rows on the clean run, expected one per unique order: " + unique);
            check(pa.rows == unique * Model.ALLOCS, "positions-by-account has " + pa.rows + " rows on the clean run, expected "
                    + unique * Model.ALLOCS);
        }

        String[] names = {"positions-by-symbol", "positions-by-account", "market-values-by-symbol", "market-values-by-account"};
        TopicResult[] all = {ps, pa, ms, ma};
        long[] back = new long[4];
        long[] keysBack = new long[4];
        for (int i = 0; i < 4; i++) {
            TopicResult t = all[i];
            check(t.splitKeys == 0, names[i] + ": " + t.splitKeys + " keys appear in more than one partition");
            for (Map.Entry<String, KeyTrack> e : t.keys.entrySet()) {
                KeyTrack k = e.getValue();
                back[i] += k.backward;
                if (k.backward > 0) {
                    keysBack[i]++;
                }
                if (!killed) {
                    check(k.backward == 0, names[i] + " key " + e.getKey() + " went backwards " + k.backward + " times on the clean run");
                    if (i < 2) {
                        check(k.gaps == 0, names[i] + " key " + e.getKey() + " skipped an input count " + k.gaps + " times on the clean run");
                    }
                } else {
                    check(k.backward <= 1, names[i] + " key " + e.getKey() + " went backwards " + k.backward + " times; at most once is a restart's replay");
                }
            }
        }

        StringBuilder sb = new StringBuilder();
        sb.append("{\"arm\":\"").append(arm).append("\"");
        for (int i = 0; i < 4; i++) {
            sb.append(",\"").append(names[i]).append("\":{\"rows\":").append(all[i].rows).append(",\"keys\":")
              .append(all[i].keys.size()).append(",\"backwardSteps\":").append(back[i])
              .append(",\"keysThatWentBackwards\":").append(keysBack[i]).append('}');
        }
        sb.append(",\"uniqueOrders\":").append(unique).append(",\"duplicateRecordsInInput\":")
          .append(m.get("duplicateRecords").asLong()).append(",\"grandTotal\":").append(grand)
          .append(",\"failures\":").append(failureCount).append('}');
        System.out.println(sb);
        if (!failures.isEmpty()) {
            System.out.println("COMPLETENESS NOT MET on the " + arm + " arm. First problems:");
            for (int i = 0; i < Math.min(20, failures.size()); i++) {
                System.out.println("  " + failures.get(i));
            }
            System.exit(1);
        }
        System.out.println("every assertion held on the " + arm + " arm");
    }

    static long finals(String level, TopicResult t, JsonNode totals, JsonNode counts) {
        long sum = 0;
        int bad = 0;
        var it = totals.fields();
        while (it.hasNext()) {
            var e = it.next();
            KeyTrack k = t.keys.get(e.getKey());
            if (k == null) {
                check(false, level + " key " + e.getKey() + " never published");
                continue;
            }
            long pos = k.last.get("position").asLong();
            long n = k.last.get("n").asLong();
            sum += pos;
            if (pos != e.getValue().asLong() || n != counts.get(e.getKey()).asLong()) {
                if (bad++ < 5) {
                    check(false, level + " key " + e.getKey() + ": final position " + pos + " (n " + n + "), manifest "
                            + e.getValue().asLong() + " (n " + counts.get(e.getKey()).asLong() + ")");
                }
            }
        }
        check(bad == 0, bad + " " + level + " positions do not match the manifest");
        return sum;
    }

    static void marketValues(String level, TopicResult t, JsonNode totals, JsonNode counts, JsonNode latest) {
        int bad = 0;
        var it = totals.fields();
        while (it.hasNext()) {
            var e = it.next();
            KeyTrack k = t.keys.get(e.getKey());
            if (k == null) {
                check(false, level + " market value for " + e.getKey() + " never published");
                continue;
            }
            JsonNode v = k.last;
            long pos = v.get("position").asLong();
            long price = v.get("priceCents").asLong();
            long mv = v.get("marketValueCents").asLong();
            long want = latest.get(Model.symbolOfKey(e.getKey())).asLong();
            boolean ok = pos == e.getValue().asLong() && v.get("n").asLong() == counts.get(e.getKey()).asLong()
                    && price == want && mv == Math.multiplyExact(e.getValue().asLong(), want);
            if (!ok && bad++ < 5) {
                check(false, level + " market value for " + e.getKey() + ": " + v + "; expected position "
                        + e.getValue().asLong() + " x latest price " + want);
            }
        }
        check(bad == 0, bad + " " + level + " final market values are not final position x latest price");
    }

    static TopicResult read(String bootstrap, String topic, boolean position, boolean killed) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10000");
        p.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, String.valueOf(64 << 20));
        p.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, String.valueOf(16 << 20));
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        ObjectMapper om = new ObjectMapper();
        TopicResult r = new TopicResult();
        int bad = 0;
        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(p)) {
            List<TopicPartition> tps = new ArrayList<>();
            for (PartitionInfo pi : c.partitionsFor(topic)) {
                tps.add(new TopicPartition(topic, pi.partition()));
            }
            c.assign(tps);
            c.seekToBeginning(tps);
            Map<TopicPartition, Long> end = c.endOffsets(tps);
            long want = end.values().stream().mapToLong(Long::longValue).sum();
            while (r.rows < want) {
                ConsumerRecords<byte[], byte[]> recs = c.poll(Duration.ofSeconds(5));
                if (recs.isEmpty()) {
                    boolean done = true;
                    for (TopicPartition tp : tps) {
                        if (c.position(tp) < end.get(tp)) {
                            done = false;
                        }
                    }
                    if (done) {
                        break;
                    }
                }
                for (ConsumerRecord<byte[], byte[]> rec : recs) {
                    if (rec.offset() >= end.get(new TopicPartition(topic, rec.partition()))) {
                        continue;
                    }
                    r.rows++;
                    String key = new String(rec.key(), StandardCharsets.UTF_8);
                    JsonNode v;
                    try {
                        v = om.readTree(rec.value());
                    } catch (Exception e) {
                        if (bad++ < 5) {
                            check(false, topic + ": unreadable record at " + rec.partition() + "/" + rec.offset());
                        }
                        continue;
                    }
                    KeyTrack k = r.keys.computeIfAbsent(key, x -> new KeyTrack());
                    if (k.partition < 0) {
                        k.partition = rec.partition();
                    } else if (k.partition != rec.partition()) {
                        r.splitKeys++;
                        k.partition = rec.partition();
                    }
                    long n = v.get("n").asLong();
                    if (k.lastN != Long.MIN_VALUE) {
                        if (n < k.lastN || (n == k.lastN && !position && !killed)) {
                            k.backward++;
                        } else if (n == k.lastN && position) {
                            k.backward++;   // a position republished with the same count is a replay
                        } else if (position && n != k.lastN + 1) {
                            k.gaps++;
                        }
                    } else if (position && n != 1 && !killed) {
                        k.gaps++;
                    }
                    if (!position && v.get("marketValueCents").asLong()
                            != Math.multiplyExact(v.get("position").asLong(), v.get("priceCents").asLong())) {
                        check(false, topic + " " + key + ": market value is not position x price: " + v);
                    }
                    k.lastN = n;
                    k.rows++;
                    k.last = v;
                }
            }
        }
        System.err.printf("read %,d rows, %,d keys from %s%n", r.rows, r.keys.size(), topic);
        return r;
    }
}
