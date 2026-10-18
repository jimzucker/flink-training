package st56;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.File;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/**
 * Reads all four outputs end to end and compares them with the generator's
 * manifest, with no tolerance. Exits 0 only if every assertion holds.
 *
 * --arm=clean: no key's count of applied orders (n) ever goes backwards, and
 *   it rises by exactly one per record, so every order was published once.
 * --arm=killed: a key may go backwards at most once (the replay after the
 *   restart), and must still end on the manifest's values.
 */
public final class VerifyCompleteness {

    static final class KeyState {
        int partition = -1;
        boolean split;
        long lastN = -1;
        long firstN = -1;
        int back;
        long gaps;
        long records;
        String last;
    }

    static final class TopicResult {
        String topic;
        long records;
        final Map<String, KeyState> keys = new HashMap<>();
    }

    static final List<String> failures = new ArrayList<>();
    static final List<String> passes = new ArrayList<>();

    static void check(boolean ok, String what) {
        if (ok) passes.add(what);
        else failures.add(what);
        System.out.println((ok ? "PASS  " : "FAIL  ") + what);
    }

    public static void main(String[] argv) throws Exception {
        Map<String, String> a = Args.parse(argv);
        String bootstrap = Args.need(a, "bootstrap");
        String arm = a.getOrDefault("arm", "clean");
        boolean killed = arm.equals("killed");
        String[] topics = {Args.need(a, "outSym"), Args.need(a, "outAcct"), Args.need(a, "mvSym"), Args.need(a, "mvAcct")};
        Map<String, Object> man = readManifest(new File(Args.need(a, "manifest")));

        @SuppressWarnings("unchecked") Map<String, Long> posSym = (Map<String, Long>) man.get("positionBySymbol");
        @SuppressWarnings("unchecked") Map<String, Long> nSym = (Map<String, Long>) man.get("ordersBySymbol");
        @SuppressWarnings("unchecked") Map<String, Long> posAcc = (Map<String, Long>) man.get("positionByAccount");
        @SuppressWarnings("unchecked") Map<String, Long> nAcc = (Map<String, Long>) man.get("ordersByAccount");
        @SuppressWarnings("unchecked") Map<String, Long> price = (Map<String, Long>) man.get("latestPriceCents");
        long unique = (Long) man.get("uniqueOrders");
        long dups = (Long) man.get("duplicateRecords");
        long t0 = System.currentTimeMillis();

        ExecutorService ex = Executors.newFixedThreadPool(4);
        List<Future<TopicResult>> fs = new ArrayList<>();
        for (String t : topics) fs.add(ex.submit(() -> read(bootstrap, t, killed)));
        TopicResult sym = fs.get(0).get(), acc = fs.get(1).get(), mvSym = fs.get(2).get(), mvAcc = fs.get(3).get();
        ex.shutdown();
        System.out.printf("read %,d + %,d + %,d + %,d records in %.1f s (arm: %s)%n",
                sym.records, acc.records, mvSym.records, mvAcc.records, (System.currentTimeMillis() - t0) / 1000.0, arm);

        // the interview's cardinality
        check(posSym.size() == Model.SYMBOLS, "manifest has 4,096 symbol keys (has " + posSym.size() + ")");
        check(posAcc.size() == Model.SYMBOLS * Model.ALLOCATIONS, "manifest has 16,384 account keys (has " + posAcc.size() + ")");

        positions("positions by symbol", sym, posSym, nSym, killed, unique);
        positions("positions by account", acc, posAcc, nAcc, killed, unique * Model.ALLOCATIONS);
        marketValues("market values by symbol", mvSym, posSym, nSym, price, killed, false);
        marketValues("market values by account", mvAcc, posAcc, nAcc, price, killed, true);

        // two paths over the same input agree
        long badSum = 0, badN = 0;
        for (Map.Entry<String, KeyState> e : sym.keys.entrySet()) {
            String s = e.getKey();
            long symPos = field(e.getValue().last, "position"), symN = field(e.getValue().last, "n");
            long sum = 0;
            for (String ac : Model.ACCOUNTS)
                for (String sb : Model.SUBS) {
                    KeyState k = acc.keys.get(Model.accountKey(ac, sb, s));
                    if (k == null) { badSum++; continue; }
                    sum += field(k.last, "position");
                    if (field(k.last, "n") != symN) badN++;
                }
            if (sum != symPos) badSum++;
        }
        check(badSum == 0, "for every symbol, its four account positions sum to its symbol position (" + badSum + " differ)");
        check(badN == 0, "for every symbol, each account key applied as many orders as the symbol (" + badN + " differ)");
        System.out.printf("repeats: the input carried %,d orders twice on purpose; every total above counts each once%n", dups);

        System.out.printf("%n%d checks passed, %d did not%n", passes.size(), failures.size());
        if (!failures.isEmpty()) {
            System.out.println("NOT COMPLETE (" + arm + "):");
            for (String f : failures) System.out.println("  " + f);
            System.exit(1);
        }
        System.out.println("COMPLETE (" + arm + ")");
    }

    static void common(String label, TopicResult r, Set<String> want, boolean killed) {
        check(r.keys.keySet().equals(want), label + ": distinct keys " + r.keys.size() + " = manifest " + want.size());
        long split = r.keys.values().stream().filter(k -> k.split).count();
        check(split == 0, label + ": every key in exactly one partition (" + split + " split)");
        long backKeys = r.keys.values().stream().filter(k -> k.back > 0).count();
        long overOnce = r.keys.values().stream().filter(k -> k.back > 1).count();
        if (killed) {
            check(overOnce == 0, label + ": no key went backwards more than once after the restart ("
                    + backKeys + " went back once, " + overOnce + " more than once)");
        } else {
            check(backKeys == 0, label + ": no key ever went backwards (" + backKeys + " did)");
        }
    }

    static void positions(String label, TopicResult r, Map<String, Long> pos, Map<String, Long> n, boolean killed, long expectRecords) {
        common(label, r, pos.keySet(), killed);
        long badPos = 0, badN = 0, gaps = 0;
        for (Map.Entry<String, KeyState> e : r.keys.entrySet()) {
            Long wantPos = pos.get(e.getKey()), wantN = n.get(e.getKey());
            if (wantPos == null) continue;
            if (field(e.getValue().last, "position") != wantPos) badPos++;
            if (field(e.getValue().last, "n") != wantN) badN++;
            gaps += e.getValue().gaps + (e.getValue().firstN == 1 ? 0 : 1);
        }
        check(badPos == 0, label + ": every key's final position equals the manifest (" + badPos + " differ)");
        check(badN == 0, label + ": every key's final count of orders applied equals the manifest (" + badN + " differ)");
        if (!killed) {
            check(gaps == 0, label + ": each key's count rose by exactly one per record, from 1 (" + gaps + " gaps or repeats)");
            check(r.records == expectRecords, label + ": " + r.records + " records = one per order applied, " + expectRecords);
        } else {
            check(r.records >= expectRecords, label + ": " + r.records + " records, at least one per order applied ("
                    + expectRecords + "); the replay adds " + (r.records - expectRecords));
        }
    }

    static void marketValues(String label, TopicResult r, Map<String, Long> pos, Map<String, Long> n,
                             Map<String, Long> price, boolean killed, boolean accountLevel) {
        common(label, r, pos.keySet(), killed);
        long badPos = 0, badPrice = 0, badMv = 0, badN = 0;
        for (Map.Entry<String, KeyState> e : r.keys.entrySet()) {
            String key = e.getKey();
            if (!pos.containsKey(key)) continue;
            String symbol = accountLevel ? key.substring(key.lastIndexOf('/') + 1) : key;
            String v = e.getValue().last;
            long p = field(v, "position"), pc = field(v, "priceCents"), mv = field(v, "marketValueCents");
            if (p != pos.get(key)) badPos++;
            if (pc != price.get(symbol)) badPrice++;
            if (mv != Math.multiplyExact(pos.get(key), price.get(symbol))) badMv++;
            if (field(v, "n") != n.get(key)) badN++;
        }
        check(badPos == 0, label + ": every key's last market value is on its final position (" + badPos + " differ)");
        check(badPrice == 0, label + ": every key's last market value uses the latest price (" + badPrice + " differ)");
        check(badMv == 0, label + ": every key's last market value = final position x latest price (" + badMv + " differ)");
        check(badN == 0, label + ": every key's last market value counts every order (" + badN + " differ)");
    }

    static TopicResult read(String bootstrap, String topic, boolean killed) {
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrap);
        p.put("key.deserializer", StringDeserializer.class.getName());
        p.put("value.deserializer", StringDeserializer.class.getName());
        p.put("max.poll.records", "20000");
        p.put("fetch.max.bytes", String.valueOf(64 << 20));
        p.put("max.partition.fetch.bytes", String.valueOf(16 << 20));
        TopicResult r = new TopicResult();
        r.topic = topic;
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(p)) {
            List<TopicPartition> tps = new ArrayList<>();
            for (PartitionInfo pi : c.partitionsFor(topic)) tps.add(new TopicPartition(topic, pi.partition()));
            c.assign(tps);
            c.seekToBeginning(tps);
            Map<TopicPartition, Long> end = c.endOffsets(tps);
            Set<TopicPartition> open = new HashSet<>();
            for (TopicPartition tp : tps) if (end.get(tp) > c.position(tp)) open.add(tp);
            int empty = 0;
            while (!open.isEmpty()) {
                ConsumerRecords<String, String> recs = c.poll(Duration.ofMillis(500));
                if (recs.isEmpty() && ++empty > 120) throw new IllegalStateException(topic + ": no records for 60 s before the end");
                for (ConsumerRecord<String, String> rec : recs) {
                    r.records++;
                    KeyState k = r.keys.computeIfAbsent(rec.key(), x -> new KeyState());
                    if (k.partition < 0) k.partition = rec.partition();
                    else if (k.partition != rec.partition()) k.split = true;
                    long n = field(rec.value(), "n");
                    if (k.lastN >= 0) {
                        if (n <= k.lastN) k.back++;
                        else if (n != k.lastN + 1) k.gaps++;
                    } else {
                        k.firstN = n;
                    }
                    k.lastN = n;
                    k.records++;
                    k.last = rec.value();
                }
                for (TopicPartition tp : new ArrayList<>(open)) if (c.position(tp) >= end.get(tp)) open.remove(tp);
            }
        }
        return r;
    }

    /** A whole-number field out of the job's own JSON. */
    static long field(String v, String name) {
        String pat = "\"" + name + "\":";
        int i = v.indexOf(pat);
        if (i < 0) throw new IllegalArgumentException("no field " + name + " in " + v);
        i += pat.length();
        int j = i;
        if (v.charAt(j) == '-') j++;
        while (j < v.length() && Character.isDigit(v.charAt(j))) j++;
        return Long.parseLong(v.substring(i, j));
    }

    /** Scalars as Long, objects of string to number as Map<String, Long>. */
    static Map<String, Object> readManifest(File f) throws Exception {
        Map<String, Object> m = new HashMap<>();
        try (JsonParser jp = new JsonFactory().createParser(f)) {
            jp.nextToken();
            while (jp.nextToken() == JsonToken.FIELD_NAME) {
                String name = jp.getCurrentName();
                JsonToken t = jp.nextToken();
                if (t == JsonToken.START_OBJECT) {
                    Map<String, Long> sub = new HashMap<>();
                    while (jp.nextToken() == JsonToken.FIELD_NAME) {
                        String k = jp.getCurrentName();
                        jp.nextToken();
                        sub.put(k, jp.getLongValue());
                    }
                    m.put(name, sub);
                } else if (t == JsonToken.VALUE_NUMBER_INT) {
                    m.put(name, jp.getLongValue());
                } else {
                    jp.skipChildren();
                }
            }
        }
        return m;
    }
}
