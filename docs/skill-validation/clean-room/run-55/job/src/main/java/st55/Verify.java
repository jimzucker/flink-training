package st55;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

/**
 * Completeness, with no tolerances. Reads all four outputs end to end and
 * compares them with the generator's manifest (never with the pipeline).
 *
 * Per topic: distinct keys equal the interview's answer; every key in exactly
 * one partition; per key, the count of inputs applied (n) never goes backwards
 * on the clean arm and at most once on the killed arm; on the clean arm each
 * position record is exactly one more input than the last (nothing lost,
 * nothing doubled, repeats dropped); the final value per key equals the
 * manifest. Market values: final = final position x latest price, at both
 * levels. Two paths: the account positions summed per symbol equal the symbol
 * positions.
 *
 * Exits 0 only if every assertion holds.
 */
public class Verify {

    static final class KeyState {
        long lastN = -1, lastPos, lastPrice, lastMv, records, backward, gaps;
        int partition = -1;
        boolean splitPartitions;
    }

    static final JsonFactory JF = new JsonFactory();
    static int failures = 0;

    public static void main(String[] argv) throws Exception {
        Args a = new Args(argv);
        String arm = a.get("arm", "clean");
        boolean clean = arm.equals("clean");
        JsonNode m = new ObjectMapper().readTree(new File(a.get("manifest")));
        String bootstrap = a.get("bootstrap");
        long unique = m.get("uniqueOrders").asLong();
        System.out.printf("verifier: arm=%s, manifest %,d records, %,d unique orders, %,d repeated on purpose%n",
                arm, m.get("recordCount").asLong(), unique, m.get("duplicateRecords").asLong());

        Map<String, KeyState> sym = read(bootstrap, a.get("outSym"), false, clean);
        Map<String, KeyState> acct = read(bootstrap, a.get("outAcct"), false, clean);
        Map<String, KeyState> mvSym = read(bootstrap, a.get("mvSym"), true, clean);
        Map<String, KeyState> mvAcct = read(bootstrap, a.get("mvAcct"), true, clean);

        JsonNode totalBySymbol = m.get("totalBySymbol");
        JsonNode countBySymbol = m.get("countBySymbol");
        JsonNode totalByAccount = m.get("totalByAccount");
        JsonNode prices = m.get("latestPriceCents");

        // expected keys: every key with at least one order in the input
        Set<String> wantSym = new TreeSet<>(), wantAcct = new TreeSet<>();
        for (Iterator<String> it = countBySymbol.fieldNames(); it.hasNext(); ) {
            String s = it.next();
            if (countBySymbol.get(s).asLong() > 0) {
                wantSym.add(s);
                for (String[] pr : Keys.PAIRS) wantAcct.add(Keys.acctKey(pr[0], pr[1], s));
            }
        }
        check(wantSym.size() == m.get("distinctSymbols").asInt(),
                "the input holds every one of the %d symbols the interview predicted (it holds %d)",
                m.get("distinctSymbols").asInt(), wantSym.size());

        common("positions-by-symbol", sym, wantSym, clean);
        common("positions-by-account", acct, wantAcct, clean);
        common("market-values-by-symbol", mvSym, wantSym, clean);
        common("market-values-by-account", mvAcct, wantAcct, clean);

        // record counts: one position per applied input, repeats dropped
        long symRecs = sym.values().stream().mapToLong(k -> k.records).sum();
        long acctRecs = acct.values().stream().mapToLong(k -> k.records).sum();
        if (clean) {
            check(symRecs == unique, "positions-by-symbol holds one record per unique order: %,d records, %,d orders", symRecs, unique);
            check(acctRecs == 4 * unique, "positions-by-account holds four records per unique order: %,d records, %,d expected", acctRecs, 4 * unique);
        } else {
            check(symRecs >= unique, "positions-by-symbol holds at least one record per unique order: %,d records, %,d orders", symRecs, unique);
            check(acctRecs >= 4 * unique, "positions-by-account holds at least four records per unique order: %,d records, %,d expected", acctRecs, 4 * unique);
        }

        // final values
        int badSym = 0, badSymN = 0, badAcct = 0, badMv = 0, badMvA = 0, badPath = 0;
        for (String s : wantSym) {
            KeyState k = sym.get(s);
            if (k == null) continue;
            if (k.lastPos != totalBySymbol.get(s).asLong()) badSym++;
            if (k.lastN != countBySymbol.get(s).asLong()) badSymN++;
            long price = prices.get(s).asLong();
            KeyState mv = mvSym.get(s);
            if (mv == null || mv.lastPos != totalBySymbol.get(s).asLong() || mv.lastPrice != price
                    || mv.lastMv != totalBySymbol.get(s).asLong() * price || mv.lastN != countBySymbol.get(s).asLong()) badMv++;
            long pathSum = 0;
            for (String[] pr : Keys.PAIRS) {
                String ak = Keys.acctKey(pr[0], pr[1], s);
                KeyState ka = acct.get(ak);
                long want = totalByAccount.get(ak).asLong();
                if (ka == null || ka.lastPos != want || ka.lastN != countBySymbol.get(s).asLong()) badAcct++;
                if (ka != null) pathSum += ka.lastPos;
                KeyState mva = mvAcct.get(ak);
                if (mva == null || mva.lastPos != want || mva.lastPrice != price || mva.lastMv != want * price
                        || mva.lastN != countBySymbol.get(s).asLong()) badMvA++;
            }
            if (pathSum != k.lastPos) badPath++;
        }
        check(badSym == 0, "final position by symbol equals the manifest for every symbol (%d differ)", badSym);
        check(badSymN == 0, "final count of orders applied by symbol equals the manifest (%d differ)", badSymN);
        check(badAcct == 0, "final position by account/sub-account/symbol equals the manifest for every key (%d differ)", badAcct);
        check(badPath == 0, "the two paths agree: account positions summed per symbol equal the symbol position (%d differ)", badPath);
        check(badMv == 0, "final market value by symbol = final position x latest price for every symbol (%d differ)", badMv);
        check(badMvA == 0, "final market value by account/sub-account/symbol = final position x latest price for every key (%d differ)", badMvA);

        System.out.println(failures == 0 ? "VERIFY PASS (" + arm + ")" : "VERIFY STOPPED: " + failures + " assertion(s) did not hold (" + arm + ")");
        System.exit(failures == 0 ? 0 : 1);
    }

    static void common(String name, Map<String, KeyState> got, Set<String> want, boolean clean) {
        Set<String> extra = new TreeSet<>(got.keySet());
        extra.removeAll(want);
        Set<String> missing = new TreeSet<>(want);
        missing.removeAll(got.keySet());
        check(missing.isEmpty() && extra.isEmpty(), "%s: distinct keys %d, predicted %d (missing %d, unexpected %d)",
                name, got.size(), want.size(), missing.size(), extra.size());
        long split = got.values().stream().filter(k -> k.splitPartitions).count();
        check(split == 0, "%s: every key in exactly one partition (%d keys in more than one)", name, split);
        long back = got.values().stream().mapToLong(k -> k.backward).sum();
        long maxBack = got.values().stream().mapToLong(k -> k.backward).max().orElse(0);
        long keysBack = got.values().stream().filter(k -> k.backward > 0).count();
        if (clean) {
            check(back == 0, "%s: no key's count of inputs applied ever goes backwards or repeats (%d steps that did not move forwards)", name, back);
        } else {
            check(maxBack <= 1, "%s: after the kill, each key goes backwards (or repeats its last record) at most once (%d keys did, most %d times)",
                    name, keysBack, maxBack);
        }
        if (!name.startsWith("market")) {
            long gaps = got.values().stream().mapToLong(k -> k.gaps).sum();
            check(gaps == 0, "%s: every position is exactly one input after the one before it, nothing skipped (%d steps skip)", name, gaps);
        }
    }

    static void check(boolean ok, String fmt, Object... args) {
        System.out.println((ok ? "  ok    " : "  WRONG ") + String.format(fmt, args));
        if (!ok) failures++;
    }

    static Map<String, KeyState> read(String bootstrap, String topic, boolean mv, boolean clean) throws Exception {
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrap);
        p.put("enable.auto.commit", "false");
        p.put("max.poll.records", "20000");
        p.put("fetch.max.bytes", String.valueOf(64 * 1024 * 1024));
        p.put("max.partition.fetch.bytes", String.valueOf(16 * 1024 * 1024));
        Map<String, KeyState> keys = new HashMap<>();
        long t0 = System.currentTimeMillis(), n = 0;
        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(p, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            List<TopicPartition> tps = new ArrayList<>();
            for (PartitionInfo pi : c.partitionsFor(topic)) tps.add(new TopicPartition(topic, pi.partition()));
            c.assign(tps);
            c.seekToBeginning(tps);
            Map<TopicPartition, Long> end = c.endOffsets(tps);
            Set<TopicPartition> open = new HashSet<>();
            for (TopicPartition tp : tps) if (end.get(tp) > c.position(tp)) open.add(tp);
            while (!open.isEmpty()) {
                ConsumerRecords<byte[], byte[]> rs = c.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<byte[], byte[]> r : rs) {
                    n++;
                    String key = new String(r.key(), StandardCharsets.UTF_8);
                    KeyState k = keys.computeIfAbsent(key, x -> new KeyState());
                    if (k.partition < 0) k.partition = r.partition();
                    else if (k.partition != r.partition()) k.splitPartitions = true;
                    long[] f = fields(r.value());   // pos, n, price, mv
                    if (k.lastN >= 0) {
                        // a replay after a restart steps back, or repeats the last
                        // record exactly (the restart resumed where the sink had got
                        // to): either way the key did not move forwards
                        if (f[1] <= k.lastN && !(mv && f[1] == k.lastN)) k.backward++;
                        else if (!mv && f[1] > k.lastN + 1) k.gaps++;
                    } else if (!mv && f[1] != 1 && clean) {
                        k.gaps++;
                    }
                    k.lastN = f[1];
                    k.lastPos = f[0];
                    k.lastPrice = f[2];
                    k.lastMv = f[3];
                    k.records++;
                }
                for (Iterator<TopicPartition> it = open.iterator(); it.hasNext(); ) {
                    TopicPartition tp = it.next();
                    if (c.position(tp) >= end.get(tp)) it.remove();
                }
            }
        }
        System.out.printf("  read %,d records off %s (%,d keys) in %.1f s%n", n, topic, keys.size(),
                (System.currentTimeMillis() - t0) / 1000.0);
        return keys;
    }

    /** pos, n, price, mv out of one output record; absent fields are 0. */
    static long[] fields(byte[] v) throws Exception {
        long[] f = new long[4];
        try (JsonParser jp = JF.createParser(v)) {
            String field = null;
            for (JsonToken t = jp.nextToken(); t != null; t = jp.nextToken()) {
                if (t == JsonToken.FIELD_NAME) field = jp.getCurrentName();
                else if (t == JsonToken.VALUE_NUMBER_INT && field != null) {
                    switch (field) {
                        case "pos": f[0] = jp.getLongValue(); break;
                        case "n": f[1] = jp.getLongValue(); break;
                        case "price": f[2] = jp.getLongValue(); break;
                        case "mv": f[3] = jp.getLongValue(); break;
                        default: break;
                    }
                }
            }
        }
        return f;
    }
}
