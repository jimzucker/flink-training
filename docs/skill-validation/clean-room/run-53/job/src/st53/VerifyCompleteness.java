package st53;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
 * Reads all four outputs to their end and checks them against the generator's manifest, with no
 * tolerance. Exit 0 only when every check holds.
 *
 *   --bootstrap=host:port --topicSym --topicAcct --mvSym --mvAcct --manifest=path --arm=clean|killed
 *
 * On the clean arm no key may ever go backwards; on the killed arm a key may go backwards once
 * (the replay after the restart) and must then reach the same final value (SKILL.md section 4).
 */
public final class VerifyCompleteness {

    static final JsonFactory JSON = new JsonFactory();
    static final List<String> failures = new ArrayList<>();
    static final List<String> passes = new ArrayList<>();

    /** What we saw for one key on one topic. */
    static final class Seen {
        int partition = -1;
        boolean splitAcrossPartitions;
        long records, lastOrders = -1, backwards, position, orders, price, priceTs, marketValue;
    }

    public static void main(String[] argv) throws Exception {
        Args a = new Args(argv);
        String arm = a.get("arm", "clean");
        long maxBackwards = arm.equals("killed") ? 1 : 0;
        Map<String, Object> m = readManifest(a.get("manifest"));
        @SuppressWarnings("unchecked") Map<String, long[]> symWant = (Map<String, long[]>) m.get("positionBySymbol");
        @SuppressWarnings("unchecked") Map<String, long[]> accWant = (Map<String, long[]>) m.get("positionByAccount");
        @SuppressWarnings("unchecked") Map<String, long[]> priceWant = (Map<String, long[]>) m.get("latestPrice");
        long uniques = (Long) m.get("uniqueOrders");
        long repeats = (Long) m.get("duplicateRecords");
        String boot = a.get("bootstrap");

        Map<String, Seen> sym = read(boot, a.get("topicSym"), false);
        Map<String, Seen> acc = read(boot, a.get("topicAcct"), false);
        Map<String, Seen> mvSym = read(boot, a.get("mvSym"), true);
        Map<String, Seen> mvAcc = read(boot, a.get("mvAcct"), true);

        check(a.get("topicSym"), sym, symWant, priceWant, false, maxBackwards, Model.SYMBOLS);
        check(a.get("topicAcct"), acc, accWant, priceWant, false, maxBackwards, Model.SYMBOLS * Model.ALLOCS);
        check(a.get("mvSym"), mvSym, symWant, priceWant, true, maxBackwards, Model.SYMBOLS);
        check(a.get("mvAcct"), mvAcc, accWant, priceWant, true, maxBackwards, Model.SYMBOLS * Model.ALLOCS);

        // two paths over the same input: the four account positions of a symbol add up to its symbol position
        long mismatched = 0;
        Map<String, Long> sumBySymbol = new HashMap<>();
        for (Map.Entry<String, Seen> e : acc.entrySet()) {
            sumBySymbol.merge(e.getKey().substring(6), e.getValue().position, Long::sum);
        }
        for (Map.Entry<String, Seen> e : sym.entrySet()) {
            if (sumBySymbol.getOrDefault(e.getKey(), Long.MIN_VALUE) != e.getValue().position) mismatched++;
        }
        result(mismatched == 0 && sumBySymbol.size() == sym.size(),
                "the two paths agree: account positions summed per symbol equal the symbol position for "
                        + sym.size() + " symbols (" + mismatched + " disagree)");

        // every order counted once, repeats included in the input on purpose
        long symRecords = sym.values().stream().mapToLong(s -> s.records).sum();
        long accRecords = acc.values().stream().mapToLong(s -> s.records).sum();
        if (arm.equals("clean")) {
            result(symRecords == uniques && accRecords == uniques * Model.ALLOCS,
                    String.format("one position per order and one per allocation: %,d and %,d records for %,d orders "
                            + "(the input held %,d of them twice)", symRecords, accRecords, uniques, repeats));
        } else {
            result(symRecords >= uniques && accRecords >= uniques * Model.ALLOCS,
                    String.format("at least one position per order and per allocation after the restart: %,d and %,d "
                            + "records for %,d orders (a replay re-publishes some)", symRecords, accRecords, uniques));
        }
        result(repeats > 0, String.format("the input repeated %,d records on purpose and every total above counts "
                + "each order once", repeats));

        System.out.println("arm: " + arm);
        for (String p : passes) System.out.println("PASS  " + p);
        for (String f : failures) System.out.println("FAIL  " + f);
        System.out.printf("{\"arm\":\"%s\",\"checks\":%d,\"failed\":%d,\"symbolRecords\":%d,\"accountRecords\":%d,"
                        + "\"mvSymbolRecords\":%d,\"mvAccountRecords\":%d}%n", arm, passes.size() + failures.size(),
                failures.size(), symRecords, accRecords,
                mvSym.values().stream().mapToLong(s -> s.records).sum(),
                mvAcc.values().stream().mapToLong(s -> s.records).sum());
        System.exit(failures.isEmpty() ? 0 : 1);
    }

    static void result(boolean ok, String what) {
        (ok ? passes : failures).add(what);
    }

    static void check(String topic, Map<String, Seen> got, Map<String, long[]> want, Map<String, long[]> prices,
                      boolean mv, long maxBackwards, int predictedKeys) {
        result(got.size() == predictedKeys, String.format("%s: %,d distinct keys, %,d predicted", topic, got.size(), predictedKeys));
        long split = 0, back = 0, wrong = 0, missing = 0;
        String firstWrong = null;
        for (Map.Entry<String, long[]> e : want.entrySet()) {
            long[] w = e.getValue();
            Seen s = got.get(e.getKey());
            if (w[1] == 0) { if (s != null) { wrong++; } continue; }
            if (s == null) { missing++; continue; }
            if (s.splitAcrossPartitions) split++;
            if (s.backwards > maxBackwards) back++;
            boolean ok = s.position == w[0] && s.orders == w[1];
            if (mv) {
                long[] p = prices.get(e.getKey().length() > 7 ? e.getKey().substring(6) : e.getKey());
                ok = ok && s.priceTs == p[0] && s.price == p[1] && s.marketValue == w[0] * p[1];
            }
            if (!ok) {
                wrong++;
                if (firstWrong == null) firstWrong = e.getKey() + " published position " + s.position + " orders " + s.orders
                        + (mv ? " price " + s.price + " at " + s.priceTs + " value " + s.marketValue : "")
                        + ", expected " + w[0] + " / " + w[1];
            }
        }
        for (String k : got.keySet()) if (!want.containsKey(k)) wrong++;
        result(split == 0, topic + ": every key is in exactly one partition (" + split + " split)");
        result(back == 0, topic + ": no key goes backwards more than " + maxBackwards + " time(s) (" + back + " did)");
        result(wrong == 0 && missing == 0, topic + ": every key's last value matches the manifest"
                + (mv ? " as final position x latest price" : "") + " (" + wrong + " wrong, " + missing + " missing"
                + (firstWrong == null ? "" : "; first: " + firstWrong) + ")");
    }

    static Map<String, Seen> read(String boot, String topic, boolean mv) throws Exception {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boot);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 20000);
        p.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 64 << 20);
        p.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 16 << 20);
        Map<String, Seen> out = new HashMap<>();
        long t0 = System.currentTimeMillis(), n = 0;
        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(p)) {
            List<TopicPartition> tps = new ArrayList<>();
            c.partitionsFor(topic).forEach(pi -> tps.add(new TopicPartition(topic, pi.partition())));
            c.assign(tps);
            c.seekToBeginning(tps);
            Map<TopicPartition, Long> end = c.endOffsets(tps);
            while (true) {
                boolean done = true;
                for (TopicPartition tp : tps) if (c.position(tp) < end.get(tp)) { done = false; break; }
                if (done) break;
                ConsumerRecords<byte[], byte[]> rs = c.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<byte[], byte[]> r : rs) {
                    String key = new String(r.key(), StandardCharsets.UTF_8);
                    Seen s = out.computeIfAbsent(key, k -> new Seen());
                    if (s.partition < 0) s.partition = r.partition();
                    else if (s.partition != r.partition()) s.splitAcrossPartitions = true;
                    parse(r.value(), s, mv);
                    n++;
                }
            }
        }
        System.err.printf("read %,d records from %s in %.1f s%n", n, topic, (System.currentTimeMillis() - t0) / 1000.0);
        return out;
    }

    static void parse(byte[] v, Seen s, boolean mv) throws Exception {
        long position = 0, orders = -1, price = 0, priceTs = 0, value = 0;
        try (JsonParser jp = JSON.createParser(v)) {
            jp.nextToken();
            while (jp.nextToken() == JsonToken.FIELD_NAME) {
                String f = jp.getCurrentName();
                jp.nextToken();
                switch (f) {
                    case "position": position = jp.getLongValue(); break;
                    case "orders": orders = jp.getLongValue(); break;
                    case "price": price = jp.getLongValue(); break;
                    case "priceTs": priceTs = jp.getLongValue(); break;
                    case "marketValue": value = jp.getLongValue(); break;
                    default: jp.skipChildren();
                }
            }
        }
        if (orders < s.lastOrders) s.backwards++;
        s.lastOrders = orders;
        s.records++;
        s.position = position; s.orders = orders;
        if (mv) { s.price = price; s.priceTs = priceTs; s.marketValue = value; }
    }

    /** The manifest: a few scalars and three key tables of [a, b] pairs. */
    static Map<String, Object> readManifest(String path) throws Exception {
        Map<String, Object> m = new HashMap<>();
        try (JsonParser jp = JSON.createParser(new File(path))) {
            jp.nextToken();
            while (jp.nextToken() == JsonToken.FIELD_NAME) {
                String f = jp.getCurrentName();
                JsonToken t = jp.nextToken();
                if (t == JsonToken.START_OBJECT) {
                    Map<String, long[]> tab = new HashMap<>();
                    while (jp.nextToken() == JsonToken.FIELD_NAME) {
                        String k = jp.getCurrentName();
                        jp.nextToken();
                        jp.nextToken(); long x = jp.getLongValue();
                        jp.nextToken(); long y = jp.getLongValue();
                        jp.nextToken();
                        tab.put(k, new long[]{x, y});
                    }
                    m.put(f, tab);
                } else if (t == JsonToken.VALUE_NUMBER_INT) {
                    m.put(f, jp.getLongValue());
                } else {
                    jp.skipChildren();
                }
            }
        }
        return m;
    }
}
