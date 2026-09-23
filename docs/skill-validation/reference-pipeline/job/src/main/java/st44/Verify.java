package st44;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Completeness, with no tolerances (skill section 4).
 *
 * The expected answer comes from the INPUT -- the generator's manifest -- and
 * never from the pipeline. Exits non-zero on any miss, and prints every
 * assertion so a reader can tell a check that passed from one that was never
 * made.
 *
 * Two arms, asserted differently, as section 4 requires:
 *
 *   --arm=clean    nothing may be published twice, and each location's
 *                  published hour must be strictly increasing
 *   --arm=killed   a restart replays, so a location may step back ONCE and
 *                  must then reach the same values; duplicates are allowed and
 *                  every copy must be byte-for-byte identical, which is what an
 *                  at-least-once sink made idempotent by the absolute value
 *                  looks like
 *
 * One section 4 assertion does not apply and is not silently skipped: "two
 * paths over the same input agree exactly" needs two aggregations, and this
 * pipeline has one. ASSUMPTIONS.md B9.
 */
public final class Verify {

    public static void main(String[] args) throws Exception {
        String bootstrap = Spec.arg(args, "bootstrap", null);
        String manifest = Spec.arg(args, "manifest", null);
        String topic = Spec.arg(args, "topic", null);
        String arm = Spec.arg(args, "arm", "clean");
        boolean killed = arm.equals("killed");

        Manifest m = Manifest.read(new File(manifest));
        System.out.printf("verifier: arm=%s topic=%s manifest=%s (%,d readings, %,d expected rows)%n",
                arm, topic, manifest, m.readingCount, m.expected.size());

        // every published row, in per-partition offset order
        Map<Integer, List<Row>> byPartition = new TreeMap<>();
        long[] read = {0};
        TopicReader.readAll(bootstrap, topic, r -> {
            Row row = Row.parse(r.value());
            row.partition = r.partition();
            row.offset = r.offset();
            row.key = r.key() == null ? null : new String(r.key(), StandardCharsets.UTF_8);
            byPartition.computeIfAbsent(r.partition(), k -> new ArrayList<>()).add(row);
            read[0]++;
        });

        List<String> fail = new ArrayList<>();
        List<String> pass = new ArrayList<>();

        // ---- 1. the published rows, against the manifest, exactly
        Map<String, Row> firstSeen = new LinkedHashMap<>();
        Map<String, Integer> copies = new HashMap<>();
        int wrongValue = 0, notExpected = 0, differingCopies = 0, keyMismatch = 0;
        for (List<Row> rows : byPartition.values()) {
            for (Row row : rows) {
                String id = row.location + "|" + row.hourStart;
                if (row.key != null && !row.key.equals(row.location)) {
                    keyMismatch++;
                }
                long[] want = m.expected.get(id);
                if (want == null) {
                    if (notExpected++ < 5) {
                        fail.add("published a row the input does not call for: " + row.raw);
                    }
                    continue;
                }
                if (row.totalTenths != want[0] || row.count != want[1]) {
                    if (wrongValue++ < 5) {
                        fail.add("wrong value for " + id + ": published totalC="
                                + Spec.decimal(row.totalTenths) + " count=" + row.count
                                + ", the input says totalC=" + Spec.decimal(want[0])
                                + " count=" + want[1]);
                    }
                    continue;
                }
                Row prev = firstSeen.get(id);
                if (prev == null) {
                    firstSeen.put(id, row);
                } else {
                    copies.merge(id, 1, Integer::sum);
                    if (!prev.raw.equals(row.raw) && differingCopies++ < 5) {
                        fail.add("two copies of " + id + " differ: " + prev.raw + " vs " + row.raw);
                    }
                }
            }
        }
        if (notExpected == 0 && wrongValue == 0) {
            pass.add(String.format("every one of the %,d published rows matches the input exactly "
                    + "(total in tenths of a degree, and count -- integers, no tolerance)", read[0]));
        } else {
            fail.add(notExpected + " rows the input does not call for, " + wrongValue + " wrong values");
        }
        if (keyMismatch > 0) {
            fail.add(keyMismatch + " rows whose Kafka key is not the location");
        } else {
            pass.add("every row is keyed by its location");
        }

        // ---- 2. nothing missing
        Set<String> missing = new HashSet<>(m.expected.keySet());
        missing.removeAll(firstSeen.keySet());
        if (missing.isEmpty()) {
            pass.add(String.format("all %,d hourly rows the input calls for were published "
                    + "(no window missing)", m.expected.size()));
        } else {
            List<String> some = new ArrayList<>(missing).subList(0, Math.min(5, missing.size()));
            fail.add(missing.size() + " hourly rows were never published, e.g. " + some);
        }

        // ---- 3. duplicates
        int dupRows = 0;
        for (int n : copies.values()) {
            dupRows += n;
        }
        if (!killed) {
            if (dupRows == 0) {
                pass.add("no row was published twice on a clean run");
            } else {
                fail.add(dupRows + " duplicate rows on a CLEAN run (" + copies.size() + " keys)");
            }
        } else {
            pass.add(dupRows + " duplicate rows over " + copies.size()
                    + " (location, hour) keys after the kill -- allowed, and every copy was identical, "
                    + "which is what makes the at-least-once sink idempotent");
        }

        // ---- 4. cardinality, predicted in the interview
        Set<String> locations = new HashSet<>();
        for (String id : firstSeen.keySet()) {
            locations.add(id.substring(0, id.indexOf('|')));
        }
        if (locations.size() == m.locationCount) {
            pass.add("distinct locations published = " + locations.size()
                    + ", the number the interview predicted");
        } else {
            fail.add("distinct locations published = " + locations.size()
                    + ", the interview predicted " + m.locationCount);
        }

        // ---- 5. each location in exactly one partition
        Map<String, Set<Integer>> partsOf = new HashMap<>();
        for (Map.Entry<Integer, List<Row>> e : byPartition.entrySet()) {
            for (Row row : e.getValue()) {
                partsOf.computeIfAbsent(row.location, k -> new HashSet<>()).add(e.getKey());
            }
        }
        List<String> split = new ArrayList<>();
        for (Map.Entry<String, Set<Integer>> e : partsOf.entrySet()) {
            if (e.getValue().size() > 1) {
                split.add(e.getKey() + " in " + e.getValue());
            }
        }
        if (split.isEmpty()) {
            pass.add("every location's rows are in exactly one partition, so per-location order "
                    + "means something end to end");
        } else {
            fail.add(split.size() + " locations are split across partitions: "
                    + split.subList(0, Math.min(5, split.size())));
        }

        // ---- 6. per-location order
        Map<String, Integer> backwards = new HashMap<>();
        for (List<Row> rows : byPartition.values()) {
            Map<String, String> last = new HashMap<>();
            for (Row row : rows) {
                String prev = last.get(row.location);
                if (prev != null && row.hourStart.compareTo(prev) < 0) {
                    backwards.merge(row.location, 1, Integer::sum);
                }
                last.put(row.location, row.hourStart);
            }
        }
        int worst = backwards.values().stream().mapToInt(Integer::intValue).max().orElse(0);
        if (!killed) {
            if (backwards.isEmpty()) {
                pass.add("no location's published hour ever went backwards on a clean run");
            } else {
                fail.add(backwards.size() + " locations published an hour out of order on a CLEAN run: "
                        + backwards);
            }
        } else {
            if (worst <= 1) {
                pass.add("after the kill, " + backwards.size() + " of " + locations.size()
                        + " locations stepped back exactly once and none more than once, and every "
                        + "total still matches the input");
            } else {
                fail.add("a location stepped backwards " + worst
                        + " times after one restart; at most once per location is the rule");
            }
        }

        // ---- 7. the readings accounted for
        long accounted = 0;
        for (Row row : firstSeen.values()) {
            accounted += row.count;
        }
        if (accounted == m.readingsInClosedWindows) {
            pass.add(String.format("the published rows account for %,d readings, exactly the number "
                            + "the input puts in closed hours (%,d of %,d readings; the newest hour of "
                            + "each location is still open and is not published)",
                    accounted, m.readingsInClosedWindows, m.readingCount));
        } else {
            fail.add("the published rows account for " + accounted
                    + " readings, the input says " + m.readingsInClosedWindows);
        }

        for (String s : pass) {
            System.out.println("  PASS  " + s);
        }
        for (String s : fail) {
            System.out.println("  FAIL  " + s);
        }
        System.out.println("  NOTE  section 4's \"two paths over the same input agree\" does not apply: "
                + "this pipeline has one aggregation and one path (ASSUMPTIONS.md B9)");
        if (fail.isEmpty()) {
            System.out.println("COMPLETENESS OK (" + arm + "): " + pass.size()
                    + " assertions, no tolerances");
            System.exit(0);
        }
        System.out.println("COMPLETENESS FAILED (" + arm + "): " + fail.size() + " assertions failed");
        System.exit(1);
    }

    // ----------------------------------------------------------------- helpers

    static final class Row {
        String location;
        String hourStart;
        long totalTenths;
        long count;
        String raw;
        String key;
        int partition;
        long offset;

        static Row parse(byte[] v) {
            Row r = new Row();
            r.raw = new String(v, StandardCharsets.UTF_8);
            try (JsonParser p = FACTORY.createParser(v)) {
                p.nextToken();
                while (p.nextToken() == JsonToken.FIELD_NAME) {
                    String f = p.currentName();
                    p.nextToken();
                    switch (f) {
                        case "location": r.location = p.getText(); break;
                        case "hourStart": r.hourStart = p.getText(); break;
                        case "totalC": r.totalTenths = Spec.parseTenths(p.getText()); break;
                        case "count": r.count = p.getLongValue(); break;
                        default: break;
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("unparseable published row: " + r.raw, e);
            }
            return r;
        }
    }

    static final JsonFactory FACTORY = new JsonFactory();

    static final class Manifest {
        long readingCount;
        long readingsInClosedWindows;
        int locationCount;
        /** "location|hourStart" -> {totalTenths, count} */
        Map<String, long[]> expected = new LinkedHashMap<>();

        static Manifest read(File f) throws Exception {
            Manifest m = new Manifest();
            try (JsonParser p = FACTORY.createParser(f)) {
                p.nextToken();
                while (p.nextToken() == JsonToken.FIELD_NAME) {
                    String field = p.currentName();
                    p.nextToken();
                    switch (field) {
                        case "readingCount": m.readingCount = p.getLongValue(); break;
                        case "readingsInClosedWindows": m.readingsInClosedWindows = p.getLongValue(); break;
                        case "locationCount": m.locationCount = p.getIntValue(); break;
                        case "expected":
                            while (p.nextToken() == JsonToken.START_OBJECT) {
                                String loc = null, hour = null;
                                long tenths = 0, count = 0;
                                while (p.nextToken() == JsonToken.FIELD_NAME) {
                                    String g = p.currentName();
                                    p.nextToken();
                                    switch (g) {
                                        case "location": loc = p.getText(); break;
                                        case "hourStart": hour = p.getText(); break;
                                        case "totalC": tenths = Spec.parseTenths(p.getText()); break;
                                        case "count": count = p.getLongValue(); break;
                                        default: break;
                                    }
                                }
                                m.expected.put(loc + "|" + hour, new long[]{tenths, count});
                            }
                            break;
                        default:
                            if (p.currentToken() == JsonToken.START_ARRAY) {
                                p.skipChildren();
                            }
                            break;
                    }
                }
            }
            return m;
        }
    }

    private Verify() {
    }
}
