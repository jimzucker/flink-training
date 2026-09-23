package st44;

/**
 * The SECOND vantage point (skill section 5).
 *
 * This pipeline's output is per window: one row per location per hour, however
 * many readings arrived in that hour. There is no constant fan-out anywhere in
 * the job, so sink rows divided by a constant is not available and would be a
 * lie about the job if the data happened to make one up.
 *
 * What the pipeline already publishes is the COUNT of readings behind each row.
 * Summed over the topic that is exactly how much input the pipeline's own
 * output accounts for -- exact, monotonic within a case, needing no arithmetic
 * against the manifest, and indifferent to rows left behind by an earlier case
 * because the harness uses the DIFFERENCE across a measurement window.
 *
 * It moves one closed hour at a time. ASSUMPTIONS.md B4 staggers the 100
 * locations across the simulated hour so that step is 36,000 readings rather
 * than 3,600,000.
 *
 * Prints exactly one JSON object: {"inputRecordsProcessed": N}
 */
public final class Progress {

    private static final byte[] COUNT_FIELD = ",\"count\":".getBytes(java.nio.charset.StandardCharsets.US_ASCII);

    public static void main(String[] args) {
        String bootstrap = Spec.arg(args, "bootstrap", null);
        String topic = Spec.arg(args, "topic", null);
        long[] total = {0};
        long[] rows = {0};
        TopicReader.readAll(bootstrap, topic, r -> {
            total[0] += countOf(r.value());
            rows[0]++;
        });
        System.out.println("{\"inputRecordsProcessed\": " + total[0] + ", \"rows\": " + rows[0] + "}");
    }

    /** The value of the "count" field, read straight out of the bytes. */
    static long countOf(byte[] v) {
        int at = indexOf(v, COUNT_FIELD);
        if (at < 0) {
            throw new IllegalArgumentException("no count field in: "
                    + new String(v, java.nio.charset.StandardCharsets.UTF_8));
        }
        int i = at + COUNT_FIELD.length;
        long n = 0;
        boolean any = false;
        while (i < v.length && v[i] >= '0' && v[i] <= '9') {
            n = n * 10 + (v[i] - '0');
            any = true;
            i++;
        }
        if (!any) {
            throw new IllegalArgumentException("count is not a number in: "
                    + new String(v, java.nio.charset.StandardCharsets.UTF_8));
        }
        return n;
    }

    private static int indexOf(byte[] hay, byte[] needle) {
        outer:
        for (int i = 0; i + needle.length <= hay.length; i++) {
            for (int j = 0; j < needle.length; j++) {
                if (hay[i + j] != needle[j]) {
                    continue outer;
                }
            }
            return i;
        }
        return -1;
    }

    private Progress() {
    }
}
