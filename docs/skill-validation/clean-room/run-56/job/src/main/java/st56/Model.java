package st56;

import java.util.SplittableRandom;

/**
 * The business case's fixed shape, shared by the generator, the job and the
 * verifier. No Flink classes here: the generator and verifier run on the host
 * with only this jar on the classpath.
 */
public final class Model {
    private Model() {}

    public static final int SYMBOLS = 4096;
    public static final String[] ACCOUNTS = {"ACC0", "ACC1"};
    public static final String[] SUBS = {"SUB0", "SUB1"};
    /** Every order has exactly one allocation per account / sub-account pair. */
    public static final int ALLOCATIONS = ACCOUNTS.length * SUBS.length;

    /** Share of order slots that are a repeat of the order before them in the same partition. */
    public static final double REPEAT_SHARE = 0.005;

    public static final int PRICE_TIMESTAMPS = 4;
    public static final long PRICE_SEED = 7_777_777L;
    public static final long PRICE_TS0 = 1_700_000_000_000L;

    public static String symbol(int idx) {
        return String.format("SYM%04d", idx);
    }

    public static String accountKey(String account, String sub, String symbol) {
        return account + "/" + sub + "/" + symbol;
    }

    /** The input partition a symbol (and every key built on it) lives in. */
    public static int partitionOf(int symbolIdx, int partitions) {
        return symbolIdx % partitions;
    }

    static long mix(long z) {
        z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
        z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
        return z ^ (z >>> 31);
    }

    /** Is order slot (partition p, local position j) a repeat of the slot before it? */
    public static boolean isRepeatSlot(long seed, int p, long j) {
        if (j == 0) return false;
        long h = mix(seed * 31 + p * 1_000_003L + j * 7919L + 0x5eed);
        return (h >>> 11) * 0x1.0p-53 < REPEAT_SHARE;
    }

    /** One order, fully determined by the seed and its slot index. */
    public static final class Order {
        public long id;
        public int symbolIdx;
        public long qty;
        public long ts;
        public final long[] alloc = new long[ALLOCATIONS];

        public String json() {
            StringBuilder b = new StringBuilder(256);
            String sym = symbol(symbolIdx);
            b.append("{\"id\":").append(id).append(",\"symbol\":\"").append(sym)
             .append("\",\"qty\":").append(qty).append(",\"ts\":").append(ts).append(",\"allocations\":[");
            int k = 0;
            for (String a : ACCOUNTS) {
                for (String s : SUBS) {
                    if (k > 0) b.append(',');
                    b.append("{\"account\":\"").append(a).append("\",\"sub\":\"").append(s)
                     .append("\",\"qty\":").append(alloc[k]).append('}');
                    k++;
                }
            }
            return b.append("]}").toString();
        }
    }

    /**
     * The order in global slot i (= j * partitions + p). Its symbol always maps
     * to partition p, and its id is i, so ids rise within each partition.
     */
    public static void order(long seed, long i, int p, int partitions, Order o) {
        SplittableRandom r = new SplittableRandom(mix(seed ^ mix(i + 0x9E3779B97F4A7C15L)));
        int perPart = SYMBOLS / partitions;
        o.id = i;
        o.symbolIdx = p + partitions * r.nextInt(perPart);
        long q = 1 + r.nextInt(1000);
        boolean sell = r.nextInt(4) == 0;
        // four non-negative parts that sum to q
        long c1 = r.nextLong(q + 1), c2 = r.nextLong(q + 1), c3 = r.nextLong(q + 1);
        long a = Math.min(c1, Math.min(c2, c3)), cc = Math.max(c1, Math.max(c2, c3));
        long b = c1 + c2 + c3 - a - cc;
        long[] parts = {a, b - a, cc - b, q - cc};
        for (int k = 0; k < ALLOCATIONS; k++) o.alloc[k] = sell ? -parts[k] : parts[k];
        o.qty = sell ? -q : q;
        o.ts = 1_760_000_000_000L + i;
    }

    /** Price in cents for symbol s at timestamp index k. Fixed: depends on nothing the run passes. */
    public static long priceCents(int s, int k) {
        SplittableRandom r = new SplittableRandom(mix(PRICE_SEED * 131 + s * 17L + k));
        return 1_000 + r.nextInt(99_000);
    }

    public static long priceTs(int s, int k) {
        return PRICE_TS0 + k * 60_000L + s;
    }

    public static long latestPriceCents(int s) {
        return priceCents(s, PRICE_TIMESTAMPS - 1);
    }
}
