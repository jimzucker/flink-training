package st53;

/**
 * The business case's fixed shape (ASSUMPTIONS.md A1-A6): 4,096 symbols, accounts A1/A2,
 * sub-accounts S1/S2, every order allocated to all four account/sub-account pairs, three
 * prices per symbol. Shared by the generator and the verifier; the job reads none of it --
 * it learns keys from the data.
 */
public final class Model {
    public static final int SYMBOLS = 4096;
    public static final String[] ACCOUNTS = {"A1", "A2"};
    public static final String[] SUBS = {"S1", "S2"};
    public static final int ALLOCS = ACCOUNTS.length * SUBS.length;   // 4, so 5 outputs per order
    public static final int PRICES_PER_SYMBOL = 3;
    public static final long PRICE_SEED = 20261011L;                  // fixed: every backlog sees the same prices
    public static final long PRICE_BASE_TS = 1_760_000_000_000L;
    /** One order in DUP_EVERY is written twice, as a retrying producer would (section 4): 0.5%. */
    public static final int DUP_EVERY = 200;

    private Model() {}

    public static String symbol(int i) {
        return String.format("SYM%04d", i);
    }

    public static String accountKey(int a, int s, String symbol) {
        return ACCOUNTS[a] + "|" + SUBS[s] + "|" + symbol;
    }

    /** SplitMix64 finaliser: a stateless, deterministic hash of (seed, index). */
    public static long mix(long seed, long i) {
        long z = seed + (i + 1) * 0x9E3779B97F4A7C15L;
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }

    public static boolean isDup(long seed, long u) {
        return Long.remainderUnsigned(mix(seed ^ 0x5DEECE66DL, u), DUP_EVERY) == 0;
    }

    /** Order u: symbol index, then the four signed allocation quantities. Order qty is their sum. */
    public static void order(long seed, long u, int[] symOut, long[] allocOut) {
        long r = mix(seed, u);
        symOut[0] = (int) Long.remainderUnsigned(r, SYMBOLS);
        boolean sell = ((r >>> 40) & 1L) == 1L;
        long r2 = mix(seed ^ 0x2545F4914F6CDD1DL, u);
        for (int k = 0; k < ALLOCS; k++) {
            long q = 1 + ((r2 >>> (k * 16)) & 0xFFFFL) % 250;
            allocOut[k] = sell ? -q : q;
        }
    }

    /** Price k of symbol s: (timestamp, cents). Timestamps differ by construction (k is the residue mod 3). */
    public static long[] price(int s, int k) {
        long r = mix(PRICE_SEED, (long) s * PRICES_PER_SYMBOL + k);
        long ts = PRICE_BASE_TS + Long.remainderUnsigned(r, 28_800_000L) * PRICES_PER_SYMBOL + k;
        long cents = 1_000 + Long.remainderUnsigned(r >>> 17, 99_000L);
        return new long[]{ts, cents};
    }

    /** The newest of a symbol's prices, by timestamp (ASSUMPTIONS.md A5). */
    public static long[] latestPrice(int s) {
        long[] best = null;
        for (int k = 0; k < PRICES_PER_SYMBOL; k++) {
            long[] p = price(s, k);
            if (best == null || p[0] > best[0]) best = p;
        }
        return best;
    }
}
