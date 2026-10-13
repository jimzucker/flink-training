package st54;

/** The business shape, shared by the generator, the job and the verifier. */
public final class Model {
    private Model() {}

    public static final int SYMBOLS = 4096;
    public static final String[] ACCOUNTS = {"ACC1", "ACC2"};
    public static final String[] SUBS = {"SUB1", "SUB2"};
    /** every order allocates to every account / sub-account pair: 2 x 2 = 4 */
    public static final int ALLOCS = ACCOUNTS.length * SUBS.length;
    /** prices per symbol, rising timestamps; the latest is the highest timestamp */
    public static final int PRICES_PER_SYMBOL = 3;
    public static final long PRICE_TS0 = 1_760_000_000_000L;
    /** fixed, so every fill writes the same prices whatever the backlog seed */
    public static final long PRICE_SEED = 777L;
    /** order id = partition << 40 | sequence within the partition */
    public static final int PARTITION_SHIFT = 40;

    private static final String[] SYMBOL_NAMES = new String[SYMBOLS];
    static {
        for (int i = 0; i < SYMBOLS; i++) {
            SYMBOL_NAMES[i] = String.format("SYM%04d", i);
        }
    }

    public static String symbol(int i) {
        return SYMBOL_NAMES[i];
    }

    public static String accountKey(String account, String sub, String symbol) {
        return account + "/" + sub + "/" + symbol;
    }

    /** the symbol in either kind of key: the whole key, or what follows the last '/' */
    public static String symbolOfKey(String key) {
        int i = key.lastIndexOf('/');
        return i < 0 ? key : key.substring(i + 1);
    }

    /** the price of symbol s at its k-th timestamp, in cents. Deterministic. */
    public static long priceCents(int s, int k) {
        java.util.SplittableRandom r = new java.util.SplittableRandom(PRICE_SEED * 1_000_003L + s * 31L + k);
        return 1_000 + r.nextInt(99_000);   // $10.00 - $999.99
    }

    public static long priceTs(int k) {
        return PRICE_TS0 + k * 1000L;
    }
}
