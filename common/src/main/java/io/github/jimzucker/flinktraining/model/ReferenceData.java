package io.github.jimzucker.flinktraining.model;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * The fixed universe the demo runs against: 4 symbols, 4 accounts, one
 * sub-account each.
 *
 * <p>These counts are why the expected output is arithmetic rather than a
 * statistic: 4 symbol keys, and 4 accounts x 4 symbols = 16 account keys.
 */
public final class ReferenceData {

    /**
     * The four named symbols the demo has always used, unless SYMBOL_COUNT asks
     * for more. A larger universe exists for one purpose: comparing this
     * pipeline against another on the same key cardinality. The demo, the deck
     * and {@code verify-topics.sh} all run with the environment unset and see
     * exactly the four names and the four accounts they always did.
     */
    public static final List<String> SYMBOLS = universe("SYMBOL_COUNT",
            List.of("AAPL", "MSFT", "GOOG", "AMZN"), "SYM");
    public static final List<String> ACCOUNTS = universe("ACCOUNT_COUNT",
            List.of("ACC1", "ACC2", "ACC3", "ACC4"), "ACC");

    private static List<String> universe(String var, List<String> named, String prefix) {
        String raw = System.getenv(var);
        int want = raw == null || raw.isBlank() ? named.size() : Integer.parseInt(raw.trim());
        if (want == named.size()) {
            return named;
        }
        if (want < 1) {
            throw new IllegalArgumentException(var + " must be at least 1, got " + want);
        }
        List<String> out = new java.util.ArrayList<>(want);
        for (int i = 0; i < want; i++) {
            out.add(prefix + i);
        }
        return List.copyOf(out);
    }

    /** One sub-account per account, which is what makes the account key count 4 x 4 and not more. */
    public static final String SUB_ACCOUNT = "SUB1";

    /** Every block trade is split across all four accounts. */
    public static final int ALLOCATIONS_PER_TRADE = ACCOUNTS.size();

    /**
     * Filler fields per allocation, from FILLER_FIELDS; zero unless set.
     *
     * <p>Exists only to measure how a larger order changes throughput. Unset,
     * every order is byte-identical to what the demo has always produced.
     */
    public static final int FILLER_FIELDS = fillerFields();

    /** The value every filler field carries. */
    public static final String FILLER_VALUE = "XXXX";

    private static int fillerFields() {
        String raw = System.getenv("FILLER_FIELDS");
        int n = raw == null || raw.isBlank() ? 0 : Integer.parseInt(raw.trim());
        if (n < 0) {
            throw new IllegalArgumentException("FILLER_FIELDS must not be negative, got " + n);
        }
        return n;
    }

    /** Opening price per symbol. Round numbers so market value is easy to check by eye. */
    public static final Map<String, BigDecimal> OPENING_PRICES = openingPrices();

    private static Map<String, BigDecimal> openingPrices() {
        if (SYMBOLS.size() == 4 && SYMBOLS.get(0).equals("AAPL")) {
            return Map.of("AAPL", new BigDecimal("100.00"),
                    "MSFT", new BigDecimal("200.00"),
                    "GOOG", new BigDecimal("300.00"),
                    "AMZN", new BigDecimal("400.00"));
        }
        Map<String, BigDecimal> out = new java.util.LinkedHashMap<>();
        for (int i = 0; i < SYMBOLS.size(); i++) {
            out.put(SYMBOLS.get(i), new BigDecimal(100 * (1 + (i % 4)) + ".00"));
        }
        return Map.copyOf(out);
    }

    public static final int SYMBOL_KEY_COUNT = SYMBOLS.size();

    /** 4 accounts x 4 symbols. */
    public static final int ACCOUNT_KEY_COUNT = ACCOUNTS.size() * SYMBOLS.size();

    private ReferenceData() {
    }
}
