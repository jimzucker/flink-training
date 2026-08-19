package io.github.jimzucker.flinktraining.generator;

/**
 * Everything the demo can be run with, in one place.
 *
 * <p>Rates are the knobs the scale cases turn: the assignment asks for orders at
 * 1000/sec, and separately for a very high price rate, each without hurting the
 * other.
 */
public record GeneratorConfig(
        String bootstrapServers,
        String ordersTopic,
        String pricesTopic,
        int tradesPerSecond,
        int priceTicksPerSecond,
        long seed,
        long startEpochMillis,
        long durationSeconds) {

    public static final String DEFAULT_BOOTSTRAP = "localhost:9092";
    public static final String ORDERS_TOPIC = "orders";
    public static final String PRICES_TOPIC = "prices";

    /** The rate the expected-output table is written against. */
    public static final int DEMO_TRADES_PER_SECOND = 10;
    public static final int DEMO_PRICE_TICKS_PER_SECOND = 1;

    /** A fixed instant, so event times are identical across runs rather than wall-clock dependent. */
    public static final long DEFAULT_START_EPOCH_MILLIS = 1_700_000_000_000L;
    public static final long DEFAULT_SEED = 42L;

    public GeneratorConfig {
        if (tradesPerSecond <= 0) {
            throw new IllegalArgumentException("tradesPerSecond must be positive");
        }
        if (priceTicksPerSecond <= 0) {
            throw new IllegalArgumentException("priceTicksPerSecond must be positive");
        }
    }

    public static GeneratorConfig fromEnvironment() {
        return new GeneratorConfig(
                env("BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP),
                env("ORDERS_TOPIC", ORDERS_TOPIC),
                env("PRICES_TOPIC", PRICES_TOPIC),
                envInt("TRADES_PER_SECOND", DEMO_TRADES_PER_SECOND),
                envInt("PRICE_TICKS_PER_SECOND", DEMO_PRICE_TICKS_PER_SECOND),
                envLong("SEED", DEFAULT_SEED),
                envLong("START_EPOCH_MILLIS", DEFAULT_START_EPOCH_MILLIS),
                envLong("DURATION_SECONDS", 0L));
    }

    public long tradeIntervalMillis() {
        return 1_000L / tradesPerSecond;
    }

    public long priceIntervalMillis() {
        return 1_000L / priceTicksPerSecond;
    }

    /** Zero means run until stopped. */
    public boolean runsForever() {
        return durationSeconds <= 0;
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static int envInt(String name, int fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : Integer.parseInt(value.trim());
    }

    private static long envLong(String name, long fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : Long.parseLong(value.trim());
    }
}
