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
        int pricesPerSecond,
        long seed,
        int generatorThreads,
        long startEpochMillis,
        long durationSeconds,
        long maxTrades,
        long maxPrices) {

    public static final String DEFAULT_BOOTSTRAP = "localhost:9092";
    public static final String ORDERS_TOPIC = "orders";
    public static final String PRICES_TOPIC = "prices";

    /** The rate the expected-output table is written against. */
    public static final int DEMO_TRADES_PER_SECOND = 10;
    public static final int DEMO_PRICES_PER_SECOND = 1_000;

    /**
     * Wall-clock event times, which is what makes end-to-end latency measurable:
     * a record's age at a sink is the sink's clock minus the instant the record
     * was created.
     *
     * <p>Setting {@code START_EPOCH_MILLIS} switches to replay instead, where
     * event times come from a counter and two runs are byte-identical. Replay is
     * how reproducibility is proved; wall clock is how the demo is run.
     */
    public static final long WALL_CLOCK = 0L;
    public static final long REPLAY_START_EPOCH_MILLIS = 1_700_000_000_000L;
    public static final long DEFAULT_SEED = 42L;

    /**
     * Threads producing trades. One by default.
     *
     * <p>A single thread serialising JSON tops out near 300,000 orders/sec while
     * the broker accepts 750,000, so the generator, not Kafka, is what limits
     * offered load. More threads close that gap; the count must divide the orders
     * topic's partition count, so that each partition has exactly one writer and
     * replay stays byte-identical.
     */
    public static final int DEFAULT_GENERATOR_THREADS = 1;

    public GeneratorConfig {
        if (generatorThreads <= 0) {
            throw new IllegalArgumentException("generatorThreads must be positive");
        }
        if (tradesPerSecond <= 0) {
            throw new IllegalArgumentException("tradesPerSecond must be positive");
        }
        if (pricesPerSecond <= 0) {
            throw new IllegalArgumentException("pricesPerSecond must be positive");
        }
    }

    public static GeneratorConfig fromEnvironment() {
        return new GeneratorConfig(
                env("BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP),
                env("ORDERS_TOPIC", ORDERS_TOPIC),
                env("PRICES_TOPIC", PRICES_TOPIC),
                envInt("TRADES_PER_SECOND", DEMO_TRADES_PER_SECOND),
                envInt("PRICES_PER_SECOND", DEMO_PRICES_PER_SECOND),
                envLong("SEED", DEFAULT_SEED),
                envInt("GENERATOR_THREADS", DEFAULT_GENERATOR_THREADS),
                envLong("START_EPOCH_MILLIS", WALL_CLOCK),
                envLong("DURATION_SECONDS", 0L),
                envLong("MAX_TRADES", 0L),
                envLong("MAX_PRICES", 0L));
    }

    public long tradeIntervalMillis() {
        return Math.max(1L, 1_000L / tradesPerSecond);
    }


    /** True when event times come from the wall clock rather than a replay counter. */
    public boolean isLive() {
        return startEpochMillis == WALL_CLOCK;
    }

    public long priceIntervalMillis() {
        return Math.max(1L, 1_000L / pricesPerSecond);
    }

    /** Zero means run until stopped. */
    public boolean runsForever() {
        return durationSeconds <= 0;
    }

    /**
     * Whether a stream has emitted everything it was asked for.
     *
     * <p>Bounding a run by record count rather than by elapsed time is what makes
     * a reproducibility demo exact: "emit 100 trades" always emits 100, whereas
     * "run for ten seconds" lands on 100 or 101 depending on where the clock
     * falls. Zero means unbounded.
     */
    public static boolean reached(long limit, long emitted) {
        return limit > 0 && emitted >= limit;
    }

    /** Both streams have a record limit, so the run ends when they are met. */
    public boolean isRecordBounded() {
        return maxTrades > 0 && maxPrices > 0;
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
