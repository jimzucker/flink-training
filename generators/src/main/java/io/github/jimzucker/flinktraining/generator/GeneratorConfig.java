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
        long startEpochMillis,
        long durationSeconds,
        long maxTrades,
        long maxPrices,
        long tradeEventTimeStepMillis,
        long priceEventTimeStepMillis) {

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

    public GeneratorConfig {
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
                envLong("START_EPOCH_MILLIS", WALL_CLOCK),
                envLong("DURATION_SECONDS", 0L),
                envLong("MAX_TRADES", 0L),
                envLong("MAX_PRICES", 0L),
                envLong("TRADE_EVENT_TIME_STEP_MS", 0L),
                envLong("PRICE_EVENT_TIME_STEP_MS", 0L));
    }

    public long tradeIntervalMillis() {
        return Math.max(1L, 1_000L / tradesPerSecond);
    }

    /**
     * How much event time each record advances in replay mode, independent of
     * how fast records are actually emitted.
     *
     * <p>Without this the two are the same number, so covering several minutes of
     * event time takes several minutes of real time. Windowed behaviour can then
     * only be tested by waiting, which makes the test slow enough that it stops
     * being run. Setting a step lets a run cover minutes of event time in
     * seconds. Zero keeps the default, where a step equals the emission interval.
     */
    public long tradeEventTimeStep() {
        return tradeEventTimeStepMillis > 0 ? tradeEventTimeStepMillis : tradeIntervalMillis();
    }

    public long priceEventTimeStep() {
        return priceEventTimeStepMillis > 0 ? priceEventTimeStepMillis : priceIntervalMillis();
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
