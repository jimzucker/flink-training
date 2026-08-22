package io.github.jimzucker.flinktraining.job;

/** Everything the jobs can be pointed at, in one place. */
public record JobConfig(
        String bootstrapServers,
        String ordersTopic,
        String positionsBySymbolTopic,
        String positionsByAccountTopic,
        String pricesTopic,
        String mvBySymbolTopic,
        String mvByAccountTopic,
        String consumerGroup,
        int parallelism,
        long checkpointIntervalMillis,
        long transactionTimeoutMillis,
        long windowMillis,
        long idlenessMillis,
        long outOfOrdernessMillis,
        long logEvery) {

    public static final String DEFAULT_BOOTSTRAP = "kafka:19092";
    public static final String ORDERS = "orders";
    public static final String POSITIONS_BY_SYMBOL = "positions-by-symbol";
    public static final String POSITIONS_BY_ACCOUNT = "positions-by-account";
    public static final String PRICES = "prices";
    public static final String MV_BY_SYMBOL = "mv-by-symbol";
    public static final String MV_BY_ACCOUNT = "mv-by-account";

    /** One minute, as the requirements specify. */
    public static final long DEFAULT_WINDOW_MS = 60_000L;

    /**
     * How long an input may be silent before it stops holding the watermark
     * back. Without this a quiet partition stalls the windows and the market
     * value sinks go silent while every record that did arrive was in order.
     */
    public static final long DEFAULT_IDLENESS_MS = 5_000L;

    /**
     * How often the sinks become visible. Under exactly-once a record is not
     * readable until the checkpoint that produced it commits, so this is also the
     * granularity at which the dashboard advances and a floor under end-to-end
     * latency.
     */
    public static final long DEFAULT_CHECKPOINT_INTERVAL_MS = 5_000L;

    /**
     * Must outlast a checkpoint, or a transaction expires before it can commit,
     * and must stay under the broker's transaction.max.timeout.ms of 15 minutes.
     */
    public static final long DEFAULT_TRANSACTION_TIMEOUT_MS = 300_000L;

    /**
     * How far out of order the position streams may be.
     *
     * <p>Orders are keyed by trade id and spread across partitions, so Part 1
     * merges trades for one symbol from several partitions and the position
     * timestamps it emits are not monotonic. This is the allowance for that; it
     * delays a window closing by the same amount.
     */
    public static final long DEFAULT_OUT_OF_ORDERNESS_MS = 2_000L;

    /** The demo raises this from 2 to 4 to show throughput scaling with it. */
    public static final int DEFAULT_PARALLELISM = 2;

    public static JobConfig fromEnvironment() {
        return new JobConfig(
                env("BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP),
                env("ORDERS_TOPIC", ORDERS),
                env("POSITIONS_BY_SYMBOL_TOPIC", POSITIONS_BY_SYMBOL),
                env("POSITIONS_BY_ACCOUNT_TOPIC", POSITIONS_BY_ACCOUNT),
                env("PRICES_TOPIC", PRICES),
                env("MV_BY_SYMBOL_TOPIC", MV_BY_SYMBOL),
                env("MV_BY_ACCOUNT_TOPIC", MV_BY_ACCOUNT),
                env("CONSUMER_GROUP", "positions-job"),
                envInt("PARALLELISM", DEFAULT_PARALLELISM),
                envLong("CHECKPOINT_INTERVAL_MS", DEFAULT_CHECKPOINT_INTERVAL_MS),
                envLong("TRANSACTION_TIMEOUT_MS", DEFAULT_TRANSACTION_TIMEOUT_MS),
                envLong("WINDOW_MS", DEFAULT_WINDOW_MS),
                envLong("IDLENESS_MS", DEFAULT_IDLENESS_MS),
                envLong("OUT_OF_ORDERNESS_MS", DEFAULT_OUT_OF_ORDERNESS_MS),
                envLong("LOG_EVERY", 50L));
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
