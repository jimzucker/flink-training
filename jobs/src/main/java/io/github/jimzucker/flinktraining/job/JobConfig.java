package io.github.jimzucker.flinktraining.job;

/** Everything the jobs can be pointed at, in one place. */
public record JobConfig(
        String bootstrapServers,
        String ordersTopic,
        String positionsBySymbolTopic,
        String positionsByAccountTopic,
        String consumerGroup,
        int parallelism,
        long checkpointIntervalMillis,
        long logEvery) {

    public static final String DEFAULT_BOOTSTRAP = "kafka:19092";
    public static final String ORDERS = "orders";
    public static final String POSITIONS_BY_SYMBOL = "positions-by-symbol";
    public static final String POSITIONS_BY_ACCOUNT = "positions-by-account";

    public static JobConfig fromEnvironment() {
        return new JobConfig(
                env("BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP),
                env("ORDERS_TOPIC", ORDERS),
                env("POSITIONS_BY_SYMBOL_TOPIC", POSITIONS_BY_SYMBOL),
                env("POSITIONS_BY_ACCOUNT_TOPIC", POSITIONS_BY_ACCOUNT),
                env("CONSUMER_GROUP", "positions-job"),
                envInt("PARALLELISM", 2),
                envLong("CHECKPOINT_INTERVAL_MS", 10_000L),
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
