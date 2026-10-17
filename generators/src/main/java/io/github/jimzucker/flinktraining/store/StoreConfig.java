package io.github.jimzucker.flinktraining.store;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Everything the store loader can be pointed at, read from the environment the
 * way the generators are.
 *
 * <p>{@code topics} maps each output topic to the prefix its keys carry in
 * Redis, so {@code positions-by-symbol} key {@code AAPL} becomes
 * {@code pos:sym:AAPL}. The colon is what RedisInsight folds a key list on.
 */
public record StoreConfig(
        String bootstrapServers,
        Map<String, String> topics,
        String groupId,
        String redisHost,
        int redisPort,
        int threads,
        int maxPollRecords,
        int metricsPort) {

    public static final String DEFAULT_TOPICS =
            "positions-by-symbol=pos:sym,positions-by-account=pos:acct,"
                    + "mv-by-symbol=mv:sym,mv-by-account=mv:acct";

    public static StoreConfig fromEnvironment() {
        return new StoreConfig(
                env("BOOTSTRAP_SERVERS", "kafka:19092"),
                parseTopics(env("STORE_TOPICS", DEFAULT_TOPICS)),
                env("STORE_GROUP", "redis-store"),
                env("REDIS_HOST", "redis"),
                Integer.parseInt(env("REDIS_PORT", "6379")),
                // Each thread is one consumer in the group, so threads past the
                // partition count of the busiest topic sit idle.
                Integer.parseInt(env("LOADER_THREADS", "4")),
                Integer.parseInt(env("MAX_POLL_RECORDS", "20000")),
                Integer.parseInt(env("METRICS_PORT", "9400")));
    }

    static Map<String, String> parseTopics(String spec) {
        Map<String, String> topics = new LinkedHashMap<>();
        for (String pair : spec.split(",")) {
            String[] parts = pair.trim().split("=", 2);
            if (parts.length != 2 || parts[0].isBlank() || parts[1].isBlank()) {
                throw new IllegalArgumentException(
                        "STORE_TOPICS entries are topic=prefix, separated by commas; got '" + pair + "'");
            }
            topics.put(parts[0].trim(), parts[1].trim());
        }
        return topics;
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value.trim();
    }
}
