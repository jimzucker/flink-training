package io.github.jimzucker.flinktraining.model;

/**
 * A running position for one key, as published to sinks 3 and 4.
 *
 * <p>{@code quantity} is a signed running sum: buys add, sells subtract, so a
 * key whose sells exceed its buys legitimately holds a negative — a short.
 *
 * <p>{@code lastTradeId} and {@code updateCount} exist for traceability. When
 * someone asks why a position reads what it does, the answer is the trade that
 * last moved it and how many trades have touched it, both of which are on the
 * record rather than reconstructed from logs.
 */
public record Position(
        String key,
        String symbol,
        long quantity,
        String lastTradeId,
        long updateCount,
        long eventTime) {

    public Position {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key is required");
        }
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("symbol is required");
        }
        if (updateCount <= 0) {
            throw new IllegalArgumentException("a published position has been updated at least once");
        }
    }
}
