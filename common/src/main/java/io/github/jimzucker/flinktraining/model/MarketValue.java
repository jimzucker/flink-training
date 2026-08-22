package io.github.jimzucker.flinktraining.model;

import java.math.BigDecimal;

/**
 * The market value of one position at a window close, as published to sinks 5
 * and 6.
 *
 * <p>Both inputs are taken as of the same instant — the window boundary — so the
 * figure reconciles against the position topic at that timestamp. An averaged
 * price would match nothing observable anywhere in the system, which makes it
 * impossible to answer when someone asks where the number came from.
 */
public record MarketValue(
        String key,
        String symbol,
        long quantity,
        BigDecimal price,
        BigDecimal marketValue,
        String lastTradeId,
        long windowEnd) {

    public MarketValue {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key is required");
        }
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("symbol is required");
        }
        if (price == null) {
            throw new IllegalArgumentException("price is required");
        }
        if (marketValue == null) {
            throw new IllegalArgumentException("marketValue is required");
        }
    }

    /** quantity x price, at the scale prices carry, so the arithmetic is checkable by eye. */
    public static MarketValue of(String key, String symbol, long quantity, BigDecimal price,
                                 String lastTradeId, long windowEnd) {
        return new MarketValue(key, symbol, quantity, price,
                price.multiply(BigDecimal.valueOf(quantity)), lastTradeId, windowEnd);
    }
}
