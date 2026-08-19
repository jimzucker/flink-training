package io.github.jimzucker.flinktraining.model;

import java.math.BigDecimal;

/**
 * A price for a symbol at a point in time.
 *
 * <p>{@link BigDecimal} rather than {@code double}: market value has to be
 * explainable to the decimal, and binary floating point cannot represent most
 * two-decimal prices exactly.
 */
public record Price(String symbol, BigDecimal price, long eventTime) {

    public Price {
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("symbol is required");
        }
        if (price == null || price.signum() <= 0) {
            throw new IllegalArgumentException("price must be positive, got " + price);
        }
    }
}
