package io.github.jimzucker.flinktraining.model;

import java.util.List;

/**
 * One order, split across several accounts.
 *
 * <p>{@code quantity} is the whole block; the allocations must sum to it. That
 * invariant is what makes the two aggregations downstream reconcile: summing
 * allocations by symbol gives the same total as summing block quantities.
 */
public record BlockTrade(
        String tradeId,
        String symbol,
        Side side,
        long quantity,
        List<Allocation> allocations,
        long eventTime) {

    public BlockTrade {
        if (tradeId == null || tradeId.isBlank()) {
            throw new IllegalArgumentException("tradeId is required");
        }
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("symbol is required");
        }
        if (side == null) {
            throw new IllegalArgumentException("side is required");
        }
        if (allocations == null || allocations.isEmpty()) {
            throw new IllegalArgumentException("a block trade must have at least one allocation");
        }
        long allocated = allocations.stream().mapToLong(Allocation::quantity).sum();
        if (allocated != quantity) {
            throw new IllegalArgumentException(
                    "allocations sum to " + allocated + " but block quantity is " + quantity);
        }
        allocations = List.copyOf(allocations);
    }

    /** Signed quantity for this trade: positive for a buy, negative for a sell. */
    public long signedQuantity() {
        return side.apply(quantity);
    }

    /** Signed quantity for one allocation of this trade. */
    public long signedQuantity(Allocation allocation) {
        return side.apply(allocation.quantity());
    }
}
