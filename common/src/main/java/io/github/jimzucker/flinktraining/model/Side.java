package io.github.jimzucker.flinktraining.model;

/** Which way a trade goes. Applied as a sign when a position is accumulated. */
public enum Side {
    BUY(1),
    SELL(-1);

    private final int sign;

    Side(int sign) {
        this.sign = sign;
    }

    /** +1 for a buy, -1 for a sell. A position is a signed running sum, not a count. */
    public int sign() {
        return sign;
    }

    /** Signed quantity for this side, so callers never re-derive the sign convention. */
    public long apply(long quantity) {
        return sign * quantity;
    }
}
