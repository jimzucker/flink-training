package io.github.jimzucker.flinktraining.job;

/** The published shape of an aggregated position, before it is encoded for Kafka. */
public class PositionState {

    public String key;
    public String symbol;
    public long quantity;
    public String lastTradeId;
    public long updateCount;
    public long eventTime;

    public PositionState() {
    }

    public PositionState(String key, String symbol, long quantity,
                         String lastTradeId, long updateCount, long eventTime) {
        this.key = key;
        this.symbol = symbol;
        this.quantity = quantity;
        this.lastTradeId = lastTradeId;
        this.updateCount = updateCount;
        this.eventTime = eventTime;
    }

    @Override
    public String toString() {
        return "PositionState{" + key + " qty=" + quantity + " n=" + updateCount + "}";
    }
}
