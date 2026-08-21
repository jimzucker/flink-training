package io.github.jimzucker.flinktraining.job;

/**
 * One signed movement against one position key, on its way to an aggregate.
 *
 * <p>A plain mutable class with public fields and a no-argument constructor
 * rather than a record: Flink 1.20 does not recognise records as POJOs and
 * falls back to Kryo, which cannot reliably instantiate them. This shape gets
 * Flink's own POJO serialiser, which is both faster and not a runtime surprise.
 */
public class PositionUpdate {

    public String key;
    public String symbol;
    /** Signed: positive for a buy, negative for a sell. */
    public long signedQuantity;
    public String tradeId;
    public long eventTime;

    public PositionUpdate() {
    }

    public PositionUpdate(String key, String symbol, long signedQuantity, String tradeId, long eventTime) {
        this.key = key;
        this.symbol = symbol;
        this.signedQuantity = signedQuantity;
        this.tradeId = tradeId;
        this.eventTime = eventTime;
    }

    @Override
    public String toString() {
        return "PositionUpdate{" + key + " " + signedQuantity + " " + tradeId + "}";
    }
}
