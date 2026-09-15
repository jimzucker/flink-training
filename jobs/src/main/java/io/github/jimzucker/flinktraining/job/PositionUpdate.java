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

    /**
     * The allocation's filler fields as alternating name and value, carried
     * across the shuffle to the aggregation so a larger order costs network and
     * serialization bytes, not only parsing. Null on the symbol side and for
     * orders without filler.
     *
     * <p>A {@code String[]} rather than a {@code Map}: Flink serializes a string
     * array natively, while a map field would fall back to Kryo and add a second
     * variable to any comparison against orders without filler.
     */
    public String[] filler;

    public PositionUpdate() {
    }

    public PositionUpdate(String key, String symbol, long signedQuantity, String tradeId, long eventTime) {
        this(key, symbol, signedQuantity, tradeId, eventTime, null);
    }

    public PositionUpdate(String key, String symbol, long signedQuantity, String tradeId, long eventTime,
                          String[] filler) {
        this.key = key;
        this.symbol = symbol;
        this.signedQuantity = signedQuantity;
        this.tradeId = tradeId;
        this.eventTime = eventTime;
        this.filler = filler;
    }

    @Override
    public String toString() {
        return "PositionUpdate{" + key + " " + signedQuantity + " " + tradeId + "}";
    }
}
