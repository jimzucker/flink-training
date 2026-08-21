package io.github.jimzucker.flinktraining.job;

import java.math.BigDecimal;

/** A market value at a window close, before it is encoded for Kafka. */
public class MarketValueState {

    public String key;
    public String symbol;
    public long quantity;
    public BigDecimal price;
    public BigDecimal marketValue;
    public String lastTradeId;
    public long windowEnd;

    public MarketValueState() {
    }

    public MarketValueState(String key, String symbol, long quantity, BigDecimal price,
                            BigDecimal marketValue, String lastTradeId, long windowEnd) {
        this.key = key;
        this.symbol = symbol;
        this.quantity = quantity;
        this.price = price;
        this.marketValue = marketValue;
        this.lastTradeId = lastTradeId;
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "MarketValueState{" + key + " " + quantity + " x " + price + " = " + marketValue + "}";
    }
}
