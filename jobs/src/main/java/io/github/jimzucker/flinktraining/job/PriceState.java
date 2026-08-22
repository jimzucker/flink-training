package io.github.jimzucker.flinktraining.job;

import java.math.BigDecimal;

/** A price as it travels inside the pipeline. */
public class PriceState {

    public String symbol;
    public BigDecimal price;
    public long eventTime;

    public PriceState() {
    }

    public PriceState(String symbol, BigDecimal price, long eventTime) {
        this.symbol = symbol;
        this.price = price;
        this.eventTime = eventTime;
    }

    @Override
    public String toString() {
        return "PriceState{" + symbol + " " + price + "}";
    }
}
