package io.github.jimzucker.flinktraining.job;

import io.github.jimzucker.flinktraining.model.Json;
import io.github.jimzucker.flinktraining.model.Position;
import io.github.jimzucker.flinktraining.model.Price;
import org.apache.flink.api.common.functions.MapFunction;

/** JSON off the wire into the pipeline's own types. */
public final class Parsers {

    public static final class ToPosition implements MapFunction<String, PositionState> {
        private static final long serialVersionUID = 1L;

        @Override
        public PositionState map(String json) {
            Position p = Json.fromJson(json, Position.class);
            return new PositionState(p.key(), p.symbol(), p.quantity(),
                    p.lastTradeId(), p.updateCount(), p.eventTime());
        }
    }

    public static final class ToPrice implements MapFunction<String, PriceState> {
        private static final long serialVersionUID = 1L;

        @Override
        public PriceState map(String json) {
            Price p = Json.fromJson(json, Price.class);
            return new PriceState(p.symbol(), p.price(), p.eventTime());
        }
    }

    private Parsers() {
    }
}
