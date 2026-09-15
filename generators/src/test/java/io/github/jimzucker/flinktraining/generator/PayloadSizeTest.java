package io.github.jimzucker.flinktraining.generator;

import io.github.jimzucker.flinktraining.model.Allocation;
import io.github.jimzucker.flinktraining.model.BlockTrade;
import io.github.jimzucker.flinktraining.model.Json;
import io.github.jimzucker.flinktraining.model.ReferenceData;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PayloadSizeTest {

    private static final long SEED = 20260910L;
    private static final long TIME = 1789000000000L;

    /**
     * Captured from the generator before filler existed, through the same
     * mapper: seed 20260910, sequence 0, event time 1789000000000.
     */
    private static final String ORDER_BEFORE_FILLER =
            "{\"allocations\":[{\"account\":\"ACC1\",\"quantity\":100,\"subAccount\":\"SUB1\"},"
            + "{\"account\":\"ACC2\",\"quantity\":100,\"subAccount\":\"SUB1\"},"
            + "{\"account\":\"ACC3\",\"quantity\":100,\"subAccount\":\"SUB1\"},"
            + "{\"account\":\"ACC4\",\"quantity\":100,\"subAccount\":\"SUB1\"}],"
            + "\"eventTime\":1789000000000,\"quantity\":400,\"side\":\"BUY\",\"symbol\":\"MSFT\","
            + "\"tradeId\":\"T000000000\"}";

    private static int bytes(BlockTrade t) {
        return Json.toJson(t).getBytes(StandardCharsets.UTF_8).length;
    }

    @Test
    @DisplayName("with no filler, an order is byte-identical to before filler existed")
    void noFillerIsUnchanged() {
        assertThat(ReferenceData.FILLER_FIELDS).as("tests run with FILLER_FIELDS unset").isZero();
        assertThat(Json.toJson(BlockTradeGenerator.at(SEED, 0, TIME))).isEqualTo(ORDER_BEFORE_FILLER);
        assertThat(Json.toJson(BlockTradeGenerator.at(SEED, 0, TIME, 0))).isEqualTo(ORDER_BEFORE_FILLER);
    }

    @Test
    @DisplayName("32 filler fields per allocation make an order of about 2 KB")
    void thirtyTwoFieldsIsAboutTwoKilobytes() {
        BlockTrade t = BlockTradeGenerator.at(SEED, 0, TIME, 32);
        int size = bytes(t);
        System.out.printf("order bytes: %d with no filler, %d with 32 filler fields per allocation%n",
                bytes(BlockTradeGenerator.at(SEED, 0, TIME, 0)), size);
        assertThat(size).isBetween(1_900, 2_100);
        for (Allocation a : t.allocations()) {
            assertThat(a.filler()).hasSize(32);
            assertThat(a.filler().keySet()).startsWith("f01", "f02").endsWith("f32");
            assertThat(a.filler().values()).containsOnly("XXXX");
        }
    }

    @Test
    @DisplayName("filler changes nothing but the padding: same symbol, side and quantities")
    void fillerDoesNotChangeTheTrade() {
        for (long n = 0; n < 1_000; n++) {
            BlockTrade plain = BlockTradeGenerator.at(SEED, n, TIME + n, 0);
            BlockTrade padded = BlockTradeGenerator.at(SEED, n, TIME + n, 32);
            assertThat(padded.tradeId()).isEqualTo(plain.tradeId());
            assertThat(padded.symbol()).isEqualTo(plain.symbol());
            assertThat(padded.side()).isEqualTo(plain.side());
            assertThat(padded.quantity()).isEqualTo(plain.quantity());
            assertThat(padded.allocations()).extracting(Allocation::quantity)
                    .isEqualTo(plain.allocations().stream().map(Allocation::quantity).toList());
        }
    }

    @Test
    @DisplayName("orders with filler are deterministic and survive a JSON round trip in order")
    void fillerIsDeterministicAndRoundTrips() {
        String first = Json.toJson(BlockTradeGenerator.at(SEED, 7, TIME, 32));
        String second = Json.toJson(BlockTradeGenerator.at(SEED, 7, TIME, 32));
        assertThat(second).isEqualTo(first);

        BlockTrade back = Json.fromJson(first, BlockTrade.class);
        assertThat(Json.toJson(back)).isEqualTo(first);
        List<String> keys = List.copyOf(back.allocations().get(0).filler().keySet());
        assertThat(keys).hasSize(32).first().isEqualTo("f01");
        assertThat(keys.get(31)).isEqualTo("f32");
    }
}
