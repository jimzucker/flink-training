package io.github.jimzucker.flinktraining.generator;

import io.github.jimzucker.flinktraining.model.BlockTrade;
import io.github.jimzucker.flinktraining.model.Json;
import io.github.jimzucker.flinktraining.model.Price;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproducibility: the same seed has to produce the same bytes, every time.
 *
 * <p>The assignment is explicit that results must be reproducible and that
 * unexplainable output is worse than no output. Randomised sides are only
 * acceptable because the randomness replays identically.
 */
class DeterminismTest {

    private static final long SEED = GeneratorConfig.DEFAULT_SEED;
    private static final long START = GeneratorConfig.DEFAULT_START_EPOCH_MILLIS;

    private static String tradesJson(long seed, int count) {
        BlockTradeGenerator generator = new BlockTradeGenerator(seed, START, 100L);
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < count; i++) {
            out.append(Json.toJson(generator.next())).append('\n');
        }
        return out.toString();
    }

    private static String pricesJson(long seed, int ticks) {
        PriceGenerator generator = new PriceGenerator(seed, START, 1_000L);
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < ticks; i++) {
            for (Price price : generator.next()) {
                out.append(Json.toJson(price)).append('\n');
            }
        }
        return out.toString();
    }

    private static String sha256(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes()));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    @DisplayName("two runs of the same seed produce byte-identical trades")
    void tradesAreByteIdentical() {
        assertThat(tradesJson(SEED, 1_000)).isEqualTo(tradesJson(SEED, 1_000));
    }

    @Test
    @DisplayName("two runs of the same seed produce byte-identical prices")
    void pricesAreByteIdentical() {
        assertThat(pricesJson(SEED, 200)).isEqualTo(pricesJson(SEED, 200));
    }

    @Test
    @DisplayName("a different seed produces different data")
    void differentSeedDiffers() {
        // Otherwise the seed is not actually wired through and "reproducible"
        // would be indistinguishable from "constant".
        assertThat(tradesJson(SEED, 100)).isNotEqualTo(tradesJson(SEED + 1, 100));
    }

    @Test
    @DisplayName("the demo sequence is pinned by hash")
    void demoSequenceIsPinned() {
        // Guards against an unnoticed change to generation order or JSON shape.
        // If this fails deliberately, re-pin it and say so in the journal.
        assertThat(sha256(tradesJson(SEED, 100)))
                .isEqualTo("241903ac4ea9b746407516075275b694c42bb3a372c107df48ecd8b69d83fdae");
        assertThat(sha256(pricesJson(SEED + 1, 100)))
                .isEqualTo("16d012bc277e838d0b62ec377bc5ad61e96b01d0c3b1cec90fc3a920eb4f6ecb");
    }

    @Test
    @DisplayName("net position per key is exact arithmetic, not an approximation")
    void netPositionIsExact() {
        BlockTradeGenerator generator = new BlockTradeGenerator(SEED, START, 100L);
        Map<String, Long> netBySymbol = new HashMap<>();
        for (int i = 0; i < 1_000; i++) {
            BlockTrade trade = generator.next();
            netBySymbol.merge(trade.symbol(), trade.signedQuantity(), Long::sum);
        }

        // Every symbol traded, and the total across symbols is the signed sum of
        // every block -- a figure that can be recomputed by hand from the stream.
        assertThat(netBySymbol).hasSize(4);
        long total = netBySymbol.values().stream().mapToLong(Long::longValue).sum();

        BlockTradeGenerator again = new BlockTradeGenerator(SEED, START, 100L);
        long recomputed = 0;
        for (int i = 0; i < 1_000; i++) {
            recomputed += again.next().signedQuantity();
        }
        assertThat(total).isEqualTo(recomputed);
    }

    @Test
    @DisplayName("prices stay positive and land on exact quarters")
    void pricesAreExactQuarters() {
        PriceGenerator generator = new PriceGenerator(SEED, START, 1_000L);
        List<BigDecimal> seen = new ArrayList<>();
        for (int tick = 0; tick < 500; tick++) {
            generator.next().forEach(p -> seen.add(p.price()));
        }
        assertThat(seen).isNotEmpty();
        for (BigDecimal price : seen) {
            assertThat(price.signum()).isPositive();
            // A quarter increment: four times the price must be a whole number,
            // so market value never carries a rounding artefact.
            assertThat(price.multiply(new BigDecimal("4")).stripTrailingZeros().scale())
                    .isLessThanOrEqualTo(0);
        }
    }
}
