package io.github.jimzucker.flinktraining.generator;

import io.github.jimzucker.flinktraining.model.Allocation;
import io.github.jimzucker.flinktraining.model.BlockTrade;
import io.github.jimzucker.flinktraining.model.Json;
import io.github.jimzucker.flinktraining.model.Price;
import io.github.jimzucker.flinktraining.model.ReferenceData;
import io.github.jimzucker.flinktraining.model.Side;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Fills a bounded backlog and writes the manifest the scaling harness needs.
 *
 * <p>This exists so this repository's own job can be measured by the same
 * instrument as the pipelines the skill produces. The demo's generator paces
 * against a clock and asserts nothing about totals; the harness needs a fixed
 * record count and an expected answer computed from the input alone. Both come
 * from {@link BlockTradeGenerator#at}, which is a pure function of (seed,
 * sequence), so the expectation here is never read back from the pipeline.
 *
 * <pre>
 *   HarnessBacklog --mode=produce --count=N --seed=S --partitions=P
 *                  --bootstrap=host:port --topic=block-trades
 *                  --pricesTopic=prices --manifest=results/manifest.json
 * </pre>
 */
public final class HarnessBacklog {

    private HarnessBacklog() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> a = new LinkedHashMap<>();
        for (String arg : args) {
            int eq = arg.indexOf('=');
            if (arg.startsWith("--") && eq > 0) {
                a.put(arg.substring(2, eq), arg.substring(eq + 1));
            }
        }
        long count = Long.parseLong(a.getOrDefault("count", "0"));
        long seed = Long.parseLong(a.getOrDefault("seed", "1"));
        String bootstrap = a.getOrDefault("bootstrap", "localhost:9092");
        String topic = a.getOrDefault("topic", "block-trades");
        String pricesTopic = a.getOrDefault("pricesTopic", "prices");
        Path manifest = Path.of(a.getOrDefault("manifest", "manifest.json"));
        long prices = Long.parseLong(a.getOrDefault("prices", Long.toString(Math.max(1, count / 100))));

        if (count <= 0) {
            throw new IllegalArgumentException("--count is required and must be positive");
        }

        // Expected answer first, from the generator alone.
        Map<String, Long> bySymbol = new TreeMap<>();
        Map<String, Long> byAccount = new TreeMap<>();
        long start = System.currentTimeMillis() - count;   // one event per millisecond, replayed
        for (long n = 0; n < count; n++) {
            BlockTrade t = BlockTradeGenerator.at(seed, n, start + n);
            long signed = t.side() == Side.BUY ? 1 : -1;
            bySymbol.merge(t.symbol(), signed * t.quantity(), Long::sum);
            for (Allocation alloc : t.allocations()) {
                byAccount.merge(io.github.jimzucker.flinktraining.model.AccountKey.of(alloc, t.symbol()),
                        signed * alloc.quantity(), Long::sum);
            }
        }

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("count", count);
        out.put("seed", seed);
        out.put("symbolCount", ReferenceData.SYMBOLS.size());
        out.put("accountCount", ReferenceData.ACCOUNTS.size());
        out.put("allocationsPerTrade", ReferenceData.ALLOCATIONS_PER_TRADE);
        out.put("distinctSymbolKeys", bySymbol.size());
        out.put("distinctAccountKeys", byAccount.size());
        out.put("symbolUpdates", count);
        out.put("accountUpdates", count * ReferenceData.ALLOCATIONS_PER_TRADE);
        out.put("netBySymbol", bySymbol);
        out.put("netByAccount", byAccount);
        Files.createDirectories(manifest.toAbsolutePath().getParent());
        Files.writeString(manifest, Json.toJson(out));

        if ("expect".equals(a.get("mode"))) {
            System.out.printf("manifest only: %d trades, %d symbol keys, %d account keys%n",
                    count, bySymbol.size(), byAccount.size());
            return;
        }

        GeneratorConfig config = new GeneratorConfig(bootstrap, topic, pricesTopic,
                GeneratorConfig.DEMO_TRADES_PER_SECOND, GeneratorConfig.DEMO_PRICES_PER_SECOND,
                seed, 1, start, 0L, count, prices);
        long t0 = System.currentTimeMillis();
        try (KafkaPublisher publisher = new KafkaPublisher(config)) {
            for (long n = 0; n < count; n++) {
                publisher.publish(BlockTradeGenerator.at(seed, n, start + n));
            }
            PriceGenerator priceGen = PriceGenerator.replaying(seed, start, 100L);
            for (long n = 0; n < prices; n++) {
                publisher.publish(priceGen.next());
            }
            publisher.flush();
        }
        double s = (System.currentTimeMillis() - t0) / 1000.0;
        System.out.printf("produced %,d trades and %,d prices in %.1fs (%,.0f rec/s); manifest %s%n",
                count, prices, s, count / s, manifest);
    }
}
