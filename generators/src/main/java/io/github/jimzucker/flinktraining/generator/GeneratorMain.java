package io.github.jimzucker.flinktraining.generator;

import io.github.jimzucker.flinktraining.model.BlockTrade;
import io.github.jimzucker.flinktraining.model.Price;
import io.github.jimzucker.flinktraining.model.ReferenceData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Runs both generators against Kafka at the configured rates.
 *
 * <p>The two streams are paced independently on their own threads, so the order
 * rate and the price rate can be turned separately. The assignment's two scale
 * cases need exactly that: push orders to 1000/sec, or push prices very high,
 * and show neither harms the other.
 */
public final class GeneratorMain {

    private static final Logger LOG = LoggerFactory.getLogger(GeneratorMain.class);

    public static void main(String[] args) throws Exception {
        GeneratorConfig config = GeneratorConfig.fromEnvironment();
        LOG.info("starting generators: bootstrap={} trades/sec={} priceTicks/sec={} seed={} duration={}",
                config.bootstrapServers(), config.tradesPerSecond(),
                config.priceTicksPerSecond(), config.seed(),
                config.runsForever() ? "forever" : config.durationSeconds() + "s");
        LOG.info("universe: {} symbols {} accounts, {} allocations per trade -> {} symbol keys, {} account keys",
                ReferenceData.SYMBOLS.size(), ReferenceData.ACCOUNTS.size(),
                ReferenceData.ALLOCATIONS_PER_TRADE,
                ReferenceData.SYMBOL_KEY_COUNT, ReferenceData.ACCOUNT_KEY_COUNT);

        AtomicBoolean running = new AtomicBoolean(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> running.set(false)));

        try (KafkaPublisher publisher = new KafkaPublisher(config)) {
            Thread trades = new Thread(() -> publishTrades(config, publisher, running), "block-trades");
            Thread prices = new Thread(() -> publishPrices(config, publisher, running), "prices");
            trades.start();
            prices.start();

            if (!config.runsForever()) {
                Thread.sleep(config.durationSeconds() * 1_000L);
                running.set(false);
            }
            trades.join();
            prices.join();
        }
        LOG.info("generators stopped");
    }

    private static void publishTrades(GeneratorConfig config, KafkaPublisher publisher, AtomicBoolean running) {
        BlockTradeGenerator generator = new BlockTradeGenerator(
                config.seed(), config.startEpochMillis(), config.tradeIntervalMillis());
        Pacer pacer = new Pacer(config.tradesPerSecond());
        long count = 0;
        while (running.get()) {
            BlockTrade trade = generator.next();
            publisher.publish(trade);
            if (++count % (config.tradesPerSecond() * 10L) == 0) {
                LOG.info("published {} block trades, latest {} {} {} qty={}",
                        count, trade.tradeId(), trade.side(), trade.symbol(), trade.quantity());
            }
            if (!pacer.awaitNext()) {
                return;
            }
        }
    }

    private static void publishPrices(GeneratorConfig config, KafkaPublisher publisher, AtomicBoolean running) {
        // A separate seed, so changing the price rate cannot shift the trade sequence.
        PriceGenerator generator = new PriceGenerator(
                config.seed() + 1, config.startEpochMillis(), config.priceIntervalMillis());
        Pacer pacer = new Pacer(config.priceTicksPerSecond());
        long ticks = 0;
        while (running.get()) {
            List<Price> prices = generator.next();
            prices.forEach(publisher::publish);
            if (++ticks % (config.priceTicksPerSecond() * 30L) == 0) {
                LOG.info("published {} price ticks, latest {}", ticks, generator.currentPrices());
            }
            if (!pacer.awaitNext()) {
                return;
            }
        }
    }

    /**
     * Paces emission in real time without letting drift accumulate.
     *
     * <p>Sleeping a fixed interval per record would fall progressively behind,
     * so the target time for record n is computed from the start rather than
     * from the previous record.
     */
    static final class Pacer {

        private final long intervalNanos;
        private final long startNanos;
        private long emitted;

        Pacer(int perSecond) {
            this.intervalNanos = 1_000_000_000L / perSecond;
            this.startNanos = System.nanoTime();
        }

        /** @return false if interrupted, so the caller can stop cleanly */
        boolean awaitNext() {
            emitted++;
            long targetNanos = startNanos + emitted * intervalNanos;
            long sleepNanos = targetNanos - System.nanoTime();
            if (sleepNanos <= 0) {
                return true;   // already behind; do not sleep, just keep going
            }
            try {
                Thread.sleep(sleepNanos / 1_000_000L, (int) (sleepNanos % 1_000_000L));
                return true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }

    private GeneratorMain() {
    }
}
