package io.github.jimzucker.flinktraining.generator;

import io.github.jimzucker.flinktraining.model.BlockTrade;
import io.github.jimzucker.flinktraining.model.ReferenceData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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
        LOG.info("starting generators: bootstrap={} trades/sec={} prices/sec={} seed={} duration={} time={}",
                config.bootstrapServers(), config.tradesPerSecond(),
                config.pricesPerSecond(), config.seed(),
                config.runsForever() ? "forever" : config.durationSeconds() + "s",
                config.isLive() ? "wall clock" : "replay from " + config.startEpochMillis());
        LOG.info("universe: {} symbols, {} accounts, {} allocations per trade "
                        + "-> {} symbol keys, {} account keys",
                ReferenceData.SYMBOLS.size(), ReferenceData.ACCOUNTS.size(),
                ReferenceData.ALLOCATIONS_PER_TRADE,
                ReferenceData.SYMBOL_KEY_COUNT, ReferenceData.ACCOUNT_KEY_COUNT);

        AtomicBoolean running = new AtomicBoolean(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> running.set(false)));

        try (KafkaPublisher publisher = new KafkaPublisher(config)) {
            List<Thread> trades = tradeThreads(config, publisher, running);
            Thread prices = new Thread(() -> publishPrices(config, publisher, running), "prices");
            trades.forEach(Thread::start);
            prices.start();

            // Three ways to finish, and only one of them stops the threads here.
            // Bounded by record count, they stop themselves. Bounded by duration,
            // they are stopped once it elapses. Unbounded -- which is how the demo
            // runs -- they must be left alone until the shutdown hook stops them;
            // stopping them here regardless made the generator exit immediately on
            // start, which is precisely the command the runbook gives.
            if (!config.isRecordBounded() && !config.runsForever()) {
                Thread.sleep(config.durationSeconds() * 1_000L);
                running.set(false);
            }
            for (Thread thread : trades) {
                thread.join();
            }
            prices.join();
            publisher.flush();
        }
        LOG.info("generators stopped");
    }

    /**
     * One thread per group of partitions, or a single thread producing exactly as
     * it always has.
     *
     * <p>The thread count has to divide the partition count. With four partitions
     * and two threads, one takes partitions 0 and 2 and the other 1 and 3; three
     * threads would leave a partition with two writers, and two writers on one
     * partition is precisely what makes replay stop being byte-identical.
     */
    private static List<Thread> tradeThreads(GeneratorConfig config, KafkaPublisher publisher,
                                             AtomicBoolean running) {
        int threads = config.generatorThreads();
        if (threads == 1) {
            return List.of(new Thread(() -> publishTrades(config, publisher, running), "block-trades"));
        }
        int partitions = publisher.ordersPartitionCount();
        if (partitions % threads != 0) {
            throw new IllegalArgumentException(
                    "GENERATOR_THREADS=" + threads + " does not divide the orders topic's "
                            + partitions + " partitions; each partition needs exactly one writer "
                            + "for replay to stay byte-identical");
        }
        LOG.info("producing trades on {} threads over {} partitions", threads, partitions);
        List<Thread> list = new ArrayList<>(threads);
        for (int t = 0; t < threads; t++) {
            int index = t;
            list.add(new Thread(
                    () -> publishTradeShard(config, publisher, running, index, partitions),
                    "block-trades-" + t));
        }
        return list;
    }

    /**
     * The part of the trade sequence belonging to one thread.
     *
     * <p>Trade <em>n</em> goes to partition {@code n % partitions}, and that
     * partition belongs to one thread, so a thread walks its own subsequence in
     * increasing order. Every partition therefore sees the same records in the
     * same order on every run, whatever the threads do relative to each other.
     */
    private static void publishTradeShard(GeneratorConfig config, KafkaPublisher publisher,
                                          AtomicBoolean running, int index, int partitions) {
        // Each thread carries its share of the rate, so the total is the rate asked for.
        Pacer pacer = new Pacer(Math.max(1, config.tradesPerSecond() / config.generatorThreads()));
        int threads = config.generatorThreads();
        long count = 0;
        for (long n = firstSequence(index, threads, partitions);
                running.get() && !GeneratorConfig.reached(config.maxTrades(), n);
                n += step(threads, partitions, n, index)) {
            long eventTime = config.isLive()
                    ? System.currentTimeMillis()
                    : config.startEpochMillis() + n * config.tradeIntervalMillis();
            publisher.publish(BlockTradeGenerator.at(config.seed(), n, eventTime),
                    (int) (n % partitions));
            count++;
            if (count % (Math.max(1, config.tradesPerSecond() / threads) * 10L) == 0) {
                LOG.info("thread {} published {} block trades, latest sequence {}", index, count, n);
            }
            if (!pacer.awaitNext()) {
                return;
            }
        }
    }

    /** The first sequence number whose partition belongs to this thread. */
    private static long firstSequence(int index, int threads, int partitions) {
        for (long n = 0; n < partitions; n++) {
            if ((n % partitions) % threads == index) {
                return n;
            }
        }
        throw new IllegalStateException("no partition for thread " + index);
    }

    /** The gap to this thread's next sequence number. */
    private static long step(int threads, int partitions, long current, int index) {
        for (long d = 1; d <= partitions; d++) {
            if (((current + d) % partitions) % threads == index) {
                return d;
            }
        }
        throw new IllegalStateException("no next partition for thread " + index);
    }

    private static void publishTrades(GeneratorConfig config, KafkaPublisher publisher, AtomicBoolean running) {
        BlockTradeGenerator generator = config.isLive()
                ? BlockTradeGenerator.live(config.seed())
                : BlockTradeGenerator.replaying(
                        config.seed(), config.startEpochMillis(), config.tradeIntervalMillis());
        Pacer pacer = new Pacer(config.tradesPerSecond());
        long count = 0;
        while (running.get() && !GeneratorConfig.reached(config.maxTrades(), count)) {
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
        PriceGenerator generator = config.isLive()
                ? PriceGenerator.live(config.seed() + 1)
                : PriceGenerator.replaying(
                        config.seed() + 1, config.startEpochMillis(), config.priceIntervalMillis());
        Pacer pacer = new Pacer(config.pricesPerSecond());
        long count = 0;
        while (running.get() && !GeneratorConfig.reached(config.maxPrices(), count)) {
            publisher.publish(generator.next());
            if (++count % (config.pricesPerSecond() * 30L) == 0) {
                LOG.info("published {} prices, latest {}", count, generator.currentPrices());
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

        /**
         * Sleeping below this is not worth the syscall: the scheduler will not
         * return control that precisely, and asking it to costs more than the
         * wait itself.
         */
        private static final long MIN_SLEEP_NANOS = 2_000_000L;   // 2ms

        /** @return false if interrupted, so the caller can stop cleanly */
        boolean awaitNext() {
            emitted++;
            long targetNanos = startNanos + emitted * intervalNanos;
            long sleepNanos = targetNanos - System.nanoTime();
            if (sleepNanos <= 0) {
                return true;   // already behind; do not sleep, just keep going
            }
            // Sleeping once per record caps the rate long before Kafka does. A
            // sub-millisecond sleep becomes a scheduler yield of a few hundred
            // microseconds, so at a target of 5000/sec the generator managed
            // about 3000 while the producer itself sustains over a hundred
            // thousand. Below the threshold, return and let the next record
            // absorb the difference -- the target time is computed from the run
            // start, so pacing stays accurate on average rather than drifting.
            if (sleepNanos < MIN_SLEEP_NANOS) {
                return true;
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
