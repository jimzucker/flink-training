package io.github.jimzucker.flinktraining.store;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Copies the latest value of every key on the four output topics into Redis.
 *
 * <p>It reads the topics the Flink jobs write, as one more consumer group, and
 * the jobs never know it is there. If Redis is slow or down, this falls behind
 * and the topics hold the difference; Flink does not wait for it, so the store
 * cannot back-pressure the pipeline.
 *
 * <p>Reads only committed records ({@code read_committed}): the jobs write
 * transactionally, and a record from a transaction that was later aborted must
 * never reach the store. Offsets are committed to Kafka only after Redis has
 * accepted the batch, so a crash means some records are read twice, and the
 * write-if-newer check in {@link RedisStore} makes reading twice harmless.
 */
public final class StoreLoader {

    private static final Logger LOG = LoggerFactory.getLogger(StoreLoader.class);

    public static void main(String[] args) throws Exception {
        StoreConfig config = StoreConfig.fromEnvironment();
        LOG.info("store loader: bootstrap={} redis={}:{} threads={} topics={}",
                config.bootstrapServers(), config.redisHost(), config.redisPort(),
                config.threads(), config.topics());

        AtomicBoolean running = new AtomicBoolean(true);
        List<Worker> workers = new ArrayList<>();
        try (LoaderMetrics metrics = new LoaderMetrics(config.metricsPort());
             TopicIds topicIds = new TopicIds(config.bootstrapServers(), config.topics().keySet())) {
            topicIds.refresh();
            ScheduledExecutorService refresher = Executors.newSingleThreadScheduledExecutor();
            refresher.scheduleWithFixedDelay(topicIds::refresh, 2, 2, TimeUnit.SECONDS);

            List<Thread> threads = new ArrayList<>();
            for (int i = 0; i < config.threads(); i++) {
                Worker worker = new Worker("loader-" + i, config, topicIds, metrics, running);
                workers.add(worker);
                Thread thread = new Thread(worker, worker.name);
                threads.add(thread);
                thread.start();
            }
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                running.set(false);
                workers.forEach(Worker::wakeup);
                for (Thread t : threads) {
                    try {
                        t.join(10_000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }));
            for (Thread t : threads) {
                t.join();
            }
            refresher.shutdownNow();
        }
    }

    static Properties consumerProperties(StoreConfig config, String clientId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.groupId());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // A consumer asks for a missing topic to be created by default. The scale
        // scripts delete the outputs between cases, and this loader must not put
        // one back -- with the broker's default partition count -- before the
        // script creates it with the right one.
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, config.maxPollRecords());
        // Large fetches, so a drain is read in few round trips. The jobs commit
        // every checkpoint, so records become readable in bursts anyway.
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1 << 20);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 200);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 8 << 20);
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 64 << 20);
        // Every record is otherwise checksummed on the way in. The loader shares
        // the machine with the jobs it must not slow, and the broker has already
        // checked these on write.
        props.put(ConsumerConfig.CHECK_CRCS_CONFIG, false);
        return props;
    }

    /** One consumer and one Redis connection, on its own thread. */
    static final class Worker implements Runnable, ConsumerRebalanceListener {

        final String name;
        private final StoreConfig config;
        private final TopicIds topicIds;
        private final LoaderMetrics metrics;
        private final AtomicBoolean running;
        private final KafkaConsumer<byte[], byte[]> consumer;
        /** The topic id this thread last read each topic under. */
        private final Map<String, String> readingUnder = new HashMap<>();
        private long lastLagReport;

        Worker(String name, StoreConfig config, TopicIds topicIds, LoaderMetrics metrics, AtomicBoolean running) {
            this.name = name;
            this.config = config;
            this.topicIds = topicIds;
            this.metrics = metrics;
            this.running = running;
            this.consumer = new KafkaConsumer<>(consumerProperties(config, name));
        }

        void wakeup() {
            consumer.wakeup();
        }

        @Override
        public void run() {
            RedisStore store = null;
            try {
                consumer.subscribe(config.topics().keySet(), this);
                while (running.get()) {
                    if (store == null) {
                        store = connect();
                        if (store == null) {
                            // Keep polling so this thread stays in the group, but
                            // read nothing: paused partitions return no records,
                            // and any that arrive anyway are read again later.
                            consumer.pause(consumer.assignment());
                            rewind(consumer.poll(Duration.ofMillis(500)));
                            // The lag keeps being reported while Redis is away:
                            // it is the one reading that shows the store behind.
                            reportLag();
                            continue;
                        }
                        consumer.resume(consumer.paused());
                    }
                    ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
                    Set<String> switched = switchedTopics(store);
                    LatestPerKey batch = new LatestPerKey();
                    Map<TopicPartition, Long> firstOffset = new HashMap<>();
                    Map<TopicPartition, Long> nextOffset = new HashMap<>();
                    Map<String, Long> readByTopic = new HashMap<>();
                    for (ConsumerRecord<byte[], byte[]> r : records) {
                        if (switched.contains(r.topic())) {
                            continue;  // read again from the start of the new topic
                        }
                        TopicPartition tp = new TopicPartition(r.topic(), r.partition());
                        firstOffset.putIfAbsent(tp, r.offset());
                        nextOffset.put(tp, r.offset() + 1);
                        readByTopic.merge(r.topic(), 1L, Long::sum);
                        batch.add(r);
                    }
                    if (!batch.isEmpty()) {
                        try {
                            Map<String, Integer> changed = write(store, batch.latest());
                            changed.forEach(metrics::written);
                        } catch (RuntimeException e) {
                            // Redis is down or restarting. Nothing is committed, so
                            // go back to the start of this batch and read it again
                            // once Redis answers. Flink is unaffected throughout.
                            metrics.redisError();
                            LOG.warn("{}: Redis did not take the batch, reading it again: {}", name, e.toString());
                            firstOffset.forEach(consumer::seek);
                            store.close();
                            store = null;
                            continue;
                        }
                        readByTopic.forEach(metrics::read);
                        Map<TopicPartition, OffsetAndMetadata> commit = new HashMap<>();
                        nextOffset.forEach((tp, next) -> commit.put(tp,
                                new OffsetAndMetadata(next, readingUnder.getOrDefault(tp.topic(), ""))));
                        consumer.commitAsync(commit, (offsets, err) -> {
                            if (err != null) {
                                LOG.debug("{}: commit not accepted, the batch will be read again: {}", name, err.toString());
                            }
                        });
                    }
                    reportLag();
                }
            } catch (WakeupException e) {
                // shutting down
            } finally {
                try {
                    consumer.commitSync(Duration.ofSeconds(5));
                } catch (RuntimeException e) {
                    LOG.debug("{}: final commit not accepted: {}", name, e.toString());
                }
                consumer.close(Duration.ofSeconds(5));
                metrics.clearLag(name);
                if (store != null) {
                    store.close();
                }
            }
        }

        private RedisStore connect() {
            try {
                return new RedisStore(config.redisHost(), config.redisPort());
            } catch (RuntimeException e) {
                metrics.redisError();
                LOG.warn("{}: Redis not reachable at {}:{}, trying again in 0.5s: {}",
                        name, config.redisHost(), config.redisPort(), e.toString());
                sleepQuietly(500);
                return null;
            }
        }

        /** Puts each partition back to the first record of a batch that was not written. */
        private void rewind(ConsumerRecords<byte[], byte[]> records) {
            for (TopicPartition tp : records.partitions()) {
                consumer.seek(tp, records.records(tp).get(0).offset());
            }
        }

        /**
         * Topics recreated since this thread last read them. For each, go back to
         * the start of the new topic and clear the keys only the old topic had.
         */
        private Set<String> switchedTopics(RedisStore store) {
            Set<String> switched = new HashSet<>();
            for (String topic : config.topics().keySet()) {
                String now = topicIds.get(topic);
                String before = readingUnder.get(topic);
                if (now == null || Objects.equals(now, before)) {
                    continue;
                }
                readingUnder.put(topic, now);
                if (before == null) {
                    continue;
                }
                switched.add(topic);
                List<TopicPartition> mine = consumer.assignment().stream()
                        .filter(tp -> tp.topic().equals(topic)).toList();
                consumer.seekToBeginning(mine);
                long removed = store.removeOtherTopicIds(config.topics().get(topic), now);
                LOG.info("{}: {} was created again (id {}); reading it from the start, removed {} old keys",
                        name, topic, now, removed);
            }
            return switched;
        }

        private Map<String, Integer> write(RedisStore store, List<LatestPerKey.Latest> latest) {
            Map<String, String> ids = new HashMap<>();
            List<LatestPerKey.Latest> known = new ArrayList<>(latest.size());
            for (LatestPerKey.Latest l : latest) {
                String id = readingUnder.get(l.topic());
                if (id == null) {
                    throw new IllegalStateException("no topic id yet for " + l.topic());
                }
                ids.put(l.topic(), id);
                known.add(l);
            }
            Map<String, Integer> changed = new HashMap<>();
            Map<String, List<LatestPerKey.Latest>> byTopic = new HashMap<>();
            known.forEach(l -> byTopic.computeIfAbsent(l.topic(), t -> new ArrayList<>()).add(l));
            byTopic.forEach((topic, entries) ->
                    changed.put(topic, store.write(entries, config.topics(), ids)));
            return changed;
        }

        /**
         * Records committed on each topic that this thread has not written yet,
         * asked of the broker every two seconds. Not the consumer's cached lag:
         * that only updates while it fetches, and it stops fetching while Redis
         * is away -- exactly when the lag matters.
         */
        private void reportLag() {
            long now = System.nanoTime();
            if (now - lastLagReport < 2_000_000_000L) {
                return;
            }
            lastLagReport = now;
            Set<TopicPartition> assigned = consumer.assignment();
            Map<String, Long> byTopic = new HashMap<>();
            try {
                Map<TopicPartition, Long> end = consumer.endOffsets(assigned, Duration.ofSeconds(5));
                for (TopicPartition tp : assigned) {
                    long behind = Math.max(0L, end.getOrDefault(tp, 0L) - consumer.position(tp, Duration.ofSeconds(5)));
                    byTopic.merge(tp.topic(), behind, Long::sum);
                }
            } catch (RuntimeException e) {
                LOG.debug("{}: lag not read this time: {}", name, e.toString());
                return;
            }
            metrics.clearLag(name);
            byTopic.forEach((topic, n) -> metrics.lag(name, topic, n));
        }

        /**
         * A partition handed to this thread starts where the group left off --
         * unless that position was committed under an older topic id, in which
         * case the topic was recreated and the position means nothing now.
         */
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            if (partitions.isEmpty()) {
                return;
            }
            topicIds.refresh();
            Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(new HashSet<>(partitions));
            List<TopicPartition> fromStart = new ArrayList<>();
            for (TopicPartition tp : partitions) {
                String now = topicIds.get(tp.topic());
                readingUnder.putIfAbsent(tp.topic(), now);
                OffsetAndMetadata c = committed.get(tp);
                if (c != null && now != null && !now.equals(c.metadata())) {
                    fromStart.add(tp);
                }
            }
            if (!fromStart.isEmpty()) {
                consumer.seekToBeginning(fromStart);
                LOG.info("{}: {} committed under an older topic id; reading from the start", name, fromStart);
            }
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            // Offsets for what was written are committed after each batch.
        }

        private static void sleepQuietly(long millis) {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
