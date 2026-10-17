package io.github.jimzucker.flinktraining.store;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Keeps only the last record per key from a batch.
 *
 * <p>The store holds the latest value per key and nothing else, so every earlier
 * record for a key in the same batch would be written and then overwritten
 * before anyone could read it. Dropping them here is what lets one loader keep
 * up with a drain: the writes to Redis are bounded by how many keys exist --
 * about twenty thousand -- not by how many records arrived.
 *
 * <p>"Last" is safe to take in arrival order: a key always lands on one
 * partition, and a consumer reads one partition in offset order.
 */
public final class LatestPerKey {

    /** One record that will be written: where it came from, and its bytes. */
    public record Latest(String topic, int partition, long offset, String key, byte[] value) {
    }

    private final Map<String, Latest> byKey = new LinkedHashMap<>();
    private long seen;

    public void add(ConsumerRecord<byte[], byte[]> record) {
        seen++;
        if (record.key() == null || record.value() == null) {
            return;
        }
        String key = new String(record.key(), StandardCharsets.UTF_8);
        // The topic is part of the identity: positions-by-symbol and
        // mv-by-symbol share the key AAPL and are different entries.
        byKey.put(record.topic() + '\u0000' + key,
                new Latest(record.topic(), record.partition(), record.offset(), key, record.value()));
    }

    /** Records seen, including the ones a later record for the same key replaced. */
    public long seen() {
        return seen;
    }

    public List<Latest> latest() {
        return new ArrayList<>(byKey.values());
    }

    public boolean isEmpty() {
        return byKey.isEmpty();
    }
}
