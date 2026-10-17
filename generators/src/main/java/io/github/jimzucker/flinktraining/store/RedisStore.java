package io.github.jimzucker.flinktraining.store;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Writes the latest value per key into Redis, one hash per key.
 *
 * <p>Each hash holds the record's own fields plus where it came from: the
 * topic's id, the partition and the offset. A write only lands if it is newer
 * than what is stored, so a loader that restarts and reads part of a topic again
 * can never move a key back to an older value. The check and the write happen
 * in one Lua script, which Redis runs without anything in between.
 *
 * <p>Not thread-safe: a Jedis connection belongs to one thread, so each loader
 * thread has its own store.
 */
public final class RedisStore implements AutoCloseable {

    public static final String TOPIC_ID = "kafka_topic_id";
    public static final String OFFSET = "kafka_offset";
    public static final String PARTITION = "kafka_partition";

    /**
     * Same topic id and an offset at or past this one: already stored, skip.
     * A different topic id means the topic was deleted and created again, and
     * its offsets started over, so the new record wins whatever its offset.
     */
    static final String WRITE_IF_NEWER = String.join("\n",
            "local id = redis.call('HGET', KEYS[1], '" + TOPIC_ID + "')",
            "if id == ARGV[1] then",
            "  local stored = tonumber(redis.call('HGET', KEYS[1], '" + OFFSET + "'))",
            "  if stored and stored >= tonumber(ARGV[2]) then return 0 end",
            "end",
            "redis.call('HSET', KEYS[1], '" + TOPIC_ID + "', ARGV[1], '" + OFFSET + "', ARGV[2], unpack(ARGV, 3))",
            "return 1");

    private static final ObjectMapper MAPPER = JsonMapper.builder()
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .nodeFactory(JsonNodeFactory.withExactBigDecimals(true))
            .build();

    private final Jedis jedis;
    private String sha;

    public RedisStore(String host, int port) {
        this.jedis = new Jedis(host, port);
        this.sha = jedis.scriptLoad(WRITE_IF_NEWER);
    }

    /**
     * Writes every entry, pipelined in one round trip.
     *
     * @return how many keys changed; the rest were already at that offset or newer
     */
    public int write(List<LatestPerKey.Latest> entries, Map<String, String> prefixes,
                     Map<String, String> topicIds) {
        if (entries.isEmpty()) {
            return 0;
        }
        try {
            return writeOnce(entries, prefixes, topicIds);
        } catch (JedisDataException e) {
            // A restarted Redis has forgotten the script. Load it again and
            // repeat the batch: every write in it is safe to repeat.
            if (e.getMessage() == null || !e.getMessage().startsWith("NOSCRIPT")) {
                throw e;
            }
            sha = jedis.scriptLoad(WRITE_IF_NEWER);
            return writeOnce(entries, prefixes, topicIds);
        }
    }

    private int writeOnce(List<LatestPerKey.Latest> entries, Map<String, String> prefixes,
                          Map<String, String> topicIds) {
        Pipeline pipeline = jedis.pipelined();
        List<Response<Object>> responses = new ArrayList<>(entries.size());
        for (LatestPerKey.Latest entry : entries) {
            List<String> args = new ArrayList<>(24);
            args.add(topicIds.get(entry.topic()));
            args.add(Long.toString(entry.offset()));
            args.add(PARTITION);
            args.add(Integer.toString(entry.partition()));
            fields(entry.value(), args);
            responses.add(pipeline.evalsha(sha, List.of(redisKey(prefixes, entry)), args));
        }
        pipeline.sync();
        int changed = 0;
        for (Response<Object> response : responses) {
            Object result = response.get();
            if (result instanceof Long n && n == 1L) {
                changed++;
            }
        }
        return changed;
    }

    static String redisKey(Map<String, String> prefixes, LatestPerKey.Latest entry) {
        return prefixes.get(entry.topic()) + ":" + entry.key();
    }

    /**
     * The record's top-level fields as name, value pairs. Decimals are written
     * plainly -- 1234.50, not 1.2345E+3 -- so a price reads in Redis exactly as
     * it does on the topic.
     */
    static void fields(byte[] json, List<String> out) {
        JsonNode node;
        try {
            node = MAPPER.readTree(json);
        } catch (IOException e) {
            throw new IllegalArgumentException("record is not JSON", e);
        }
        Iterator<Map.Entry<String, JsonNode>> it = node.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> field = it.next();
            JsonNode value = field.getValue();
            out.add(field.getKey());
            if (value.isTextual()) {
                out.add(value.asText());
            } else if (value.isBigDecimal()) {
                out.add(value.decimalValue().toPlainString());
            } else if (value.isNull()) {
                out.add("");
            } else {
                out.add(value.toString());
            }
        }
    }

    /**
     * Deletes the keys under a prefix that came from any topic id but this one.
     * Run when a topic is found to have been deleted and created again, so the
     * keys only the old topic had do not linger next to the new topic's values.
     *
     * @return how many keys were deleted
     */
    public long removeOtherTopicIds(String prefix, String topicId) {
        long removed = 0;
        String cursor = ScanParams.SCAN_POINTER_START;
        ScanParams params = new ScanParams().match(prefix + ":*").count(1000);
        do {
            ScanResult<String> page = jedis.scan(cursor, params);
            cursor = page.getCursor();
            List<String> keys = page.getResult();
            if (keys.isEmpty()) {
                continue;
            }
            Pipeline pipeline = jedis.pipelined();
            List<Response<String>> ids = new ArrayList<>(keys.size());
            for (String key : keys) {
                ids.add(pipeline.hget(key, TOPIC_ID));
            }
            pipeline.sync();
            List<String> stale = new ArrayList<>();
            for (int i = 0; i < keys.size(); i++) {
                if (!topicId.equals(ids.get(i).get())) {
                    stale.add(keys.get(i));
                }
            }
            if (!stale.isEmpty()) {
                removed += jedis.unlink(stale.toArray(new String[0]));
            }
        } while (!ScanParams.SCAN_POINTER_START.equals(cursor));
        return removed;
    }

    @Override
    public void close() {
        jedis.close();
    }
}
