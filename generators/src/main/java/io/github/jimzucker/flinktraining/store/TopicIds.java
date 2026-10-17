package io.github.jimzucker.flinktraining.store;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.Uuid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * The current id of each output topic, as the broker reports it.
 *
 * <p>The scale scripts delete and recreate the output topics between cases, and
 * a recreated topic starts its offsets again at zero. The id is the only thing
 * that tells the old topic from the new one: offsets alone would make every
 * record of the new topic look older than what is already stored.
 */
public final class TopicIds implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(TopicIds.class);

    private final Admin admin;
    private final Collection<String> topics;
    private final Map<String, String> current = new ConcurrentHashMap<>();

    public TopicIds(String bootstrapServers, Collection<String> topics) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        this.admin = Admin.create(props);
        this.topics = topics;
    }

    /** Asks the broker again. A topic that does not exist right now is left out. */
    public synchronized void refresh() {
        Map<String, String> fresh = new HashMap<>();
        for (String topic : topics) {
            try {
                Uuid id = admin.describeTopics(java.util.List.of(topic))
                        .topicNameValues().get(topic).get(10, TimeUnit.SECONDS).topicId();
                fresh.put(topic, id.toString());
            } catch (Exception e) {
                // Between a delete and the create that follows it. Normal during
                // the scale scripts; the next refresh picks the new id up.
                LOG.debug("topic {} not described: {}", topic, e.toString());
            }
        }
        current.keySet().retainAll(fresh.keySet());
        current.putAll(fresh);
    }

    /** The id for a topic, or null if it does not exist at the last refresh. */
    public String get(String topic) {
        return current.get(topic);
    }

    @Override
    public void close() {
        admin.close();
    }
}
