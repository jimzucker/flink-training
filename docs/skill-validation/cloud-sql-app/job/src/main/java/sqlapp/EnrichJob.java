package sqlapp;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * The laptop side of the one app that runs on both platforms. It writes the two
 * tables the way the laptop connects them to Kafka, then runs enrich.sql -- the
 * same INSERT that Confluent Cloud runs as platform.jobSql. Only the tables
 * differ by platform.
 */
public final class EnrichJob {
    public static void main(String[] args) throws Exception {
        Map<String, String> a = parse(args);
        String bootstrap = a.get("bootstrap"), in = a.get("in"), out = a.getOrDefault("out", "enriched");
        String group = a.get("group");
        TableEnvironment t = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        t.getConfig().set("parallelism.default", a.get("parallelism"));
        t.getConfig().set("execution.checkpointing.interval", a.get("checkpointMs") + " ms");
        t.getConfig().set("execution.checkpointing.mode", "EXACTLY_ONCE");
        t.getConfig().set("pipeline.name", "sqlapp-enrich");
        t.executeSql("CREATE TABLE `" + in + "` (order_id STRING, symbol INT, account INT, qty INT, price DOUBLE) WITH ("
                + "'connector' = 'kafka', 'topic' = '" + in + "', 'properties.bootstrap.servers' = '" + bootstrap + "', "
                + "'properties.group.id' = '" + group + "', 'scan.startup.mode' = 'group-offsets', "
                + "'properties.auto.offset.reset' = 'earliest', 'properties.commit.offsets.on.checkpoint' = 'true', "
                + "'format' = 'json')");
        t.executeSql("CREATE TABLE `" + out + "` (order_id STRING, symbol INT, account INT, qty INT, price DOUBLE, "
                + "notional DOUBLE) WITH ('connector' = 'kafka', 'topic' = '" + out + "', "
                + "'properties.bootstrap.servers' = '" + bootstrap + "', 'format' = 'json', "
                + "'sink.delivery-guarantee' = 'exactly-once', 'sink.transactional-id-prefix' = '" + group + "', "
                + "'properties.transaction.timeout.ms' = '600000')");
        t.executeSql(sql().replace("{in}", in));
    }

    /** The INSERT both platforms run, as committed in enrich.sql. */
    static String sql() throws Exception {
        try (InputStream s = EnrichJob.class.getResourceAsStream("/enrich.sql")) {
            return new String(s.readAllBytes(), StandardCharsets.UTF_8).trim();
        }
    }

    static Map<String, String> parse(String[] args) {
        Map<String, String> m = new HashMap<>();
        for (String x : args) {
            int i = x.indexOf('=');
            if (x.startsWith("--") && i > 2) m.put(x.substring(2, i), x.substring(i + 1));
        }
        return m;
    }
}
