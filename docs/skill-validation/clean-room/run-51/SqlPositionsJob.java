package scaletest;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * The default business case, in Flink SQL.
 *
 * orders -> positions by symbol (1 per order) and by account/sub/symbol (1 per
 * allocation, 4 per order); prices joined to both running positions and
 * emitted as market values on a processing-time tumbling window (10 s by
 * default). All four INSERTs go in one STATEMENT SET so the orders topic is
 * scanned once.
 */
public class SqlPositionsJob {

    static Map<String, String> parse(String[] args) {
        Map<String, String> m = new HashMap<>();
        for (String a : args) {
            if (a.startsWith("--")) {
                int i = a.indexOf('=');
                if (i > 0) m.put(a.substring(2, i), a.substring(i + 1));
                else m.put(a.substring(2), "true");
            }
        }
        return m;
    }

    static String need(Map<String, String> a, String k) {
        String v = a.get(k);
        if (v == null || v.isEmpty()) throw new IllegalArgumentException("missing --" + k);
        return v;
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> a = parse(args);
        String bootstrap = need(a, "bootstrap");
        String in = need(a, "in");
        String outSym = need(a, "outSym");
        String outAcct = need(a, "outAcct");
        String mvSym = need(a, "mvSym");
        String mvAcct = need(a, "mvAcct");
        String prices = a.getOrDefault("prices", "prices");
        String group = need(a, "group");
        int par = Integer.parseInt(need(a, "parallelism"));
        long ckptMs = Long.parseLong(a.getOrDefault("checkpointMs", "10000"));
        int mvIntervalS = Integer.parseInt(a.getOrDefault("mvIntervalS", "10"));
        boolean explainOnly = a.containsKey("explainOnly");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(par);
        env.enableCheckpointing(ckptMs, CheckpointingMode.EXACTLY_ONCE);
        StreamTableEnvironment t = StreamTableEnvironment.create(env);
        t.getConfig().set("pipeline.name", "positions-sql " + group);
        // one scan of the source, shared by every INSERT
        t.getConfig().set("table.optimizer.reuse-source-enabled", "true");
        t.getConfig().set("table.optimizer.reuse-sub-plan-enabled", "true");
        // one output per input: no mini-batch folding of updates
        t.getConfig().set("table.exec.mini-batch.enabled", "false");

        String kafka = "'connector' = 'kafka', 'properties.bootstrap.servers' = '" + bootstrap + "', ";

        t.executeSql("CREATE TABLE orders (" +
                " orderId STRING, symbol STRING, qty BIGINT," +
                " allocations ARRAY<ROW<account STRING, sub STRING, qty BIGINT>>," +
                " pt AS PROCTIME()" +
                ") WITH (" + kafka +
                " 'topic' = '" + in + "'," +
                " 'properties.group.id' = '" + group + "'," +
                " 'properties.auto.offset.reset' = 'earliest'," +
                " 'properties.commit.offsets.on.checkpoint' = 'true'," +
                " 'scan.startup.mode' = 'group-offsets'," +
                " 'format' = 'json', 'json.fail-on-missing-field' = 'true')");

        t.executeSql("CREATE TABLE prices (" +
                " symbol STRING, ts BIGINT, price BIGINT" +
                ") WITH (" + kafka +
                " 'topic' = '" + prices + "'," +
                " 'properties.group.id' = '" + group + "-prices'," +
                " 'properties.commit.offsets.on.checkpoint' = 'false'," +
                " 'scan.startup.mode' = 'earliest-offset'," +
                " 'format' = 'json')");

        String upsert = "'connector' = 'upsert-kafka', 'properties.bootstrap.servers' = '" + bootstrap + "', " +
                "'key.format' = 'json', 'value.format' = 'json', ";

        t.executeSql("CREATE TABLE positions_by_symbol (" +
                " symbol STRING, `position` BIGINT, updates BIGINT," +
                " PRIMARY KEY (symbol) NOT ENFORCED" +
                ") WITH (" + upsert + " 'topic' = '" + outSym + "')");
        t.executeSql("CREATE TABLE positions_by_account (" +
                " account STRING, sub_account STRING, symbol STRING, `position` BIGINT, updates BIGINT," +
                " PRIMARY KEY (account, sub_account, symbol) NOT ENFORCED" +
                ") WITH (" + upsert + " 'topic' = '" + outAcct + "')");
        t.executeSql("CREATE TABLE market_values_by_symbol (" +
                " symbol STRING, `position` BIGINT, price BIGINT, market_value BIGINT, updates BIGINT," +
                " PRIMARY KEY (symbol) NOT ENFORCED" +
                ") WITH (" + upsert + " 'topic' = '" + mvSym + "')");
        t.executeSql("CREATE TABLE market_values_by_account (" +
                " account STRING, sub_account STRING, symbol STRING, `position` BIGINT, price BIGINT," +
                " market_value BIGINT, updates BIGINT," +
                " PRIMARY KEY (account, sub_account, symbol) NOT ENFORCED" +
                ") WITH (" + upsert + " 'topic' = '" + mvAcct + "')");

        // one row per allocation
        t.executeSql("CREATE TEMPORARY VIEW allocations_flat AS" +
                " SELECT o.symbol, al.account, al.sub AS sub_account, al.qty AS aqty, o.pt" +
                " FROM orders AS o CROSS JOIN UNNEST(o.allocations) AS al (account, sub, qty)");

        // latest price per symbol, by timestamp
        t.executeSql("CREATE TEMPORARY VIEW latest_price AS" +
                " SELECT symbol, price FROM (" +
                "  SELECT symbol, price, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY ts DESC) AS rn" +
                "  FROM prices) WHERE rn = 1");

        String win = "INTERVAL '" + mvIntervalS + "' SECOND";

        // market value, by symbol: throttle the position changes into windows,
        // keep a running sum of the windows, join the latest price
        t.executeSql("CREATE TEMPORARY VIEW mv_position_by_symbol AS" +
                " SELECT symbol, SUM(wq) AS `position`, SUM(wc) AS updates FROM (" +
                "  SELECT symbol, SUM(qty) AS wq, COUNT(*) AS wc" +
                "  FROM TABLE(TUMBLE(TABLE orders, DESCRIPTOR(pt), " + win + "))" +
                "  GROUP BY symbol, window_start, window_end)" +
                " GROUP BY symbol");
        t.executeSql("CREATE TEMPORARY VIEW mv_position_by_account AS" +
                " SELECT account, sub_account, symbol, SUM(wq) AS `position`, SUM(wc) AS updates FROM (" +
                "  SELECT account, sub_account, symbol, SUM(aqty) AS wq, COUNT(*) AS wc" +
                "  FROM TABLE(TUMBLE(TABLE allocations_flat, DESCRIPTOR(pt), " + win + "))" +
                "  GROUP BY account, sub_account, symbol, window_start, window_end)" +
                " GROUP BY account, sub_account, symbol");

        StatementSet set = t.createStatementSet();
        set.addInsertSql("INSERT INTO positions_by_symbol" +
                " SELECT symbol, SUM(qty), COUNT(*) FROM orders GROUP BY symbol");
        set.addInsertSql("INSERT INTO positions_by_account" +
                " SELECT account, sub_account, symbol, SUM(aqty), COUNT(*) FROM allocations_flat" +
                " GROUP BY account, sub_account, symbol");
        set.addInsertSql("INSERT INTO market_values_by_symbol" +
                " SELECT p.symbol, p.`position`, lp.price, p.`position` * lp.price, p.updates" +
                " FROM mv_position_by_symbol AS p JOIN latest_price AS lp ON p.symbol = lp.symbol");
        set.addInsertSql("INSERT INTO market_values_by_account" +
                " SELECT p.account, p.sub_account, p.symbol, p.`position`, lp.price," +
                " p.`position` * lp.price, p.updates" +
                " FROM mv_position_by_account AS p JOIN latest_price AS lp ON p.symbol = lp.symbol");

        System.out.println("===== EXPLAIN =====");
        System.out.println(set.explain());
        if (explainOnly) return;
        set.execute();
    }
}
