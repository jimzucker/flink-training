package st50;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.HashMap;
import java.util.Map;

/**
 * The default business case, in Flink SQL. The Java here only reads arguments,
 * sets the checkpoint and submits SQL: every operator in the running job is
 * chosen by the planner.
 *
 * Inputs:  orders (the harness's), prices (the pipeline's own).
 * Outputs: positions-by-symbol, positions-by-account (one row per input each,
 *          1 + 4 = 5 per order), market-values-by-symbol and
 *          market-values-by-account (throttled by a processing-time window).
 *
 * --explain prints the optimized plan with changelog modes and exits without
 * submitting anything, so the plan can be read before any stack exists.
 */
public final class PositionsSqlJob {

    static Map<String, String> args(String[] a) {
        Map<String, String> m = new HashMap<>();
        for (String s : a) {
            if (s.startsWith("--")) {
                int eq = s.indexOf('=');
                if (eq < 0) m.put(s.substring(2), "true");
                else m.put(s.substring(2, eq), s.substring(eq + 1));
            }
        }
        return m;
    }

    static String req(Map<String, String> m, String k) {
        String v = m.get(k);
        if (v == null) throw new IllegalArgumentException("missing --" + k);
        return v;
    }

    public static void main(String[] argv) throws Exception {
        Map<String, String> a = args(argv);
        String bootstrap = req(a, "bootstrap");
        String topicIn = req(a, "topicIn");
        String topicSym = req(a, "topicSym");
        String topicAcct = req(a, "topicAcct");
        String topicMvSym = req(a, "topicMvSym");
        String topicMvAcct = req(a, "topicMvAcct");
        String topicPrices = a.getOrDefault("topicPrices", "prices");
        String group = req(a, "group");
        int par = Integer.parseInt(req(a, "parallelism"));
        long ckptMs = Long.parseLong(req(a, "checkpointMs"));
        int mvIntervalS = Integer.parseInt(a.getOrDefault("mvIntervalS", "10"));
        boolean explain = a.containsKey("explain");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(par);
        env.enableCheckpointing(ckptMs, CheckpointingMode.EXACTLY_ONCE);
        StreamTableEnvironment t = StreamTableEnvironment.create(env);
        t.getConfig().set("pipeline.name", "st50-positions-sql");
        t.getConfig().set("parallelism.default", String.valueOf(par));

        String kafkaCommon = "'properties.bootstrap.servers' = '" + bootstrap + "'";

        t.executeSql(
            "CREATE TABLE orders (\n"
          + "  id BIGINT, sym STRING, qty BIGINT, ts BIGINT,\n"
          + "  allocs ARRAY<ROW<acct STRING, sub STRING, qty BIGINT>>,\n"
          + "  pt AS PROCTIME()\n"
          + ") WITH (\n"
          + "  'connector' = 'kafka', 'topic' = '" + topicIn + "', " + kafkaCommon + ",\n"
          + "  'properties.group.id' = '" + group + "',\n"
          + "  'scan.startup.mode' = 'group-offsets',\n"
          + "  'properties.auto.offset.reset' = 'earliest',\n"
          + "  'format' = 'json', 'json.fail-on-missing-field' = 'true', 'json.ignore-parse-errors' = 'false'\n"
          + ")");

        t.executeSql(
            "CREATE TABLE prices (sym STRING, ts BIGINT, px DECIMAL(10, 2)) WITH (\n"
          + "  'connector' = 'kafka', 'topic' = '" + topicPrices + "', " + kafkaCommon + ",\n"
          + "  'properties.group.id' = '" + group + "-prices',\n"
          + "  'scan.startup.mode' = 'earliest-offset',\n"
          + "  'format' = 'json'\n"
          + ")");

        String upsert = "'connector' = 'upsert-kafka', " + kafkaCommon + ",\n"
          + "  'key.format' = 'json', 'value.format' = 'json',\n"
          + "  'properties.compression.type' = 'lz4', 'properties.linger.ms' = '20',\n"
          + "  'properties.batch.size' = '262144'";

        t.executeSql("CREATE TABLE positions_by_symbol (sym STRING, pos BIGINT, orders BIGINT,"
          + " PRIMARY KEY (sym) NOT ENFORCED) WITH ('topic' = '" + topicSym + "', " + upsert + ")");
        t.executeSql("CREATE TABLE positions_by_account (acct STRING, sub STRING, sym STRING, pos BIGINT, orders BIGINT,"
          + " PRIMARY KEY (acct, sub, sym) NOT ENFORCED) WITH ('topic' = '" + topicAcct + "', " + upsert + ")");
        t.executeSql("CREATE TABLE market_values_by_symbol (sym STRING, pos BIGINT, px DECIMAL(10, 2),"
          + " mv DECIMAL(38, 2), orders BIGINT,"
          + " PRIMARY KEY (sym) NOT ENFORCED) WITH ('topic' = '" + topicMvSym + "', " + upsert + ")");
        t.executeSql("CREATE TABLE market_values_by_account (acct STRING, sub STRING, sym STRING, pos BIGINT,"
          + " px DECIMAL(10, 2), mv DECIMAL(38, 2), orders BIGINT,"
          + " PRIMARY KEY (acct, sub, sym) NOT ENFORCED) WITH ('topic' = '" + topicMvAcct + "', " + upsert + ")");

        // one row per allocation, off the same scan
        t.executeSql("CREATE TEMPORARY VIEW allocations AS\n"
          + "SELECT o.sym, a.acct, a.sub, a.qty, o.pt\n"
          + "FROM orders AS o CROSS JOIN UNNEST(o.allocs) AS a (acct, sub, qty)");

        // latest price per symbol, by the price's own timestamp
        t.executeSql("CREATE TEMPORARY VIEW latest_price AS\n"
          + "SELECT sym, px FROM (\n"
          + "  SELECT sym, px, ROW_NUMBER() OVER (PARTITION BY sym ORDER BY ts DESC) AS rn FROM prices)\n"
          + "WHERE rn = 1");

        // throttle: one delta per key per processing-time window, then the running position
        String iv = "INTERVAL '" + mvIntervalS + "' SECOND";
        t.executeSql("CREATE TEMPORARY VIEW symbol_window_position AS\n"
          + "SELECT sym, SUM(dq) AS pos, SUM(dn) AS orders FROM (\n"
          + "  SELECT window_start, window_end, sym, SUM(qty) AS dq, COUNT(*) AS dn\n"
          + "  FROM TABLE(TUMBLE(TABLE orders, DESCRIPTOR(pt), " + iv + "))\n"
          + "  GROUP BY window_start, window_end, sym)\n"
          + "GROUP BY sym");
        t.executeSql("CREATE TEMPORARY VIEW account_window_position AS\n"
          + "SELECT acct, sub, sym, SUM(dq) AS pos, SUM(dn) AS orders FROM (\n"
          + "  SELECT window_start, window_end, acct, sub, sym, SUM(qty) AS dq, COUNT(*) AS dn\n"
          + "  FROM TABLE(TUMBLE(TABLE allocations, DESCRIPTOR(pt), " + iv + "))\n"
          + "  GROUP BY window_start, window_end, acct, sub, sym)\n"
          + "GROUP BY acct, sub, sym");

        StatementSet s = t.createStatementSet();
        s.addInsertSql("INSERT INTO positions_by_symbol\n"
          + "SELECT sym, SUM(qty), COUNT(*) FROM orders GROUP BY sym");
        s.addInsertSql("INSERT INTO positions_by_account\n"
          + "SELECT acct, sub, sym, SUM(qty), COUNT(*) FROM allocations GROUP BY acct, sub, sym");
        s.addInsertSql("INSERT INTO market_values_by_symbol\n"
          + "SELECT w.sym, w.pos, p.px, CAST(w.pos * p.px AS DECIMAL(38, 2)), w.orders\n"
          + "FROM symbol_window_position AS w JOIN latest_price AS p ON w.sym = p.sym");
        s.addInsertSql("INSERT INTO market_values_by_account\n"
          + "SELECT w.acct, w.sub, w.sym, w.pos, p.px, CAST(w.pos * p.px AS DECIMAL(38, 2)), w.orders\n"
          + "FROM account_window_position AS w JOIN latest_price AS p ON w.sym = p.sym");

        if (explain) {
            System.out.println(s.explain(org.apache.flink.table.api.ExplainDetail.CHANGELOG_MODE));
            return;
        }
        s.execute();   // detached under `flink run -d`: returns once submitted
    }
}
