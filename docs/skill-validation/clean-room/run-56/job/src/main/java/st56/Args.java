package st56;

import java.util.HashMap;
import java.util.Map;

/** Reads --key=value and --key value alike (Flink's ParameterTool takes only the second). */
public final class Args {
    private Args() {}

    public static Map<String, String> parse(String[] argv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < argv.length; i++) {
            String s = argv[i];
            if (!s.startsWith("--")) throw new IllegalArgumentException("unexpected argument: " + s);
            s = s.substring(2);
            int eq = s.indexOf('=');
            if (eq >= 0) {
                m.put(s.substring(0, eq), s.substring(eq + 1));
            } else if (i + 1 < argv.length && !argv[i + 1].startsWith("--")) {
                m.put(s, argv[++i]);
            } else {
                m.put(s, "true");
            }
        }
        return m;
    }

    public static String need(Map<String, String> m, String k) {
        String v = m.get(k);
        if (v == null || v.isEmpty()) throw new IllegalArgumentException("missing --" + k);
        return v;
    }
}
