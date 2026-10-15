package st55;

import java.util.HashMap;
import java.util.Map;

/** --name value or --name=value. */
public final class Args {
    private final Map<String, String> m = new HashMap<>();

    public Args(String[] a) {
        for (int i = 0; i < a.length; i++) {
            String s = a[i];
            if (!s.startsWith("--")) throw new IllegalArgumentException("unexpected argument " + s);
            s = s.substring(2);
            int eq = s.indexOf('=');
            if (eq >= 0) {
                m.put(s.substring(0, eq), s.substring(eq + 1));
            } else {
                if (i + 1 >= a.length) throw new IllegalArgumentException("--" + s + " has no value");
                m.put(s, a[++i]);
            }
        }
    }

    public String get(String k) {
        String v = m.get(k);
        if (v == null) throw new IllegalArgumentException("missing --" + k);
        return v;
    }

    public String get(String k, String dflt) {
        return m.getOrDefault(k, dflt);
    }

    public long getLong(String k) { return Long.parseLong(get(k)); }

    public long getLong(String k, long d) { return m.containsKey(k) ? Long.parseLong(m.get(k)) : d; }

    public double getDouble(String k, double d) { return m.containsKey(k) ? Double.parseDouble(m.get(k)) : d; }
}
