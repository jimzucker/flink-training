package st44;

import java.time.Instant;

/**
 * The workload, in one place: what a reading is, which location a sensor
 * belongs to, what hour a timestamp falls in, and how a temperature is derived
 * from (sensor, second, seed).
 *
 * The generator, the manifest and the verifier all read this file, so there is
 * exactly one definition of the expected answer and it comes from the INPUT,
 * never from the pipeline.
 *
 * ANSWERS.md, question 3: 100 locations, 10 sensors each, 1,000 sensors,
 * one reading per sensor per second. The average is per location.
 *
 * ASSUMPTIONS.md B4: location n reads 36 * n seconds ahead of location 0, so
 * the 100 hour boundaries fall at 100 different points in the stream instead of
 * all at once. Nothing else about the pipeline depends on it.
 */
public final class Spec {

    public static final int LOCATIONS = 100;
    public static final int SENSORS_PER_LOCATION = 10;
    public static final int SENSORS = LOCATIONS * SENSORS_PER_LOCATION;   // 1,000
    public static final long HOUR_MS = 3_600_000L;
    /** 2026-01-01T00:00:00Z, and an exact multiple of one hour. */
    public static final long EPOCH_BASE_MS = 1_767_225_600_000L;
    /** ASSUMPTIONS.md B4. 100 locations * 36 s = one hour. */
    public static final int LOCATION_STAGGER_S = 36;
    /** Temperatures are 15.0 .. 40.0 C, always one decimal. ASSUMPTIONS.md B2. */
    public static final int MIN_TENTHS = 150;
    public static final int TENTHS_SPAN = 251;

    private Spec() {
    }

    public static int locationOfSensor(int sensor) {
        return sensor / SENSORS_PER_LOCATION;
    }

    public static String locationName(int location) {
        return "loc-" + (location < 10 ? "0" : "") + location;
    }

    public static String sensorName(int sensor) {
        StringBuilder b = new StringBuilder("sensor-");
        String s = Integer.toString(sensor);
        for (int i = s.length(); i < 4; i++) {
            b.append('0');
        }
        return b.append(s).toString();
    }

    /** Event time of the reading sensor {@code sensor} takes in simulated second {@code s}. */
    public static long eventMillis(int sensor, long s) {
        return EPOCH_BASE_MS + (s + (long) LOCATION_STAGGER_S * locationOfSensor(sensor)) * 1000L;
    }

    /** Absolute hour bucket of an event time. Floor division; every time here is positive. */
    public static long hourOf(long eventMs) {
        return Math.floorDiv(eventMs, HOUR_MS);
    }

    public static String hourStart(long hour) {
        return Instant.ofEpochMilli(hour * HOUR_MS).toString();
    }

    /** Temperature in tenths of a degree. Deterministic in (seed, sensor, second). */
    public static int tenths(long seed, int sensor, long s) {
        long h = mix(mix(seed * 0x2545F4914F6CDD1DL + sensor) + s * 0x9E3779B97F4A7C15L);
        return MIN_TENTHS + (int) Math.floorMod(h, (long) TENTHS_SPAN);
    }

    private static long mix(long z) {
        z += 0x9E3779B97F4A7C15L;
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }

    /** Tenths rendered as a decimal with exactly one fractional digit. */
    public static String decimal(long tenths) {
        boolean neg = tenths < 0;
        long a = Math.abs(tenths);
        return (neg ? "-" : "") + (a / 10) + "." + (a % 10);
    }

    /**
     * The inverse of {@link #decimal}, exactly: no double anywhere on the path,
     * so the verifier compares integers and question 4's "no rounding to argue
     * about" is a fact rather than a hope.
     */
    public static long parseTenths(String text) {
        int n = text.length();
        boolean neg = n > 0 && text.charAt(0) == '-';
        int i = neg ? 1 : 0;
        long whole = 0;
        for (; i < n && text.charAt(i) != '.'; i++) {
            char ch = text.charAt(i);
            if (ch < '0' || ch > '9') {
                throw new NumberFormatException("not a decimal: " + text);
            }
            whole = whole * 10 + (ch - '0');
        }
        long frac = 0;
        if (i < n && text.charAt(i) == '.') {
            i++;
            if (i >= n) {
                throw new NumberFormatException("not a decimal: " + text);
            }
            frac = text.charAt(i) - '0';
            if (frac < 0 || frac > 9) {
                throw new NumberFormatException("not a decimal: " + text);
            }
            if (i + 1 != n) {
                throw new NumberFormatException("expected exactly one decimal digit: " + text);
            }
        } else {
            throw new NumberFormatException("expected exactly one decimal digit: " + text);
        }
        long v = whole * 10 + frac;
        return neg ? -v : v;
    }

    /** Simple `--name=value` argument lookup, used by every entry point here. */
    public static String arg(String[] args, String name, String dflt) {
        String prefix = "--" + name + "=";
        for (String a : args) {
            if (a.startsWith(prefix)) {
                return a.substring(prefix.length());
            }
        }
        if (dflt == null) {
            throw new IllegalArgumentException("missing required argument --" + name + "=");
        }
        return dflt;
    }
}
