package io.github.jimzucker.flinktraining.job;

/** Minute-boundary arithmetic, in one place so the two sides cannot disagree. */
public final class Windows {

    public static final long ONE_MINUTE_MILLIS = 60_000L;

    private Windows() {
    }

    /** The first minute boundary strictly after {@code timestamp}. */
    public static long nextBoundary(long timestamp, long windowMillis) {
        return Math.floorDiv(timestamp, windowMillis) * windowMillis + windowMillis;
    }
}
