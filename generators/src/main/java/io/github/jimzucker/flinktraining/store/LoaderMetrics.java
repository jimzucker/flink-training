package io.github.jimzucker.flinktraining.store;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * What the loader has done, as a Prometheus endpoint on {@code /metrics}.
 *
 * <p>The number that matters is the lag: records on the output topics that are
 * not in Redis yet. It is how the dashboard shows the store keeping up, or
 * falling behind while Flink carries on at full speed -- which is the point of
 * loading from the topics rather than from inside the job.
 */
public final class LoaderMetrics implements AutoCloseable {

    private final Map<String, LongAdder> read = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> written = new ConcurrentHashMap<>();
    private final Map<String, Long> lag = new ConcurrentHashMap<>();
    private final LongAdder redisErrors = new LongAdder();
    private final HttpServer server;

    public LoaderMetrics(int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/metrics", exchange -> {
            byte[] body = render().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "text/plain; version=0.0.4");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        server.start();
    }

    public void read(String topic, long n) {
        read.computeIfAbsent(topic, t -> new LongAdder()).add(n);
    }

    public void written(String topic, long n) {
        written.computeIfAbsent(topic, t -> new LongAdder()).add(n);
    }

    /** One thread's lag on one topic; the endpoint sums the threads. */
    public void lag(String thread, String topic, long records) {
        lag.put(thread + '\u0000' + topic, records);
    }

    public void clearLag(String thread) {
        lag.keySet().removeIf(k -> k.startsWith(thread + '\u0000'));
    }

    public void redisError() {
        redisErrors.increment();
    }

    String render() {
        StringBuilder out = new StringBuilder();
        counter(out, "store_loader_records_read_total",
                "Records read from the output topics, including ones a later record for the same key replaced", read);
        counter(out, "store_loader_keys_written_total",
                "Keys whose value in Redis changed", written);
        Map<String, Long> lagByTopic = new TreeMap<>();
        lag.forEach((k, v) -> lagByTopic.merge(k.substring(k.indexOf('\u0000') + 1), v, Long::sum));
        out.append("# HELP store_loader_lag_records Records on the topic not yet in Redis\n");
        out.append("# TYPE store_loader_lag_records gauge\n");
        lagByTopic.forEach((topic, n) ->
                out.append("store_loader_lag_records{topic=\"").append(topic).append("\"} ").append(n).append('\n'));
        out.append("# HELP store_loader_redis_errors_total Batches Redis did not accept; each is read again\n");
        out.append("# TYPE store_loader_redis_errors_total counter\n");
        out.append("store_loader_redis_errors_total ").append(redisErrors.sum()).append('\n');
        return out.toString();
    }

    private static void counter(StringBuilder out, String name, String help, Map<String, LongAdder> values) {
        out.append("# HELP ").append(name).append(' ').append(help).append('\n');
        out.append("# TYPE ").append(name).append(" counter\n");
        new TreeMap<>(values).forEach((topic, n) ->
                out.append(name).append("{topic=\"").append(topic).append("\"} ").append(n.sum()).append('\n'));
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
