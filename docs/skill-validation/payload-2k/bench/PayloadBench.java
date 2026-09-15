import io.github.jimzucker.flinktraining.generator.BlockTradeGenerator;
import io.github.jimzucker.flinktraining.job.PositionUpdate;
import io.github.jimzucker.flinktraining.job.SplitByAllocation;
import io.github.jimzucker.flinktraining.job.ToSymbolUpdate;
import io.github.jimzucker.flinktraining.model.BlockTrade;
import io.github.jimzucker.flinktraining.model.Json;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.util.Collector;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/**
 * The job's per-order CPU work, with no Kafka and no Flink runtime, on T threads.
 * Modes:
 *   full   - both flatMaps (JSON parsed twice) + Flink's POJO serializer on every PositionUpdate
 *   parse  - both flatMaps only
 *   json   - one Json.fromJson(BlockTrade) per order
 * args: fillerFields threads warmupSeconds measureSeconds mode
 */
public class PayloadBench {
    public static void main(String[] a) throws Exception {
        int filler = Integer.parseInt(a[0]);
        int threads = Integer.parseInt(a[1]);
        int warm = Integer.parseInt(a[2]);
        int measure = Integer.parseInt(a[3]);
        String mode = a[4];

        int n = 20_000;
        String[] orders = new String[n];
        long bytes = 0;
        for (int i = 0; i < n; i++) {
            orders[i] = Json.toJson(BlockTradeGenerator.at(20260910L, i, 1789000000000L + i, filler));
            bytes += orders[i].length();
        }
        TypeSerializer<PositionUpdate> base = TypeInformation.of(PositionUpdate.class).createSerializer(new ExecutionConfig());

        LongAdder done = new LongAdder();
        AtomicBoolean counting = new AtomicBoolean(false);
        AtomicBoolean stop = new AtomicBoolean(false);
        CountDownLatch go = new CountDownLatch(1);
        Thread[] ts = new Thread[threads];
        for (int t = 0; t < threads; t++) {
            final int offset = t * (n / Math.max(1, threads));
            ts[t] = new Thread(() -> {
                TypeSerializer<PositionUpdate> ser = base.duplicate();
                DataOutputSerializer out = new DataOutputSerializer(8192);
                ToSymbolUpdate sym = new ToSymbolUpdate();
                SplitByAllocation split = new SplitByAllocation();
                boolean serialize = mode.equals("full");
                Collector<PositionUpdate> c = new Collector<>() {
                    public void collect(PositionUpdate u) {
                        if (serialize) {
                            try { ser.serialize(u, out); out.clear(); } catch (Exception e) { throw new RuntimeException(e); }
                        }
                    }
                    public void close() { }
                };
                try { go.await(); } catch (InterruptedException e) { return; }
                long local = 0;
                int i = offset;
                while (!stop.get()) {
                    String json = orders[i];
                    if (mode.equals("json")) {
                        Json.fromJson(json, BlockTrade.class);
                    } else {
                        sym.flatMap(json, c);
                        split.flatMap(json, c);
                    }
                    if (++i == orders.length) i = 0;
                    if (counting.get()) done.increment();
                }
            });
            ts[t].setDaemon(true);
            ts[t].start();
        }
        go.countDown();
        Thread.sleep(warm * 1000L);
        long gc0 = gcMillis();
        long t0 = System.nanoTime();
        counting.set(true);
        Thread.sleep(measure * 1000L);
        counting.set(false);
        long elapsedNs = System.nanoTime() - t0;
        long gc1 = gcMillis();
        stop.set(true);
        double secs = elapsedNs / 1e9;
        double rate = done.sum() / secs;
        System.out.printf("RESULT filler=%d bytesPerOrder=%.1f threads=%d cpus=%d mode=%s orders/s=%.0f perThread=%.0f gcPctOfWall=%.2f%n",
                filler, (double) bytes / n, threads, Runtime.getRuntime().availableProcessors(), mode,
                rate, rate / threads, 100.0 * (gc1 - gc0) / (secs * 1000.0));
    }

    private static long gcMillis() {
        long s = 0;
        for (GarbageCollectorMXBean b : ManagementFactory.getGarbageCollectorMXBeans()) {
            s += Math.max(0, b.getCollectionTime());
        }
        return s;
    }
}
