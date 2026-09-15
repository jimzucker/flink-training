import io.github.jimzucker.flinktraining.generator.BlockTradeGenerator;
import io.github.jimzucker.flinktraining.job.PositionUpdate;
import io.github.jimzucker.flinktraining.job.SplitByAllocation;
import io.github.jimzucker.flinktraining.job.ToSymbolUpdate;
import io.github.jimzucker.flinktraining.model.BlockTrade;
import io.github.jimzucker.flinktraining.model.Json;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.CloseableIterator;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * One stage of the positions job's per-order path, timed alone on one thread, in its own JVM.
 * Every stage is reported as nanoseconds per ORDER (a stage that handles 5 updates per order
 * counts all 5).
 *
 *   fetch   Kafka lz4 batch (producer's 262144 batch.size) decompressed, each value copied to byte[]
 *   decode  byte[] -> String (SimpleStringSchema)
 *   parse   one Json.fromJson(String, BlockTrade)
 *   symbol  ToSymbolUpdate.flatMap   (parse + 1 update)
 *   split   SplitByAllocation.flatMap (parse + 4 updates, carrying filler)
 *   ser     Flink POJO serializer on the order's 5 updates (network output)
 *   deser   Flink POJO deserializer on those 5 updates (network input at the aggregations)
 *
 * args: fillerFields stage warmupSeconds measureSeconds
 */
public class StageBench {
    static volatile long sink;

    /** Same features as model.Json, but unknown properties (the filler) are skipped, not bound. */
    static final ObjectMapper LEAN = JsonMapper.builder()
            .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    @JsonIgnoreProperties(ignoreUnknown = true)
    record LeanAllocation(String account, String subAccount, long quantity) { }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record LeanTrade(String tradeId, String symbol, String side, long quantity,
                     List<LeanAllocation> allocations, long eventTime) { }

    public static void main(String[] a) throws Exception {
        int filler = Integer.parseInt(a[0]);
        String stage = a[1];
        int warm = Integer.parseInt(a[2]);
        int measure = Integer.parseInt(a[3]);

        int n = 20_000;
        String[] json = new String[n];
        byte[][] bytes = new byte[n][];
        long totalBytes = 0;
        for (int i = 0; i < n; i++) {
            json[i] = Json.toJson(BlockTradeGenerator.at(20260910L, i, 1789000000000L + i, filler));
            bytes[i] = json[i].getBytes(StandardCharsets.UTF_8);
            totalBytes += bytes[i].length;
        }

        TypeSerializer<PositionUpdate> ser = TypeInformation.of(PositionUpdate.class).createSerializer(new ExecutionConfig());
        ToSymbolUpdate sym = new ToSymbolUpdate();
        SplitByAllocation split = new SplitByAllocation();

        // the 5 updates each order produces, and their serialized form
        List<PositionUpdate[]> updates = new ArrayList<>(n);
        List<byte[]> wire = new ArrayList<>(n);
        DataOutputSerializer out = new DataOutputSerializer(16384);
        long wireBytes = 0;
        for (int i = 0; i < n; i++) {
            List<PositionUpdate> u = new ArrayList<>(5);
            Collector<PositionUpdate> c = collectorInto(u);
            sym.flatMap(json[i], c);
            split.flatMap(json[i], c);
            updates.add(u.toArray(new PositionUpdate[0]));
            out.clear();
            for (PositionUpdate x : u) ser.serialize(x, out);
            wire.add(out.getCopyOfBuffer());
            wireBytes += out.length();
        }

        // Kafka batches the way the producer builds them: fill to batch.size by its own estimate
        List<ByteBuffer> batches = new ArrayList<>();
        List<Integer> batchCounts = new ArrayList<>();
        long compressedBytes = 0;
        int i = 0;
        while (i < n) {
            MemoryRecordsBuilder b = MemoryRecords.builder(ByteBuffer.allocate(4 * 1024 * 1024),
                    Compression.lz4().build(), TimestampType.CREATE_TIME, 0L);
            int count = 0;
            while (i < n && b.estimatedSizeInBytes() < 262144) {
                b.append(1789000000000L + i, null, bytes[i]);
                i++; count++;
            }
            MemoryRecords r = b.build();
            ByteBuffer buf = r.buffer();
            compressedBytes += buf.remaining();
            batches.add(buf);
            batchCounts.add(count);
        }

        Collector<PositionUpdate> discard = new Collector<>() {
            public void collect(PositionUpdate u) { sink ^= u.signedQuantity; }
            public void close() { }
        };
        BufferSupplier supplier = BufferSupplier.create();
        DataInputDeserializer in = new DataInputDeserializer();

        long[] result = new long[2]; // orders, nanos
        for (int phase = 0; phase < 2; phase++) {
            long seconds = phase == 0 ? warm : measure;
            long orders = 0;
            long t0 = System.nanoTime();
            long end = t0 + seconds * 1_000_000_000L;
            int k = 0;
            while (System.nanoTime() < end) {
                for (int rep = 0; rep < 256; rep++) {
                    switch (stage) {
                        case "fetch": {
                            ByteBuffer buf = batches.get(k % batches.size()).duplicate();
                            for (MutableRecordBatch batch : MemoryRecords.readableRecords(buf).batches()) {
                                try (CloseableIterator<Record> it = batch.streamingIterator(supplier)) {
                                    while (it.hasNext()) {
                                        ByteBuffer v = it.next().value();
                                        byte[] copy = new byte[v.remaining()];
                                        v.get(copy);
                                        sink ^= copy.length;
                                    }
                                }
                            }
                            orders += batchCounts.get(k % batches.size());
                            break;
                        }
                        case "decode":
                            sink ^= new String(bytes[k % n], StandardCharsets.UTF_8).length(); orders++; break;
                        case "parse":
                            sink ^= Json.fromJson(json[k % n], BlockTrade.class).quantity(); orders++; break;
                        case "tokens": {
                            try (JsonParser p = LEAN.getFactory().createParser(json[k % n])) {
                                int t = 0;
                                while (p.nextToken() != null) t++;
                                sink ^= t;
                            }
                            orders++; break;
                        }
                        case "skip":
                            sink ^= LEAN.readValue(json[k % n], LeanTrade.class).quantity(); orders++; break;
                        case "symbol":
                            sym.flatMap(json[k % n], discard); orders++; break;
                        case "split":
                            split.flatMap(json[k % n], discard); orders++; break;
                        case "ser": {
                            out.clear();
                            for (PositionUpdate x : updates.get(k % n)) ser.serialize(x, out);
                            sink ^= out.length(); orders++; break;
                        }
                        case "deser": {
                            byte[] w = wire.get(k % n);
                            in.setBuffer(w);
                            for (int u = 0; u < updates.get(k % n).length; u++) sink ^= ser.deserialize(in).signedQuantity;
                            orders++; break;
                        }
                        default: throw new IllegalArgumentException(stage);
                    }
                    k++;
                }
            }
            result[0] = orders;
            result[1] = System.nanoTime() - t0;
        }
        double nsPerOrder = (double) result[1] / result[0];
        System.out.printf("RESULT filler=%d stage=%s bytesPerOrder=%.1f wireBytesPerOrder=%.1f lz4BytesPerOrder=%.1f recordsPerBatch=%.0f nsPerOrder=%.1f ordersPerSec=%.0f%n",
                filler, stage, (double) totalBytes / n, (double) wireBytes / n, (double) compressedBytes / n,
                (double) n / batches.size(), nsPerOrder, 1e9 / nsPerOrder);
    }

    static Collector<PositionUpdate> collectorInto(List<PositionUpdate> list) {
        return new Collector<>() {
            public void collect(PositionUpdate u) { list.add(u); }
            public void close() { }
        };
    }
}
