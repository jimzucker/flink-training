package st55;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * The running position for one key space, joined to the broadcast prices.
 *
 * Every applied input publishes the absolute position (at-least-once sink,
 * idempotent by value). A market value is emitted on a processing-time timer,
 * at most once per interval per key that changed. Repeats are dropped with one
 * number per key: order ids rise within each input partition and every key
 * lives in one input partition, so an id not above the last applied one is a
 * repeat.
 */
public class PositionFn extends KeyedBroadcastProcessFunction<String, Leg, Price, Out> {

    public static final MapStateDescriptor<String, Price> PRICES =
            new MapStateDescriptor<>("prices", Types.STRING, TypeInformation.of(Price.class));

    private final boolean bySymbol;
    private final long intervalMs;
    private final OutputTag<Out> mvTag;

    private transient ValueState<Acc> state;
    private transient Counter distinctKeys;
    private transient Counter quantityApplied;
    private transient Counter repeatsDropped;
    private transient Counter marketValuesEmitted;

    public PositionFn(boolean bySymbol, long intervalMs, OutputTag<Out> mvTag) {
        this.bySymbol = bySymbol;
        this.intervalMs = intervalMs;
        this.mvTag = mvTag;
    }

    @Override
    public void open(Configuration parameters) {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("acc", Acc.class));
        distinctKeys = getRuntimeContext().getMetricGroup().counter("distinctKeys");
        quantityApplied = getRuntimeContext().getMetricGroup().counter("quantityApplied");
        repeatsDropped = getRuntimeContext().getMetricGroup().counter("repeatsDropped");
        marketValuesEmitted = getRuntimeContext().getMetricGroup().counter("marketValuesEmitted");
    }

    @Override
    public void processElement(Leg leg, ReadOnlyContext ctx, Collector<Out> out) throws Exception {
        Acc acc = state.value();
        if (acc == null) {
            acc = new Acc();
            distinctKeys.inc();
        }
        if (leg.id <= acc.lastId) {
            repeatsDropped.inc();
            return;
        }
        acc.pos += leg.qty;
        acc.n++;
        acc.lastId = leg.id;
        quantityApplied.inc(leg.qty);
        out.collect(new Out(leg.key, positionJson(leg.key, acc)));
        if (!acc.timer) {
            ctx.timerService().registerProcessingTimeTimer(
                    ctx.timerService().currentProcessingTime() + intervalMs);
            acc.timer = true;
        }
        state.update(acc);
    }

    @Override
    public void onTimer(long ts, OnTimerContext ctx, Collector<Out> out) throws Exception {
        Acc acc = state.value();
        if (acc == null) return;
        String key = ctx.getCurrentKey();
        String sym = bySymbol ? key : Keys.symOfAcctKey(key);
        Price p = ctx.getBroadcastState(PRICES).get(sym);
        if (p == null) {
            // no price yet for this symbol: try again next interval
            ctx.timerService().registerProcessingTimeTimer(ts + intervalMs);
            return;
        }
        acc.timer = false;
        state.update(acc);
        marketValuesEmitted.inc();
        ctx.output(mvTag, new Out(key, marketValueJson(key, acc, p)));
    }

    @Override
    public void processBroadcastElement(Price p, Context ctx, Collector<Out> out) throws Exception {
        BroadcastState<String, Price> s = ctx.getBroadcastState(PRICES);
        Price cur = s.get(p.sym);
        if (cur == null || p.ts > cur.ts) {
            s.put(p.sym, p);
        }
    }

    private String positionJson(String key, Acc acc) {
        StringBuilder b = new StringBuilder(96);
        keyFields(b, key);
        b.append(",\"pos\":").append(acc.pos).append(",\"n\":").append(acc.n).append('}');
        return b.toString();
    }

    private String marketValueJson(String key, Acc acc, Price p) {
        StringBuilder b = new StringBuilder(128);
        keyFields(b, key);
        b.append(",\"pos\":").append(acc.pos)
         .append(",\"price\":").append(p.cents)
         .append(",\"priceTs\":").append(p.ts)
         .append(",\"mv\":").append(acc.pos * p.cents)
         .append(",\"n\":").append(acc.n).append('}');
        return b.toString();
    }

    private void keyFields(StringBuilder b, String key) {
        if (bySymbol) {
            b.append("{\"sym\":\"").append(key).append('"');
        } else {
            b.append("{\"acct\":\"").append(key, 0, 4)
             .append("\",\"sub\":\"").append(key, 5, 9)
             .append("\",\"sym\":\"").append(key, 10, key.length()).append('"');
        }
    }
}
