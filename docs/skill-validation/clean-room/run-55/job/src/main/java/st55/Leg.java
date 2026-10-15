package st55;

/** One unit of work for a keyed aggregation: the order itself (key = symbol)
 *  or one of its allocations (key = account/sub-account/symbol). A Flink POJO. */
public class Leg {
    public long id;      // order id; rises within each input partition
    public String key;   // the aggregation key
    public long qty;     // signed quantity

    public Leg() {}

    public Leg(long id, String key, long qty) {
        this.id = id;
        this.key = key;
        this.qty = qty;
    }
}
