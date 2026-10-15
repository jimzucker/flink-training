package st55;

/** Per-key state: running position, inputs applied, highest order id applied
 *  (repeat dropping with one number per key), and whether a market-value timer
 *  is pending. A Flink POJO. */
public class Acc {
    public long pos;
    public long n;
    public long lastId = -1;
    public boolean timer;

    public Acc() {}
}
