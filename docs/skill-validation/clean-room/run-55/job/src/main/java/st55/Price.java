package st55;

/** A price for a symbol at a time, in integer cents. A Flink POJO. */
public class Price {
    public String sym;
    public long ts;
    public long cents;

    public Price() {}

    public Price(String sym, long ts, long cents) {
        this.sym = sym;
        this.ts = ts;
        this.cents = cents;
    }
}
