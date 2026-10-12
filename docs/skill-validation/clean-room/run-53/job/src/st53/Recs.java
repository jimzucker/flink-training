package st53;

/** Records that cross the network inside the job. Public fields and no-arg constructors: Flink's POJO serializer. */
public final class Recs {
    private Recs() {}

    /** One order, for the symbol aggregation. */
    public static class SymUpdate {
        public long id;
        public String symbol;
        public long qty;
        public SymUpdate() {}
        public SymUpdate(long id, String symbol, long qty) { this.id = id; this.symbol = symbol; this.qty = qty; }
    }

    /** One allocation, for the account / sub-account / symbol aggregation. key = "A1|S1|SYM0001". */
    public static class AcctUpdate {
        public long id;
        public String key;
        public String symbol;
        public long qty;
        public AcctUpdate() {}
        public AcctUpdate(long id, String key, String symbol, long qty) {
            this.id = id; this.key = key; this.symbol = symbol; this.qty = qty;
        }
    }

    public static class Price {
        public String symbol;
        public long ts;
        public long cents;
        public Price() {}
        public Price(String symbol, long ts, long cents) { this.symbol = symbol; this.ts = ts; this.cents = cents; }
    }

    /** A Kafka record on its way out: key and JSON value. */
    public static class KV {
        public String key;
        public String value;
        public KV() {}
        public KV(String key, String value) { this.key = key; this.value = value; }
    }
}
