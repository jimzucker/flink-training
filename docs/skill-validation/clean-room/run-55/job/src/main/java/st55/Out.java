package st55;

/** A record for a Kafka sink: key and value, both text. A Flink POJO. */
public class Out {
    public String key;
    public String value;

    public Out() {}

    public Out(String key, String value) {
        this.key = key;
        this.value = value;
    }
}
