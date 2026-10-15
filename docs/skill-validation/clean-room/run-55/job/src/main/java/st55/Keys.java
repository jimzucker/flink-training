package st55;

/** The key spaces of the default business case, shared by the job, the
 *  generator and the verifier. */
public final class Keys {
    private Keys() {}

    public static final int SYMBOLS = 4096;
    public static final String[] ACCTS = {"ACC1", "ACC2"};
    public static final String[] SUBS = {"SUB1", "SUB2"};
    /** account/sub-account pairs, in allocation order */
    public static final String[][] PAIRS = {
        {"ACC1", "SUB1"}, {"ACC1", "SUB2"}, {"ACC2", "SUB1"}, {"ACC2", "SUB2"}};

    public static String sym(int i) {
        return String.format("S%04d", i);
    }

    /** fixed width: ACC1/SUB1/S0042 */
    public static String acctKey(String acct, String sub, String sym) {
        return acct + "/" + sub + "/" + sym;
    }

    public static String symOfAcctKey(String k) {
        return k.substring(10);
    }
}
