package io.github.jimzucker.flinktraining.model;

/**
 * The composite key the account-side of the pipeline is aggregated on:
 * {@code account/subAccount/symbol}.
 *
 * <p>Built in one place so the diagram, the topics and the jobs cannot drift
 * apart on separator or field order.
 */
public final class AccountKey {

    public static final String SEPARATOR = "/";

    private AccountKey() {
    }

    public static String of(String account, String subAccount, String symbol) {
        return account + SEPARATOR + subAccount + SEPARATOR + symbol;
    }

    public static String of(Allocation allocation, String symbol) {
        return of(allocation.account(), allocation.subAccount(), symbol);
    }

    /** The symbol part, for the price join on the account side of Part 2. */
    public static String symbolOf(String accountKey) {
        int last = accountKey.lastIndexOf(SEPARATOR);
        if (last < 0) {
            throw new IllegalArgumentException("not an account key: " + accountKey);
        }
        return accountKey.substring(last + SEPARATOR.length());
    }
}
