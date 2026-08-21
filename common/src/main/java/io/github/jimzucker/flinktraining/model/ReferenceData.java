package io.github.jimzucker.flinktraining.model;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * The fixed universe the demo runs against: 4 symbols, 4 accounts, one
 * sub-account each.
 *
 * <p>These counts are why the expected output is arithmetic rather than a
 * statistic: 4 symbol keys, and 4 accounts x 4 symbols = 16 account keys.
 */
public final class ReferenceData {

    public static final List<String> SYMBOLS = List.of("AAPL", "MSFT", "GOOG", "AMZN");
    public static final List<String> ACCOUNTS = List.of("ACC1", "ACC2", "ACC3", "ACC4");

    /** One sub-account per account, which is what makes the account key count 4 x 4 and not more. */
    public static final String SUB_ACCOUNT = "SUB1";

    /** Every block trade is split across all four accounts. */
    public static final int ALLOCATIONS_PER_TRADE = ACCOUNTS.size();

    /** Opening price per symbol. Round numbers so market value is easy to check by eye. */
    public static final Map<String, BigDecimal> OPENING_PRICES = Map.of(
            "AAPL", new BigDecimal("100.00"),
            "MSFT", new BigDecimal("200.00"),
            "GOOG", new BigDecimal("300.00"),
            "AMZN", new BigDecimal("400.00"));

    public static final int SYMBOL_KEY_COUNT = SYMBOLS.size();

    /** 4 accounts x 4 symbols. */
    public static final int ACCOUNT_KEY_COUNT = ACCOUNTS.size() * SYMBOLS.size();

    private ReferenceData() {
    }
}
