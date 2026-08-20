package io.github.jimzucker.flinktraining.model;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * The fixed universe the demo runs against: 4 symbols, 4 accounts, ten
 * sub-accounts each.
 *
 * <p>These counts are why the expected output is arithmetic rather than a
 * statistic: 4 symbol keys, and 4 accounts x 4 symbols = 16 account keys.
 */
public final class ReferenceData {

    public static final List<String> SYMBOLS = List.of("AAPL", "MSFT", "GOOG", "AMZN");
    public static final List<String> ACCOUNTS = List.of("ACC1", "ACC2", "ACC3", "ACC4");

    /** Ten sub-accounts under every account. */
    public static final List<String> SUB_ACCOUNTS = List.of(
            "SUB01", "SUB02", "SUB03", "SUB04", "SUB05",
            "SUB06", "SUB07", "SUB08", "SUB09", "SUB10");

    /** Every block trade is split across all four accounts. */
    public static final int ALLOCATIONS_PER_TRADE = ACCOUNTS.size();

    /** Opening price per symbol. Round numbers so market value is easy to check by eye. */
    public static final Map<String, BigDecimal> OPENING_PRICES = Map.of(
            "AAPL", new BigDecimal("100.00"),
            "MSFT", new BigDecimal("200.00"),
            "GOOG", new BigDecimal("300.00"),
            "AMZN", new BigDecimal("400.00"));

    public static final int SYMBOL_KEY_COUNT = SYMBOLS.size();

    /** 4 accounts x 10 sub-accounts x 4 symbols. */
    public static final int ACCOUNT_KEY_COUNT =
            ACCOUNTS.size() * SUB_ACCOUNTS.size() * SYMBOLS.size();

    private ReferenceData() {
    }
}
