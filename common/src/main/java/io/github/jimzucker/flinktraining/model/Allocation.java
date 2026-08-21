package io.github.jimzucker.flinktraining.model;

/** One account's slice of a block trade. */
public record Allocation(String account, String subAccount, long quantity) {

    public Allocation {
        if (account == null || account.isBlank()) {
            throw new IllegalArgumentException("account is required");
        }
        if (subAccount == null || subAccount.isBlank()) {
            throw new IllegalArgumentException("subAccount is required");
        }
        if (quantity <= 0) {
            throw new IllegalArgumentException("allocation quantity must be positive, got " + quantity);
        }
    }
}
