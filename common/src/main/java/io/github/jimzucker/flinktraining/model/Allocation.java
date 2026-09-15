package io.github.jimzucker.flinktraining.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Map;

/**
 * One account's slice of a block trade.
 *
 * <p>{@code filler} holds optional padding fields, name to value, used only to
 * study how payload size affects throughput. It is left out of the JSON when
 * empty, so an allocation without filler serializes to exactly the bytes it
 * always has. Callers pass an immutable map; the generator shares one instance.
 */
public record Allocation(
        String account,
        String subAccount,
        long quantity,
        @JsonInclude(JsonInclude.Include.NON_EMPTY) Map<String, String> filler) {

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
        filler = filler == null ? Map.of() : filler;
    }

    /** An allocation with no filler: the shape the demo has always used. */
    public Allocation(String account, String subAccount, long quantity) {
        this(account, subAccount, quantity, Map.of());
    }
}
