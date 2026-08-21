# Step 02 — Generators: review

## Round 1

### Asked

1. Price tick rate defaults to 1/sec. Right demo default?
2. Every block splits across all 4 accounts, always. Should some allocate to fewer?
3. Sub-account fixed at `SUB1` — confirm one per account?
4. Seed 42 with a fixed start epoch means historical event times. Fine before Grafana?

### Feedback

> 1 - eo 1000/second/3 yes/4 make 10 sub accounts per acount / 4 - use wall clock
> we need to track latency from creation time to output sinks

### Actions

| Feedback | Action |
|---|---|
| Prices at 1000/second | `PriceGenerator` now emits one price per call, round-robin across symbols, so the configured rate is a plain count of prices per second. Default `PRICES_PER_SECOND=1000`. Verified even: 2501 per symbol out of 10004, spread 0 |
| 10 sub-accounts per account | `ReferenceData.SUB_ACCOUNTS` holds `SUB01`–`SUB10`; each block allocates to all four accounts, one sub-account each. **Account keys go from 16 to 160** |
| Use wall clock, to track latency creation → sink | Event time now comes from a `LongSupplier`. `live()` uses the wall clock and is the default; `replaying()` uses a counter and is selected with `START_EPOCH_MILLIS`. Confirmed current: an event time read back off the topic was 2.5 minutes old, not three years |

### Consequences worth recording

- **The assignment prints 16 unique account keys.** With 10 sub-accounts it is
  160. The 40/sec rate for sink 4 is unchanged, since that follows from four
  allocations per trade. Raised with the reviewer.
- **Reproducibility is now split in two.** Wall-clock event times cannot be
  byte-identical across runs, so the seed guarantees the *content* of the
  sequence — symbols, sides, quantities, sub-accounts — and replay mode exists
  for when byte-identical output has to be demonstrated. Asserted directly
  rather than assumed.
- **Key coverage is a function of run length.** 160 keys reached at 45s;
  147 of 160 at 10s, which is what coupon-collector predicts for 400 draws.
  A short demo shows the key count climbing rather than settled.
- **The pinned sequence hash failed, on purpose.** Drawing a sub-account
  consumes extra randomness, so the sequence changed. Re-pinned, as the test's
  own comment instructs.
- **Two verification-script bugs found and fixed**, both false alarms rather
  than defects: reading a fixed-size prefix slices unevenly across partitions
  and made an exactly-even round-robin look uneven; and a run ends mid-cycle, so
  record count is nominal ±1 while the invariants stay exact.

### Outcome

Superseded in round 2 — the sub-account change was a typo.

## Round 2

### Asked

Confirm the deck should present 160 account keys, deviating from the 16 the
assignment prints.

### Feedback

> the 160 was a typo revert it

### Actions

| Feedback | Action |
|---|---|
| Revert the 160 | Back to one sub-account per account. `ReferenceData.SUB_ACCOUNT` is `SUB1` again and `ACCOUNT_KEY_COUNT` is 4 x 4 = **16**, matching the assignment. Design doc, README and verification script all reverted |

Removing the sub-account draw restored the original random sequence exactly: the
pinned trades hash went back to `241903ac...`, the value it held before the
change. The round-robin price generator, the 1000/sec default and wall-clock
event times all stand — only the sub-account expansion was reverted.

### What the round turned up

Proving reproducibility through the broker exposed a real gap. Two replay runs
bounded by *duration* still differed, because a six-second run emits 60 or 61
trades depending on where the clock falls. Every record they shared was
byte-identical, but the run boundary was not.

Runs are now bounded by **record count** (`MAX_TRADES`, `MAX_PRICES`), which
makes the boundary exact, and a record-bounded run finishes as soon as its
limits are met instead of waiting out a duration. Two such runs now produce
byte-identical topics, and the orders hash equals the value `DeterminismTest`
pins in-process — so the sequence the unit test guards and the bytes that reach
the broker are provably the same.

### Outcome

Approved. Squash-merged to `main`, tagged `step-02`.
