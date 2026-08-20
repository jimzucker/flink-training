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

_Pending._
