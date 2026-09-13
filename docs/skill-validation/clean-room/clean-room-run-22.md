# Clean-room validation, run 22 — no table, and the guard that caused it

This run produced **no step ratios at all**. Both `prove.py all --quick`
chains stopped at the tiny proof, and the cause was a guard I had merged two
hours earlier without replaying it against the record.

## What happened

| attempt | cases | outcome |
|---|---|---|
| A | 1, 2, 4 | `FAIL at tinyproof 14.2 min` — 1-core case refused, 2,011 broker limit hits |
| B | 2, 4 | `FAIL at tinyproof 13.4 min` — 2-core case refused, 5,453 hits |

The 4-core case passed every guard in both attempts. The two guards had **no
configuration in common on this host**: the broker refusal asked for
`kafkaMemory` = 5,632m, while the VM budget rule from #68 allowed at most
3,613m on a 7,838 MiB VM. The agent tried both permitted broker sizes, declined
to fork the harness or resize Docker Desktop, and said so.

What did pass: preflight 17/17 twice, and completeness twice with no
tolerances — 6M trades at 1 core and 8M at 2, each drained clean and again with
the worker killed mid-drain, all 512 symbol and 1,024 account positions exact,
fan-out exactly 5, and the killed drains replaying 66,248 and 104,259 records
to exactly correct final positions.

Wall clock 1 h 17 m: 27.6 min in the two chains, 19 min calibration, 4 min
redoing two of its own generator bugs, the rest building.

## The cause, and what it cost

[#68](https://github.com/jimzucker/flink-training/pull/68) added a rule
refusing a configuration whose worker, broker and job manager did not leave the
VM a spare gigabyte. It was never replayed against the record. Runs 20 and 21
had both **passed** with a 6,144m broker on that same VM, so the rule refused
configurations already known to work, and it contradicted the broker guard's
own remediation figure.

[#69](https://github.com/jimzucker/flink-training/pull/69) removed the
enforcement the same day; preflight now reports the total and flags
over-commitment as normal, because the broker's page cache is elastic.

[#70](https://github.com/jimzucker/flink-training/pull/70) closed the hole that
allowed it: `record/configs.json` holds configurations whose verdict is known,
and `replay` now builds each one and fails if a guard disagrees. Re-inserting
the #68 rule makes replay report three disagreements in about a second — the
check that would have cost nothing and saved this run.

## What the run is worth

Nothing as a measurement, and quite a lot as a test of the discipline: the
agent hit a contradiction in the harness, refused to work around it by editing
the harness or the host, reported exactly which two numbers were irreconcilable,
and left the stack clean. That is the behaviour the skill asks for. The failure
was upstream of it.
