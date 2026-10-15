# Journal

## Step 1 — interview, plan, build (2026-10-14 19:26–)

- Read SKILL.md and harness/README.md in full; skill level with origin/main at e0e13f7.
- Took every interview default (ANSWERS.md); wrote ASSUMPTIONS.md and PLAN.md before code.
- Built one Maven module: job (st55.PositionsJob), generator (st55.Gen), verifier (st55.Verify).
- Generator determinism checked by hand: two manifests at 200,000 records, seed 55, byte-identical.
- `prove.py up` 18 s; `preflight` 28/28 PASS (2 min 49 s). Keys per subtask at 4 cores: 1035/1004/1032/1025 (symbol), 4082/4059/4107/4136 (account), at Flink's default 128 key groups.
- First `completeness` (build 0d459ad5f351f64b): clean arm passed every assertion; drained 10,000,000 orders in 128.9 s at 1 core.
  Killed arm (kill at 4,172,543 committed, 42%) stopped on ONE account key: after the restart its position repeated the
  last published record (same n) instead of stepping back. That is an at-least-once repeat the arm allows; my verifier
  counted it as a gap. Verifier fixed (a repeat of the last record counts as the key's one step back; a gap is only n
  jumping forwards). Proved on the recorded killed-arm outputs: killed arm now passes (14,336 keys stepped back once,
  none twice); the same rows checked as a clean arm still stop on 4 assertions.
- Renamed the market-value sinks to `sink-market-values-by-*`: the graph ties an output to the Writer whose name holds
  the topic's name, and `sink-mv-by-*` did not, so the two market-value topics were drawn unconnected.

## Step 2 — the chain (`prove.py all`, build 55e693b915fbe2a6), 19:52–21:32

- up 2 s, preflight 158 s, completeness 1,099 s (includes about 5 min of the Mac asleep, 20:01–20:06, on battery; held awake with caffeinate from 20:07), tiny proof 598 s, fill 214 s (300,000,000 orders), suite 3,929 s, report 2 s. 100.0 min.
- Completeness PASS on both arms (kill landed at 4,156,959 of 10,000,000 committed).
- Tiny proof PASS: 4 cores a ceiling (93.3% of cap, broker memory-limit hits), 1->2 1.778x inside its 1.5-2.5 bound.
- Suite: 1->2 1.830x, judged on 1.763x-1.847x: not met, 2.1% under, "more passes could change that" after all 6 extra cases.
  2->4 1.863x, judged on 1.784x-1.980x: met within the 1% margin. One 4-core pass (p2-desc) kept as a ceiling at 94.5% of cap.
- Next, per SKILL.md §6: probe --repeats 9, then ceiling. Change nothing in the pipeline.

## Step 3 — probe and ceiling, 21:33–21:51

- `probe --repeats 9`: simple arithmetic 1->2 1.94x, 2->4 1.97x; memory-heavy 1->2 1.80x [1.68-1.94], 2->4 1.51x [1.37-1.57]; memory-heavy middle half 4%, wider than the 2.1% shortfall.
- `ceiling` at 4 cores: broker 2.5 / 1.0 / 0.5 cores -> 447,262 / 437,915 / 394,602 orders/s. Broker not the limit at its real cap.
- Per SKILL.md §6, 1->2 is reported as not met, 2.1% under, and nothing in the pipeline is changed. Stack left up.
