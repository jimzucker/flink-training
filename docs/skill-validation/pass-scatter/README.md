# Pass-to-pass scatter: raw results

The six experiments behind [findings §7](../findings.md#7-why-the-same-case-reads-differently-from-pass-to-pass--2026-092829),
run 2026-09-28/29 on the reference pipeline's stack, 2-core case, one build.

| folder | what it holds |
|---|---|
| experiment-1 | `suite.json` from the harness (11 passes), and every Mac process sampled every 5 s (`host-processes.log`, `hostsample.sh`) |
| experiment-2 | the same with background-priority load on alternate passes: `suite.json`, the load script `burn.py`, when it switched (`switch0.log`) and the process samples |
| experiment-3 … 6 | the script that ran it (`exp3.py` … `exp6.py`), its log, and the per-window rates (`results.json`) |

Experiments 3–6 compute 60 s windows from the harness's own offset sampler,
not aligned to commit boundaries; see the caveat in findings §7. The scripts
import the harness from `<scalable-flink-skill>/harness`.
