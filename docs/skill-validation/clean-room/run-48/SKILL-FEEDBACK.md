# Skill feedback — clean-room run 48

Every place the skill or the harness was wrong, unclear, out of date, or cost
time. Each quotes the exact wording and says what it should say instead.
Skill as installed: git commit fe5cdf1 plus uncommitted edits in the working
tree (SKILL.md sha256 388ea51b…).

Ordered by what it cost, most first. Findings added during the chain are at the
end, under the step where they appeared.

---

## Before the chain

### F1. The completeness drain cancels the job 12 seconds after the last commit — a throttled output has to beat that, and nothing says so

`harness/prove.py`, `cmd_completeness`, once the input is fully committed:

```
if cm >= c.small:
    time.sleep(c.ckpt_s + 2)
```

then the `finally` cancels the job, and only then does the verifier run. The
default business case has two outputs that are emitted **every 10 seconds**, and
section 4 asks the verifier to prove that each final market value equals final
position × latest price. That final market value is emitted by a timer up to
10 s after the last position change. With the default 10 s checkpoint interval
the window between "last offset committed" and "job cancelled" is 12 s, so the
throttled output has about 2 s of margin. It passed here (both arms, every key),
but only because 10 < 12: a user who answers question 2 with "every 30
seconds" — which the question invites, "a configurable interval" — gets a
completeness failure that looks like a wrong market value.

Nowhere in SKILL.md §4 or `harness/README.md` is this wait mentioned. The
`topicsAlsoWritten` row says the drain "stops the run on any that stayed empty",
which is about *any* rows arriving, not the *last* one.

**Should say** (README, `topicsAlsoWritten` row, and §4): *"After the last
input offset commits, the harness waits one checkpoint interval plus 2 seconds
and then cancels the job before the verifier runs. A timer-driven output must
emit its final value inside that time, so keep its interval below
`checkpointMs`, or the verifier will see a stale last value."* Better: have the
harness wait `max(checkpointMs, a declared throttle interval) + 2 s`, from a
`throttleMs` field.

### F2. The key-layout row passes a 4% ceiling in silence

Preflight printed:

```
PASS  keys divide evenly across subtasks   maxParallelism 128 (Flink's own default for these parallelisms); keys per subtask at 4 cores: position-by-symbol 1065/1008/979/1044; position-by-account 4169/4071/4044/4100
```

`results/preflight.json` has the ceiling it computed: `"stageCeiling":
0.9615` for the symbol stage at 4 cores and `0.9879` at 2. If that stage
sets the pace, the 2→4 step cannot exceed 2 × 0.9615 / 0.9879 = **1.95×
against a 1.90× target** — the key layout alone uses up more than half of the
margin (arithmetic from the harness's own figures, not a measurement), and the
row does not say so. `prove.py` only adds the sentence and a suggested `pipeline.max-parallelism`
when `stageCeiling < floor` (0.95).

SKILL.md §1 q3 frames this as a small-key-set problem — "a small one rarely
does", "16 keys land 5/3/4/4". The new default is 4,096 symbols, and it still
lands 1065/979 at four subtasks. The row's message makes the reader think a
large key set is safe.

**Should say:** print the ceiling on every PASS row — *"…1065/1008/979/1044:
the busiest subtask holds 26.0% against 25.0%, so this stage cannot return more
than 0.96 of linear at 4 cores"* — and offer the `pipeline.max-parallelism`
that evens it out whenever the ceiling is within the two-pass noise (±3–4%) of
the floor, not only below it. And because `flinkProperties` is baked in at
`up` (as the README rightly warns), say at §1a to run preflight **before** the
chain, as this run did, so the value can be added before anything is measured.

### F3. The README tells you to wait in a way its next section says will get your shell killed

`harness/README.md`, the quick-start block:

> Wait with `until [ -f results/DONE ]; do sleep 30; done` and nothing more.

Twenty lines later, same file:

> **During `tinyproof` and `down` the reaper kills any host process whose
> command line names the project directory** … a tool that runs `bash -c "cd
> /path/to/project && until ...; do sleep 30; done"` puts the path on the
> command line *because* it had to `cd` …

For an agent, the first instruction is the one that gets killed; SKILL.md §5
says to use `watch.sh` through a scratch script instead. The quick-start line
is the one a reader copies.

**Should say:** *"Wait with `harness/watch.sh`, started from a two-line script
outside the project (see 'Watching a run from an agent' below). A loop run
from inside the project directory can be killed by the teardown."* Delete the
`until` line.

### F4. Prometheus rewrites operator names, so dashboard queries built from the job's names match nothing

SKILL.md §7 asks for seven panels and says to check each has data on first
render. It does not say that Flink's Prometheus reporter replaces every
character outside `[A-Za-z0-9_:]` with `_`. The job names its operators
`kafka-source`, `parse-orders`, `position-by-symbol + market-value-by-symbol`
(names the design diff needs verbatim); in Prometheus they are
`Source:_kafka_source`, `parse_orders`,
`position_by_symbol___market_value_by_symbol`. Every panel filtered on an
operator name returned nothing, silently, until I queried the store by hand.

**Should say** (§7, beside "Five of the seven panels need engine metrics"):
*"The Prometheus reporter turns every character that is not a letter, digit,
`_` or `:` into `_`, and prefixes sources with `Source:_`. Filter on the
rewritten name — `parse-orders` becomes `parse_orders`."*

### F5. `extraServices` needs its own quoting, and cannot declare a volume

The README's `extraServices` example has no `ports`, no `user`, and no
storage. `yaml_block` writes scalars bare, so a port mapping has to be written
as `"\"13000:3000\""` in JSON to come out quoted in YAML, and `user` likewise.
Nothing says so. The top-level `volumes:` block is fixed to `kafkadata` and
`ckpt`, so Prometheus's and Grafana's data cannot be named volumes; left alone,
both images create **anonymous** volumes, which the harness tracks as dangling
(`volumes-before.json`). I bind-mounted host directories instead
(`dashboard/prometheus-data`, `dashboard/grafana-data`).

**Should say** (README, below the exporter example): a Prometheus + Grafana
`extraServices` example with `ports` quoted as `"\"19090:9090\""`, and one line:
*"Services that keep data need a bind mount to a directory in the project;
`extraServices` cannot add named volumes, and an image's anonymous volume is
left behind."*

### F6. The generator's progress is thrown away

`lib.fill()` runs the generator through `sh()`, which captures its output, and
logs only the last line of stdout when it ends. A 250,000,000-record fill is
about ten minutes of nothing in `harness.log` or `PROGRESS.txt`. The fill's
stderr (where a generator would print progress) is never shown, even on
failure through this path. I watched broker offsets from outside to know it was
moving.

**Should say / do:** stream the generator's stderr to `results/fill.log` and
say so in the `generator.cmd` row: *"Anything the generator prints on stderr
goes to results/fill.log."*

### F7. Minor: "Offer the default with each" says nothing about defaults that are examples

Question 2's default says *"If the order has 4 allocations it emits 5
records."* That is an example, not a rule, and the harness needs a
**constant** fan-out. Taking it literally, I had to decide that every order has
exactly four allocations (one per account / sub-account pair) for
`outputsPerInput` to be a constant. It is the natural reading, but it is a
decision that changes a number the harness divides by.

**Should say:** *"Default: every order carries one allocation per account /
sub-account — four at the default keys — so one order produces 1 + 4 = 5
position records."*

---

## Chain 1 (06:00–06:33): stopped at completeness after 32.4 minutes

### F8. A generator that stops writing is waited on for two hours, in silence

`lib.fill()` runs the generator as `r = sh(cmd, timeout=7200)` and checks
nothing until it returns. My generator (my bug — see `FIXES.md` B1) lost its
producer threads 7.5 s into a 20,000,000-record fill and then sat in
`flush()` forever. For **29 minutes** `PROGRESS.txt` said

```
06:03:20  [######..............]  29%  step 3 of 7: proving nothing is lost, including after killing the pipeline mid-run
```

and `harness.log` said only `fill: …`. Nothing distinguishes a slow fill from
a dead one. I found it by reading the broker's offsets by hand (12,952,490 of
20,000,000, not moving) and a thread dump. Left alone, the chain would have
waited until 08:03. SKILL.md §3 already records run 45's generator swapping to
a halt on this same laptop — that is two runs.

**Should do:** while the generator runs, read the topic's log end every 30 s
and stop the run when it has not moved for two minutes: *"STOPPED at fill: the
generator has written nothing for 120 s (12,952,490 of 20,000,000 records on
orders-small). Its output is in results/fill.log."* And put the fill's record
count into `PROGRESS.txt`, the way the suite puts its case count there.

### F9. The chain-start sweep kills the shell that launched the chain

The README's launch line is `nohup python3 $H all > results/all.log 2>&1 &`,
which for an agent means a shell that `cd`s into the project first. The
chain-start sweep kills any host process "naming the project directory on its
command line", so the launching shell died with exit 144 a few seconds after
the launch (the chain itself survived). Harmless here, because the launch was
the last command in that shell; any command after it on the same line is lost,
and a reader sees a failed tool call where the launch actually worked.

**Should say** (README, beside the launch line): *"Launch it from a script
outside the project, as with `watch.sh`. The chain's first act is to kill
host processes that name the project directory, and that includes the shell
you launched it from."*

---

## Chain 2 (from 06:35)

### F10. The disk projection counts the tiny proof's and the completeness topic's bytes as the suite's backlog — a harness bug

The tiny proof printed:

```
disk: input 74 B/record x 250,000,000 = 18.5 GB (12.5 GB of it already on the broker, 5.9 GB still to write); … 113.7 GB free now + 17.1 GB the tiny proof gives back = 130.8 GB: FITS
```

The suite's topic `orders` **did not exist yet** — the fill had not run.
`tinyproof.json` has `"suiteInputBytesOnDisk": 12543787008` against
`"tinyTopicBytes": 11077226496`. The cause is in `lib.topic_bytes()`:

```
pat = " ".join(f"/var/lib/kafka/data/{t}-*" for t in topics)
```

For the topic `orders` that glob is `/var/lib/kafka/data/orders-*`, which also
matches the partition directories of `orders-tiny-0…7` and `orders-small-0…7` —
topics the harness itself names by appending `-tiny` and `-small` to
`topics.in`. So **every run** has this overlap, not just mine. The 11.1 GB tiny
topic is then credited twice: once as "the tiny proof gives back" and once as
"already on the broker". Here the projection said 63.6 GB needed; by my
arithmetic from the same line (18.5 GB of input + 37.7 GB of sinks + 20 GB
floor, checkpoints ~0) it should have been about 76 GB. It fitted either way
on this disk. On a tighter disk it would pass a suite that does not fit — the
exact failure this check exists to prevent.

**Should do:** match a partition directory exactly — `{t}-[0-9]*` and then
check the suffix after the last `-` is all digits, or list the partitions and
`du` each `{t}-{p}` — and add a self-test with a topic whose name is a prefix
of another.

### F11. PROGRESS.txt says the tiny proof has two cases; it ran three

```
06:50:43  [#########...........]  43%  step 4 of 7: the tiny proof: two cases end to end, and every guard broken on purpose
```

It ran 1, 2 and 4 cores, as SKILL.md §3 says it must ("run **every case the
suite will run**"). **Should say:** *"step 4 of 7: the tiny proof: every case
(1, 2 and 4 cores) end to end, then every guard broken on purpose"*.

### F12. During the suite, PROGRESS.txt stops saying which step it is on, and its bar starts again from 0%

```
07:02:35  [###########.........]  57%  step 5 of 7: filling the backlog — the long quiet one
07:05:51  [....................]   0%  suite: case 1 of 10 (1 cores, pass p1-asc)
```

SKILL.md §2 and §1a promise *"which step of seven, which case of ten"*. The
suite line drops "step 6 of 7", and the bar goes from 57% back to 0%, which
reads as the run starting over. It also says **"1 cores"**, which SKILL.md §6
names as a rule ("get the plurals right — *1 core*, not *1 cores*. Both are
self-tested") — the self-test evidently does not cover `PROGRESS.txt`.
**Should say:** *"step 6 of 7: suite case 1 of 10 (1 core, pass p1-asc)"*,
with the bar on the chain's scale.

### F13. The sentinel was thrown out for Kafka's memory with fewer limit hits than three baseline passes that were kept — and the scorecard then blames Kafka for the whole 1-core case

From `results/suite.json`, the four 1-core passes:

| pass | orders/s | pipeline CPU used | Kafka memory-limit hits in the window | verdict |
|---|---:|---:|---:|---|
| p1-asc | 129,516 | 99.8% | 2,747 | kept |
| p2-desc | 137,046 | 99.1% | 2,175 | kept |
| p3-asc | 137,723 | 100.2% | 1,897 | kept |
| sentinel | 130,915 | **97.9%** | 1,494 | **thrown out as a ceiling** |

The sentinel had the *fewest* hits, a rate inside the range of the three that
were kept, 0.1% source idle, and was above the 95% floor every other guard
uses. It was thrown out only because the broker-hit rule exempts a case at
**99%** of cap and this one read 97.9%. `lib.py`'s own comment on
`brokerHitsCapExempt` already says this rule is *"UNSETTLED"*, that *"This one
rule asks for 0.99, and nothing says why"*, and that run 42 lost its two
fastest passes in the same four-point gap. This is a second run.

It then reaches the scorecard as a verdict on the whole case:

```
      1    134,762/s   …   5g, full 1,494x   Kafka memory    raise kafkaMemory
  1 core: raise kafkaMemory from 5g to about 8g.
  1 core: Kafka's memory, blocking higher throughput. Kafka hit its limit 1,494 times and had to
      read the test data back off disk, so the pipeline was waiting on Kafka rather than using
      its CPU.
```

Three things are wrong in those lines. (1) The 1-core row's speed, 134,762/s,
is the mean of the three passes that were kept — its verdict is taken from the
one that was not. (2) *"the pipeline was waiting on Kafka rather than using its
CPU"* is a mechanism nobody measured: the pipeline used 97.9% of its core and
its source was idle 0.1% of the time. (3) *"raise kafkaMemory … to about
8g"* is advice the tiny proof, 45 minutes earlier in the same chain, had
already said this machine cannot follow: *"8192m is the size to try -- but
this machine cannot give it that … over by 2,927"*. The scorecard drops that
half.

**Should do:** replay the 97.9% sentinel against the record (the README's own
rule for a threshold that has cost a run); take a case's verdict from the
passes its mean is made of; say *"hit its memory limit 1,494 times; the rate
was inside the range of the passes that did not, so the effect was not
measurable"* rather than asserting a cause; and carry the "this machine cannot
give it that" clause wherever the 8g advice is printed.

### F14. "The machine was busy with something else" — it was busy with this run

```
  the 4-core p1-asc: the machine was busy with something else: load reached 8.2 on 8 cores while
      measuring. … Close what else is running and measure again.
```

The recorded load averages grow with the case under test: 2.1–6.2 at 1 core,
3.0–5.7 at 2, 7.0–10.2 at 4. The four-core worker, Kafka, the job manager and
the dashboard all run inside the Docker VM, whose threads count in the host's
load average. Preflight's own row says so — *"load average 2.90 / 2.60 /
2.45, which also counts this run's own containers"* — and then excludes the VM
when it decides whether the machine is quiet. The per-case note does not, and
tells the reader to close programs that are not there. (Also: *"the 4-core
p1-asc"* — the sentence reads as if a word is missing.)

**Should do:** judge the per-case load the way preflight does — subtract the
run's own cores (the case's cap + Kafka's cap + the job manager's) before
calling it "something else" — or say *"load reached 8.2 on 8 cores; this
run's own containers are capped at 7.7 of them (worker 4, Kafka 2.5, job
manager 0.5, dashboard 0.7)"*.

---

## Chain 3 (08:06–09:16) and tuning

### F13, again — a third instance, and this time it cost the tiny proof its 1→2 bound

Chain 3's tiny proof threw out its 1-core case for Kafka's memory at **98.7%**
of cap (1,180 hits), and then printed:

```
[08:33:37]   1->2: not bounded — 1 cores was a ceiling
[08:33:37]   1->4: not bounded — 1 cores was a ceiling
```

SKILL.md §3 insists the tiny proof bound "every step the suite will report …
including the middle one". Here the 99% exemption, not the pipeline, removed
that bound. In the suite that followed, the 1-core `p3-asc` pass was thrown
out at 97.8% of cap with 1,122 hits, while `p1-asc` (2,470 hits, 99.8%) and the
sentinel (1,791 hits, 99.8%) were kept at the same rate within 0.6%. The evidence
across this run's two suites, read from `suite.json`:

- every 1-core pass hit Kafka's limit (1,122–2,747 times), so there is no
  hit-free 1-core pass to compare with; the two thrown out for it read 130,915
  and 129,686, inside the range of the kept ones (129,516–137,723);
- at 2 cores, the one pass per suite with hits read 249,317 against hit-free
  245,570 and 238,424 (chain 2), and 241,251 against 234,637 and 246,165
  (chain 3) — the fastest of three once, the middle once.

In neither place did hits come with a lower rate. That is the kind of evidence
the `brokerHitsCapExempt` comment says the next run should bring.

### F15. The key-layout search looks for a perfect split, which a realistic key set never has — so it offers nothing where something better exists

The harness asks `KeyCheck suggest` for a `maxParallelism` that "divide[s]
every set evenly at every parallelism". For 4,096 and 16,384 hashed keys no
such value exists; preflight would have said *"no maxParallelism between 128
and 32,768 divides these keys; the keys themselves are the fix"* had the
ceiling been below the floor. But the keys are not the fix: scanning 128–32,768
with Flink's own `KeyGroupRangeAssignment` (a 30-line program, and it
reproduces preflight's 1065/1008/979/1044 at 128 exactly) finds **440**, where
every stage is within 0.7% of even (worst ceiling 0.993 against 0.962 at 128),
and 21,036 at 0.996.

SKILL.md §6a's row says *"Add the `pipeline.max-parallelism` it names"*. With
the default's new 4,096 symbols, it names nothing.

**Should do:** have `suggest` return the values with the **highest worst-stage
ceiling** when no exact split exists, and print the best one with its ceiling:
*"440 would put every stage within 0.7% of even (0.993 of linear, against
0.962 now)"*. (For the record: in this run it made no measurable difference —
see `FIXES.md` T1 — so it is a finding about the tool, not a claim that it
helps.)

### F16. A default-built pipeline arrives at the tuning loop with almost nothing left to try

§6a's two largest levers — *read the input once* (+22–24%) and *compress the
sink writes* — are both things a first build following the skill already does,
because §1's own diagram says "parse once" and the sink volume makes
compression the obvious default. *Fewer subtasks* has no hook, *memory per
subtask* is enforced from the start, the broker's page cache cannot grow on a
9.9 GB VM (the tiny proof says so itself), and the pipeline is already CPU
bound. That left one row, key layout, which (F15) the harness offers no value
for. §6's *"tune until you run out of levers … this terminates"* is true, but a
reader should know before the run that the answer may be "after one".

**Should say** (§6a, first paragraph): *"If you built the default business
case as §1 draws it, rows 1 and 4 are already spent and row 5 cannot be
driven. Expect key layout and broker memory to be all that is left."*

### F17. `prove.py ceiling` read the four-core case 4% lower than the suite had minutes earlier, and nothing noticed

Chain 2's suite read the four-core case at 470,555 / 476,895 / 478,384 orders/s
(07:14–07:45). `prove.py ceiling` ran at 07:52–08:04 on the same build and
configuration, and its unthrottled first step (broker at its full 2.5 cores)
read **457,050**, then 451,840 and 453,661. The ceiling command compares its
steps only with each other, so a rig that has drifted since the suite is
invisible. Why it drifted is **not known**.

**Should do:** print the suite's rate for the same case beside the ceiling's
first step, and say *"the unthrottled step reads 4% below the suite's mean for
this case; the rig has moved since the suite"* when they differ by more than
the case's own spread.
