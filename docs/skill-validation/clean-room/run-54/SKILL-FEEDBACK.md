# Skill feedback — clean-room run 54

Every place the skill or the harness was wrong, unclear, out of date, or cost
time. Each quotes the wording and says what it should say instead. Skill
checkout: `df12dcf`, level with `origin/main` at the start.

## Before the build

### F1. Question 2's default does not say every order has 4 allocations

> *"Default: one trade input produces 1 position per symbol and one position
> per allocation. If the order has 4 allocations it emits 5 records."*

"If the order has 4 allocations" reads as an example, not as a rule. The
harness needs a **constant** fan-out (`outputsPerInput`), so the allocation
count cannot vary per order, and question 3's "2 accounts and each has 2
sub-accounts" only makes 4 the natural number by inference. I had to decide it
and write it down as an assumption.

**Should say:** *"Every order has exactly 4 allocations, one to each account /
sub-account pair, so it emits 5 records: 1 symbol position and 4 account
positions."*

### F2. The template placeholders are documented in two places, and neither is complete

`harness/README.md`, `job.args`:

> *"`args` is a template: `{bootstrap}` `{in}` `{out0}` `{out1}`… `{group}`
> `{par}` `{ckptMs}`, and every input the requirements declare in
> `design.inputs`…"*

The job needs to know the names of the two market-value topics. The README
never says `{also0}`, `{also1}` or `{alsos}` exist, nor `{partitions}`. I found
them only by reading `Cfg.fmt` in `lib.py`, whose own comment says they were
added so a run would not hand-type topic names. Likewise `generator.cmd` is
documented as filling *"`{topic}` with `{count}` records from `{seed}` and
writes `{manifest}` … bootstrap `{bootstrapExt}`"*, but the example's
`_topics` note says *"Create and fill the price topic yourself (the generator is
the natural place…)"* — without saying the generator is given `{in1}` to do it
with. It is (same `fmt`), but nothing says so.

**Should say:** one table of every placeholder `fmt` fills, and which commands
get which: `{java} {jar} {bootstrap} {bootstrapExt} {partitions} {ckptMs}
{seed} {in} {in0}… {ins} {out0}… {outs} {also0}… {alsos}`, plus the
per-command ones (`{group} {par}` for the job, `{count} {topic} {manifest}`
for the generator, `{manifest} {arm}` for the verifier).

### F3. The worker's memory limit is 1.25 × its process size, and only the code says so

The harness README says preflight *"fails a configuration where the pipeline at
its largest case plus the broker plus the job manager do not leave the VM a
spare gigabyte"*, and gives `tmMemoryBase` + `tmMemoryPerCore` × cores. It does
not say the container limit the budget counts is **1.25 ×** that figure
(`tm_memory_for` in `lib.py`: *"headroom over the JVM's own figure"*), nor that
the job manager is budgeted at its 2 g container limit. With the example's own
values (1088m + 640m × 4 = 3,648m, so 4,560m limit; broker 6g; job manager 2g)
the sum is 12,752 MiB, which does not leave a spare gigabyte on this machine's
12,955 MiB VM. The example is labelled "not a template", but it is the only
worked budget, and a reader has to read the code to find out it does not fit an
ordinary 16 GB laptop.

**Should say**, in the `caps` row: *"The budget is: worker limit at the largest
case = 1.25 × (tmMemoryBase + tmMemoryPerCore × cores), plus kafkaMemory, plus
2 GiB for the job manager, plus 1 GiB spare, against the Docker VM's memory.
The example's values need 13.8 GiB; on a 13 GiB VM, lower the broker or the
per-core share."*

### F4. Section 2's review rhythm contradicts an unattended run and says nothing about it

> *"One branch per step, squash-merged. Each step ends with the system **running
> and measured**, not compiling. Pause for review between steps"*

Section 1 and §1a both say what to do with nobody to ask. Section 2 does not:
with no reviewer there is nobody to pause for, and `prove.py all` runs the
seven steps as one chain, so "a branch per step" has no step to attach to.

**Should say:** *"With no human to review, keep one branch and commit at each
of the harness's step boundaries (§9), with the build hash in the message."*

### F5. The way the skill says to wait leaves the waiting agent no way to read the waiting

> *"If you are an agent, put the path inside a file, not on a command line."*

I did exactly this (`scratch/w.sh`, run from the parent by a relative path).
But Claude Code's background-command tool writes the watcher's output to a file
under `/private/tmp`, and this run's rules forbid reading anything there,
because other runs' files live there. So `watch.sh`'s printed progress could
not be read; I read `results/PROGRESS.txt` directly instead. The skill cannot
know an operator's rules, but it can make the watcher's output land somewhere
safe.

**Should say:** *"`watch.sh` also appends each line it prints to
`$PROJECT_DIR/scratch/watch.log`"* — or take an output path — so the progress
lands in the run's own directory whatever the agent's tool does with stdout.
