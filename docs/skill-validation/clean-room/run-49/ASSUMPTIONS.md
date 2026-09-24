# Assumptions

**Mine, and not approved by anyone.** This is a clean-room run with no human to
ask. Every line below is a decision the six answers in `ANSWERS.md` did not
settle, written beside the claim it feeds. A reader can object to any of them.

## What the answers settle and how I read them

| topic | what I decided | why / the claim it feeds |
|---|---|---|
| inputs | Two topics. `orders` is the one the harness fills, measures and drains (question 1: "That input is the one scaled up"). `prices` is the second input, created and filled by my generator, left out of `topics.in`/`topics.out`. | Question 1 and the skill's note under question 2. |
| outputs | Four topics. `positions-by-symbol` and `positions-by-account` are per input and are the harness's `topics.out`. `market-values-by-symbol` and `market-values-by-account` are throttled, the pipeline's own, listed in `topicsAlsoWritten`. | Question 1 says the price join applies to **each** position output, so two market values, not one. |
| allocations per order | **Every order has exactly 4 allocations, one to each account / sub-account pair** (2 accounts x 2 sub-accounts). | Question 2 says "if the order has 4 allocations it emits 5 records", and question 3 gives exactly 4 account / sub-account pairs. The harness needs a constant fan-out, so it is fixed at 4. Fan-out = 1 + 4 = **5** outputs per order (`outputsPerInput: 5`). |
| allocation quantities | The four allocation quantities sum to the order quantity, and all share its sign (buy or sell). Order quantity is signed, never zero. | Makes the "two paths over one input agree" assertion possible: symbol position = sum of the four account positions for that symbol. |
| distinct keys | 4,096 symbol keys; 4 x 4,096 = **16,384** account / sub-account / symbol keys. Symbol names `SYM0000`–`SYM4095`, accounts `ACC1`,`ACC2`, sub-accounts `SUB1`,`SUB2`; account key string is `ACC1/SUB1/SYM0000`. | Question 3. The verifier asserts both counts exactly. |
| prices | 4 price ticks per symbol (16,384 records), each with a distinct timestamp, written in a shuffled order. "Latest price" = the tick with the largest timestamp, not the last one read. Prices are integer cents. Fixed seed, independent of the order backlog, written once. | Question 1 ("keyed by symbol and timestamp"); question 4 "latest price". Integer cents make position x price exact, so the verifier needs no tolerance. |
| how prices reach both aggregations | Broadcast to both keyed operators. | The account side is keyed on account/sub-account/symbol and cannot join a symbol-keyed stream by key (SKILL section 1). |
| market-value throttle | Configurable, default 10,000 ms. Each key, once seen, holds a processing-time timer on the 10 s boundary. When it fires it emits a market value only if the position or the price changed since the key last emitted. So a key emits at most once per 10 s, and within 10 s of its last change. | Question 2. Emitting only on change keeps the output proportional to activity; the timer never stops so a late price still produces a correct final value. |
| position operator and market value in one operator | Each aggregation is one keyed broadcast operator: it publishes the position on every input (main output) and the market value on the timer (side output). The design block names both `positions-by-X` and `market-value-by-X`, and both names are in the operator's name in the running plan. | Splitting them would serialize every position a second time across a two-input operator that cannot chain; the throttled output does not need a vertex of its own. **This is a design decision a reader might object to**: the diagram in SKILL section 1 draws them as separate boxes. |
| "published in order" | Per key only. Every published row carries `seq`, how many inputs that key has absorbed. The verifier asserts `seq` never goes backwards per key on the clean arm, and goes backwards at most once per key on the killed arm. | Question 4; SKILL section 4. Order across keys is not a guarantee a keyed stream makes, and I do not claim it. |
| "duplicates handled, not double counted" | Read as **replays after a failure**, handled by the two settings in SKILL section 4: exactly-once checkpointing of state, and an at-least-once sink that emits the absolute position per key. The killed arm is what proves it. **I do not inject duplicate order ids into the input and the job does not de-duplicate by order id.** | SKILL section 4 says "how the duplicates are handled is a build decision". De-duplicating by id across 300,000,000 unique ids would need state that grows with the backlog and would change what is being measured. A reader who meant producer-side duplicate inputs should object here. |
| input format | JSON, one order per record, about 230 bytes, parsed once with Jackson's streaming parser. The generator compresses its batches with lz4. | A pipeline that is cheap per record measures the broker, not the cores (SKILL 6a). Parsing is real work. |
| output format | Small JSON per row, keyed by the aggregation key string, so each key lands in one partition. Sink batches compressed with lz4. | "Each key appears in exactly one partition" (SKILL 4). Compression per SKILL 6a row 4, chosen up front, not as a tuning change. |
| Kafka client | Apache `kafka-clients` (from `flink-connector-kafka` 3.3.0-1.20) in the job, generator and verifier. No Schema Registry, no Confluent serializers. | Question 6 note: the build must run on either broker with a change to `images.kafka` and `images.kafkaLibs` only. |
| Kafka broker | `confluentinc/cp-kafka:7.7.0` (Kafka 3.7 under Confluent's packaging), KRaft, one broker, libs at `/usr/share/java/kafka`. Image and path taken from SKILL question 6's table. | Question 6 = Confluent. |

## Settings the answers do not touch

| setting | value | why |
|---|---|---|
| partitions | 8 | divides 1, 2 and 4 evenly (SKILL 6) |
| cases, baseline, passes | 1, 2, 4 cores; baseline 1; 3 passes per case plus the sentinel = 10 measured cases | SKILL 1a: always 1, 2 and 4 |
| checkpoint interval | 10,000 ms, exactly-once | SKILL 1a / 4 |
| broker cap | 2.5 cores | **not measured**: nothing tells me the right figure for this pipeline. If the broker turns out to be the ceiling, the harness will say so. |
| broker memory | 5,376m with a 1G heap = 4.25 GiB page cache, the least any accepted configuration on record left (read from the harness's own `record/configs.json`) | the preflight floor |
| worker memory | `tmMemoryBase` 1024m + `tmMemoryPerCore` 512m = 1,536m / 2,048m / 3,072m at 1 / 2 / 4 cores | my state is tiny (20,480 keys); this is chosen to leave the Docker VM room, not measured. Over-committed against the 9,937 MiB VM with the broker at 5,376m; the harness says that is normal because the broker's cache is elastic. |
| garbage collector | G1 pinned on every case (`-XX:+UseG1GC`) | SKILL 5, run 35 |
| max parallelism | not set up front; preflight's key-layout row will say if one is needed | 4,096 and 16,384 keys over 128 key groups should spread; to be checked, not assumed |
| dashboard | Prometheus + Grafana + the shipped `docker_cpu_exporter.py`, all through `extraServices`, all CPU-capped (0.25 / 0.25 / 0.1 cores) | SKILL 7 |
| backlogs | suite 300,000,000; tiny proof 160,000,000; completeness 20,000,000; kill at 40% | **guesses**: a 4-core rate of 600k–1M orders/s is my guess, not a measurement. The tiny proof will re-size them. |

## Assertions that do not apply, or apply only partly

| SKILL 4 assertion | status |
|---|---|
| distinct keys = predicted | applies: 4,096 and 16,384, asserted exactly |
| every aggregation sums to the manifest | applies: both position levels, exact |
| two paths agree | applies: symbol position = sum of its four account positions, on the outputs |
| never goes backwards per key | applies, via `seq`, on positions and market values, both levels |
| each key in exactly one partition | applies, all four output topics |
| after the kill, all of the above | applies, with "backwards at most once per key" |
| market value = final position x latest price | applies, both levels, exact integer arithmetic |
| input duplicates by order id | **not checked** — see "duplicates" above |
