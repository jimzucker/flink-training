"""Readiness probe, 2026-10-06 (item 4), plus a live check of statement_rows.

Six stacks one after another, alternating the reused environment name
`flink-training` with a fresh name each time. Each: up (readiness: first
statement, then a row written to a new table and read back from Kafka), its
timings or the reason it was unusable, then down. On the first usable stack
only: a three-row table read back with a bounded GROUP BY through the harness's
statement_rows; the expected answer is known. Everything prefixed
flink-training; every stack torn down."""
import json, os, sys, time
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "harness"))          # a snapshot of the branch under test
import lib as L
import platform_confluent as PC

OUT = sys.argv[1]
say = lambda *a: print(time.strftime("%H:%M:%S"), *a, flush=True)
names = ["flink-training", f"flink-training-r{int(time.time()) % 100000}a",
         "flink-training", f"flink-training-r{int(time.time()) % 100000}b",
         "flink-training", f"flink-training-r{int(time.time()) % 100000}c"]
results, rows_checked = [], False
for k, env in enumerate(names, 1):
    state = os.path.join(OUT, f"stack{k}")
    os.makedirs(state, exist_ok=True)
    p = PC.ConfluentCloud({"environment": env, "prefix": "flink-training", "stateDir": state, "estimateUsd": 2.0,
                           "credentials": os.path.join(state, "creds.env")}, log=print)
    r = {"stack": k, "environment": env, "reused": env == "flink-training", "t0": time.time()}
    try:
        p.up()
        r["usable"] = True
        r["firstStatementS"] = p.state.get("readyAfterS")
        r["rowReadBackS"] = p.state.get("usableAfterS")
        if not rows_checked:
            rows_checked = True
            db = p.database()
            p.run_statement("flink-training-rows-ddl", "CREATE TABLE t_rows (account INT, qty INT) "
                            "DISTRIBUTED INTO 4 BUCKETS", db, pool=p.state["pool"])
            p.run_statement("flink-training-rows-insert", "INSERT INTO t_rows VALUES (1, 10), (1, 5), (2, 3)", db,
                            pool=p.state["pool"])
            got = p.statement_rows("flink-training-rows-count",
                                   "SELECT account, COUNT(*) AS n, SUM(qty) AS qty FROM t_rows "
                                   "/*+ OPTIONS('scan.startup.mode'='earliest-offset', "
                                   "'scan.bounded.mode'='latest-offset') */ GROUP BY account")
            want = [{"account": 1, "n": 2, "qty": 15}, {"account": 2, "n": 1, "qty": 3}]
            norm = lambda rs: sorted(json.dumps({k2: int(v) for k2, v in x.items()}, sort_keys=True) for x in rs)
            r["rows"] = got
            r["rowsMatch"] = norm(got) == norm(want)
            say(f"ROWS stack {k}: {got} -> {'match' if r['rowsMatch'] else 'DO NOT match'} {want}")
    except L.Refusal as e:
        r["usable"] = False
        r["reason"] = e.msg[:400]
        r["firstStatementS"] = p.state.get("readyAfterS") if p.state else None
    finally:
        try:
            p.down()
            r["survivors"] = p.surviving()
        except L.Refusal as e:
            r["teardown"] = e.msg[:300]
    r["seconds"] = round(time.time() - r["t0"])
    results.append(r)
    say(f"STACK {k} ({'reused name' if r['reused'] else 'fresh name'} {env}): "
        f"{'usable' if r['usable'] else 'NOT usable'}; first statement {r.get('firstStatementS')} s; "
        f"row read back {r.get('rowReadBackS')} s; {r.get('reason', '')[:200]}")
    json.dump(results, open(os.path.join(OUT, "results.json"), "w"), indent=1, default=str)
say("DONE " + json.dumps([{k3: x.get(k3) for k3 in ("stack", "reused", "usable", "firstStatementS")} for x in results]))
