#!/usr/bin/env python3
"""
prove-it-scales harness — the one entry point.

    python3 harness/prove.py <command>     (run from the directory holding pipeline.json,
                                            or set PIPELINE_JSON=/path/to/pipeline.json)

  replay        check every threshold against the recorded runs   (no stack; seconds)
  selftest      break every guard on purpose                       (stack up; ~3 min)
  up            generate stack/compose.yml, bring it up, compile the sampler
  preflight     the §3 table, PASS/FAIL per row
  tinyproof     two cases on a small backlog, ratio bounded 1.5x-2.5x, + selftest
  fill          fill the full backlog and write results/manifest.json (run it detached)
  completeness  drain a small backlog twice (clean, worker killed), verify, no tolerances
  suite         the table: every case, N passes, asc/desc, rig vs data refusals
  ceiling       hold the largest case, starve the broker in steps
  report        results/suite.json -> results/suite.txt + results/suite.md
  down          tear everything down, assert nothing survives, fstrim
  all           up -> preflight -> completeness -> tinyproof -> fill -> suite -> report,
                one stack session, stops at the first non-zero step; results/phases.log
                carries the timestamps and results/DONE the outcome   (run it detached)

Exit code 0 means the command's own assertion held; anything else, read the log.
"""
import json
import os
import shutil
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lib as L  # noqa: E402
from lib import (T, Refusal, CaseRefused, cfg, log, sh, rest, save_json, load_json,  # noqa: E402
                 build_hash, build_table, render_table, render_markdown)


# ---------------------------------------------------------------------- replay

def replay_names():
    """Names that only exist in another scope. The pure self-test and the replay
    both pass without ever running preflight, so a defect like that reaches the
    rig; one did on 2026-09-08 and cost a chain attempt."""
    import namecheck
    bad = []
    for f in ("prove.py", "lib.py"):
        bad += [(f,) + b for b in namecheck.undefined(os.path.join(L.HERE, f))]
    for f, line, scope, name in bad:
        print(f"  UNDEFINED: {f}:{line} {scope}() reads {name!r}")
    print(f"checked 2 harness files for undefined names" + ("" if not bad else f" — {len(bad)} FOUND"))
    return 1 if bad else 0


def replay_cases():
    """Case verdicts against classifications whose answer is already known.
    The suite record has no cap fractions or broker counters, so a change to
    what makes a case a scaling claim cannot be replayed against it."""
    path = os.path.join(L.HERE, "record", "cases.json")
    if not os.path.exists(path):
        return 0
    doc = json.load(open(path))
    base = dict(boundaries=6, recordsConsumed=10_000_000, elapsedS=60.0, recordsPerSec=166_666.7,
                rateSource="kafka committed offsets", vantageSinkRecords=10_010_000.0,
                vantageDisagreement=0.001, backlogRemaining=50_000_000, headroomS=300.0,
                sourceIdle=0.0, tmCapFrac=0.99, bpSamples=6, brokerLimitHits=0, brokerRefaults=0)
    bad = 0
    for entry in doc["cases"]:
        rec = dict(base); rec.update(entry["rec"])
        got = "claim"
        try:
            L.check_case(rec, 4, False)
        except L.Ceiling:
            got = "ceiling"
        except Refusal:
            got = "invalid"
        if got != entry["expect"]:
            bad += 1
            print(f"  DISAGREES: {entry['name']}: expected {entry['expect']}, got {got}")
    print(f"replayed {len(doc['cases'])} recorded case verdicts" + ("" if not bad else f" — {bad} DISAGREE"))
    return 1 if bad else 0


def replay_configs():
    """Config guards against configurations whose verdict we already know.

    The suite record cannot exercise these: it re-derives step verdicts from
    recorded rates and never builds a Cfg. #68 shipped a rule that refused two
    configurations which had already produced accepted runs, and cost a whole
    clean-room run to find out. Same discipline as the suite replay -- a guard
    that disagrees with the record is wrong.
    """
    path = os.path.join(L.HERE, "record", "configs.json")
    if not os.path.exists(path):
        return 0
    doc = json.load(open(path))
    bad = 0
    for entry in doc["configs"]:
        cfg_doc = json.load(open(os.path.join(L.HERE, "pipeline.example.json")))
        cfg_doc["cases"] = entry["cases"]
        cfg_doc["baseline"] = min(entry["cases"])
        cfg_doc["partitions"] = entry["partitions"]
        cfg_doc["caps"] = entry["caps"]
        tmp = os.path.join(tempfile.mkdtemp(prefix="replay-cfg-"), "pipeline.json")
        json.dump(cfg_doc, open(tmp, "w"))
        got, why = "accept", ""
        try:
            c = L.Cfg(tmp)
            if [n for n in c.cases if c.partitions % n]:
                got, why = "refuse", f"{c.partitions} partitions do not divide by {c.cases}"
        except Refusal as e:
            got, why = "refuse", e.msg
        if got != entry["expect"]:
            bad += 1
            print(f"  DISAGREES: {entry['name']}: expected {entry['expect']}, got {got}"
                  + (f" ({why[:90]})" if why else ""))
    print(f"replayed {len(doc['configs'])} recorded configurations"
          + ("" if not bad else f" — {bad} DISAGREE"))
    return 1 if bad else 0


def cmd_replay():
    """Every threshold, checked against every recorded suite before it can refuse
    anything new. record/*.json holds per-pass rates per case and a verdict on
    which step ratios the record considers valid."""
    import glob
    rec_dir = os.path.join(L.HERE, "record")
    files = [f for f in sorted(glob.glob(os.path.join(rec_dir, "*.json")))
             if os.path.basename(f) not in ("configs.json", "cases.json")]
    bad, n = [], 0
    for f in files:
        d = json.load(open(f))
        runs = []
        for cores, rates in d["rates"].items():
            for i, r in enumerate(rates):
                runs.append({"cores": int(cores), "pass": f"p{i+1}", "recordsPerSec": float(r), "status": "OK"})
        t = build_table(runs)
        for step, valid in d.get("validSteps", {}).items():
            n += 1
            got = next((x for x in t["stepRatios"] if x["step"] == step), None)
            reportable = bool(got and got["reportable"])
            if valid and not reportable:
                bad.append(f"{os.path.basename(f)}: {step} is valid in the record but would be voided "
                           f"({got['reason'] if got else 'no such step'})")
            if not valid and reportable:
                bad.append(f"{os.path.basename(f)}: {step} is NOT valid in the record but would be reported "
                           f"({got['ratio']}x)")
    print(f"replayed {len(files)} recorded suites, {n} step verdicts, thresholds "
          f"spreadCeil={T['spreadCeil']:.0%} minPasses={T['minPasses']}")
    for b in bad:
        print("  DISAGREES:", b)
    if bad:
        print("REPLAY FAILED: a threshold disagrees with the record. Fix the threshold, not the record.")
        return 1
    if replay_names() or replay_cases() or replay_configs():
        print("REPLAY FAILED: a guard disagrees with a recorded configuration. Fix the guard, not the record.")
        return 1
    print("REPLAY OK: no recorded valid table would be refused, no recorded invalid one reported, "
          "and every recorded configuration still gets its recorded verdict")
    return 0


# -------------------------------------------------------------------- selftest

def cmd_selftest(live=True, topic=None):
    """A guard that has never fired is a guess. Each guard is broken on purpose
    through the same code path the suite uses."""
    c = cfg()
    results = []
    t_self = time.time()

    def expect(name, fn, needle, should_fire=True, ceiling=False):
        try:
            fn()
            res = dict(guard=name, ok=not should_fire, result="DID NOT FIRE")
        except L.Ceiling as e:
            res = dict(guard=name, ok=should_fire and ceiling and needle.lower() in e.msg.lower(),
                       result="CEILING", message=e.msg[:200])
        except (Refusal, CaseRefused) as e:
            msg = e.msg if isinstance(e, Refusal) else e.refusal.msg
            res = dict(guard=name, ok=should_fire and needle.lower() in msg.lower(), result="REFUSED", message=msg[:200])
        except Exception as e:
            res = dict(guard=name, ok=False, result=f"WRONG ERROR {type(e).__name__}: {e}"[:200])
        results.append(res)
        print(("  ok  " if res["ok"] else "  BAD ") + f"{name:46s} -> {res['result']}"
              + (" | " + res["message"][:100] if "message" in res else ""), flush=True)

    good = dict(boundaries=6, recordsConsumed=10_000_000, elapsedS=60.0, recordsPerSec=166_666.7,
                rateSource="kafka committed offsets", vantageSinkRecords=10_010_000.0,
                vantageDisagreement=0.001, backlogRemaining=50_000_000, headroomS=300.0,
                sourceIdle=0.0, tmCapFrac=0.99, bpSamples=6)

    def case(**kw):
        r = dict(good); r.update(kw)
        return lambda: L.check_case(r, 4, kw.pop("_baseline", False))

    print("pure guards (synthetic case records through check_case):")
    expect("window has < 3 commit boundaries", case(boundaries=2), "commit boundaries")
    expect("measured rate is zero", case(recordsConsumed=0), "not positive")
    expect("rate came from the engine", case(rateSource="engine numRecordsIn"), "engine")
    expect("two vantage points disagree", case(vantageDisagreement=0.12), "vantage")
    expect("backlog lacks headroom at close", case(backlogRemaining=1000, headroomS=0.006), "headroom")
    expect("external-boundary samples missing", case(sourceIdle=None), "samples")
    expect("too few reporter samples in the window", case(bpSamples=2), "samples")
    expect("worker is not the constraint (baseline, run 5\'s 94%)", case(tmCapFrac=0.94, _baseline=True), "not the constraint", ceiling=True)
    expect("baseline at 95.9% is the constraint (run 12 p3; must not fire)",
           case(tmCapFrac=0.959, _baseline=True), "", should_fire=False)
    expect("worker is not the constraint (other)", case(tmCapFrac=0.90), "not the constraint", ceiling=True)
    expect("source idle past the ceiling", case(sourceIdle=0.4), "waited on input", ceiling=True)
    expect("the broker was starved of page cache (worker off its cap)",
           case(brokerLimitHits=310423, brokerRefaults=6270562, tmCapFrac=0.964), "memory limit", ceiling=True)
    expect("broker limit hits while the worker is pinned (must not fire)",
           case(brokerLimitHits=9437, brokerRefaults=572000, tmCapFrac=0.996), "", should_fire=False)
    expect("a broker that never hit its limit (must not fire)",
           case(brokerLimitHits=0, brokerRefaults=1200), "", should_fire=False)
    expect("job graph differs across cases",
           lambda: L.check_shape({"vertexCount": 3, "signature": [["a", ["HASH"]]]},
                                 {"vertexCount": 3, "signature": [["a", ["REBALANCE"]]]}), "shape")

    def spread():
        t = build_table([{"cores": 2, "pass": "p1", "recordsPerSec": 100.0},
                         {"cores": 2, "pass": "p2", "recordsPerSec": 130.0},
                         {"cores": 4, "pass": "p1", "recordsPerSec": 200.0},
                         {"cores": 4, "pass": "p2", "recordsPerSec": 205.0}])
        c2, step = t["cases"][2], t["stepRatios"][0]
        if c2["reportable"] or step["reportable"] or t["cases"][4]["reportable"] is not True:
            raise Exception(f"spread guard did not void the case and only the case: {t}")
        raise Refusal("case", c2["unreportableReason"])
    expect("a case's passes spread past the ceiling", spread, "spread")

    def one_pass():
        t = build_table([{"cores": 2, "pass": "p1", "recordsPerSec": 100.0},
                         {"cores": 4, "pass": "p1", "recordsPerSec": 200.0}])
        if t["cases"][2]["reportable"]:
            raise Exception("single pass was reportable")
        raise Refusal("case", t["cases"][2]["unreportableReason"])
    expect("a case measured only once", one_pass, "pass")

    def split_commit_boundary():
        """REGRESSION, from the rig's own ticks (2026-09-05, 4c, 10 s checkpoints):
        a commit arrived as +1,902,797 then +5,672,429 half a second later, the
        window closed on the first piece, and the vantage points disagreed by 33%."""
        ticks = [{"ts": 39590, "committed": 54613970}, {"ts": 49600, "committed": 56516767},
                 {"ts": 50100, "committed": 62189196}, {"ts": 59610, "committed": 69800000}]
        got = L.settle_boundary(ticks, ticks[1], 1500.0)
        if got["committed"] != 62189196:
            raise Exception(f"the window closed on half a commit: {got}")
        whole = L.settle_boundary(ticks, ticks[3], 1500.0)
        if whole["committed"] != 69800000:
            raise Exception(f"a settled boundary was dragged forward into the next commit: {whole}")
        clean = [{"ts": 1000, "committed": 100}, {"ts": 11000, "committed": 200}]
        if L.settle_boundary(clean, clean[0], 1500.0)["ts"] != 1000:
            raise Exception("a boundary with nothing to settle was moved")
    expect("a commit that lands in two pieces (must not fire)", split_commit_boundary, "", should_fire=False)

    def quick_does_not_leak(): 
        """REGRESSION (2026-09-05): with the flag set process-wide, the replay
        re-derived the record with minPasses bypassed and three recorded-invalid
        suites would have been reported, and this file's own single-pass guard
        stopped firing. Both correctly refused a run. Quickness belongs to one
        table, never to the record or the guards."""
        prev = L.QUICK          # restore, never assume: setting this False at the
        L.QUICK = True          # end turned quick mode off for the rest of a live
        try:                    # chain on 2026-09-05, and the suite voided itself
            if cmd_replay() != 0:
                raise Exception("the replay disagreed with the record while --quick was set")
            t = build_table([{"cores": 2, "pass": "p1", "recordsPerSec": 100.0},
                             {"cores": 4, "pass": "p1", "recordsPerSec": 200.0}])
            if t["cases"][2]["reportable"] or t.get("quickLook"):
                raise Exception(f"the flag leaked into a table that did not ask for it: {t['cases'][2]}")
        finally:
            L.QUICK = prev
        if L.QUICK != prev:
            raise Exception("the self-test did not restore the quick flag")
    expect("quick look: the flag reaches neither the record nor the guards (must not fire)",
           quick_does_not_leak, "", should_fire=False)

    def quick_two_passes():
        """REGRESSION (2026-09-06): --quick measured each case once, and a ratio
        built from two single measurements wandered 8.3% against the same
        build's three-pass answer. It now measures twice."""
        if L.T["quickPasses"] < 2:
            raise Exception(f"quick mode would report a ratio from {L.T['quickPasses']} pass(es)")
        t = build_table([{"cores": 2, "pass": "p1", "recordsPerSec": 100.0},
                         {"cores": 2, "pass": "p2", "recordsPerSec": 104.0},
                         {"cores": 4, "pass": "p1", "recordsPerSec": 200.0},
                         {"cores": 4, "pass": "p2", "recordsPerSec": 208.0}], quick=True)
        if t["cases"][2]["spread"] <= 0 or not t["stepRatios"][0]["reportable"]:
            raise Exception(f"a two-pass quick table must carry a spread and a ratio: {t['cases'][2]}")
    expect("quick look: two passes, so every case has a spread (must not fire)",
           quick_two_passes, "", should_fire=False)

    def quick_marks_unpublishable():
        """QUICK: one pass per case still produces numbers, and every one of them
        is stamped unpublishable. The mode exists to answer 'did it run clean and
        how fast', and the record says a single pass lands anywhere in a band
        wider than the accept line (2.04-2.27x where a suite reported 2.15x)."""
        t = build_table([{"cores": 2, "pass": "p1-asc", "recordsPerSec": 100.0},
                         {"cores": 4, "pass": "p1-asc", "recordsPerSec": 200.0}], quick=True)
        c2, step = t["cases"][2], t["stepRatios"][0]
        if t.get("publishable") is not False or t.get("quickLook") is not True:
            raise Exception(f"quick table was not stamped: {t.get('quickLook')} {t.get('publishable')}")
        if c2.get("publishable") is not False or not c2["reportable"] or not step["reportable"]:
            raise Exception(f"quick mode should compute the ratio and mark it unpublishable: {c2} {step}")
        if abs(step["ratio"] - 2.0) > 1e-9:
            raise Exception(f"quick ratio wrong: {step}")
    expect("quick look: one pass computes a ratio, stamped unpublishable (must not fire)",
           quick_marks_unpublishable, "", should_fire=False)

    def sentinel_drift():
        runs = [{"cores": 2, "pass": "p1-asc", "recordsPerSec": 400.0},
                {"cores": 4, "pass": "p1-asc", "recordsPerSec": 800.0},
                {"cores": 4, "pass": "p2-desc", "recordsPerSec": 790.0},
                {"cores": 2, "pass": "p2-desc", "recordsPerSec": 395.0},
                {"cores": 2, "pass": "sentinel", "recordsPerSec": 300.0}]
        t = build_table(runs)
        sd = t["sentinel"]
        if sd is None or abs(sd["drift"] - (300.0 - 400.0) / 350.0) > 1e-3:
            raise Exception(f"sentinel drift not computed: {sd}")
        if t["cases"][2]["reportable"]:
            raise Exception("a 25% first-to-last drift left the baseline reportable")
        raise Refusal("case", f"sentinel drift {sd['drift']:.1%}: " + t["cases"][2]["unreportableReason"])
    expect("sentinel: rig drifted across the suite", sentinel_drift, "spread")

    def sentinel_ok():
        runs = [{"cores": 2, "pass": "p1-asc", "recordsPerSec": 400.0},
                {"cores": 4, "pass": "p1-asc", "recordsPerSec": 800.0},
                {"cores": 2, "pass": "sentinel", "recordsPerSec": 370.0}]
        t = build_table(runs)
        if not t["cases"][2]["reportable"] or t["sentinel"]["drift"] > 0:
            raise Exception(f"an 8% drift (inside the 10% noise floor) was refused: {t['sentinel']}")
    expect("sentinel: drift inside the noise floor (must not fire)", sentinel_ok, "", should_fire=False)

    def watcher():
        import subprocess
        os.makedirs(c.results, exist_ok=True)
        probe = os.path.join(c.results, "selftest-watcher.log")
        open(probe, "a").close()
        # three shapes of watcher: names the project on its command line; holds a
        # results file open; names prove.py and runs from inside the project (run 11's shells)
        w = subprocess.Popen(["bash", "-c", f"while true; do ls {c.results} >/dev/null; sleep 5; done"],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True, cwd="/")
        t = subprocess.Popen(["tail", "-f", probe], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                             start_new_session=True, cwd="/")
        u = subprocess.Popen(["bash", "-c", "while true; do pgrep -f 'prove.py suite' >/dev/null; sleep 5; done"],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True, cwd=c.root)
        # and two that must NOT be touched: another project's harness elsewhere, and a
        # bystander shell merely sitting in the project directory
        other = subprocess.Popen(["bash", "-c", "exec -a 'python3 harness/prove.py tinyproof' sleep 300"],
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True, cwd="/")
        bystander = subprocess.Popen(["bash", "-c", "sleep 300"], stdout=subprocess.DEVNULL,
                                     stderr=subprocess.DEVNULL, start_new_session=True, cwd=c.root)
        time.sleep(0.5)
        # they are our children here; the suite's watchers are not, so look at them as strangers
        found = {pid for pid, _ in L.host_watchers(ignore_children=True)}
        want = {w.pid, t.pid, u.pid}
        if not want <= found or other.pid in found or bystander.pid in found:
            for x in (w, t, u, other, bystander):
                x.kill()
            raise Exception(f"host_watchers wanted {sorted(want)} and not {other.pid}/{bystander.pid}; got {sorted(found)}")
        L.reap_host_watchers(ignore_children=True)
        time.sleep(0.5)
        left = {pid for pid, _ in L.host_watchers(ignore_children=True)} & want
        other_alive = other.poll() is None and bystander.poll() is None
        other.kill(); bystander.kill()
        try:
            os.remove(probe)
        except OSError:
            pass
        if left:
            raise Exception(f"watchers survived reaping: {left}")
        if not other_alive:
            raise Exception("another project's harness, or a bystander shell in the project directory, was killed")
        raise Refusal("rig", f"host processes watching this run were found and killed: {sorted(want)}")
    expect("a host-side watcher outlives the run", watcher, "watching this run")

    # run 11's build A: 210.6 B/input, 607 B of sink per input, 200M backlog, 8 partitions,
    # two sinks, 103 GB free. Retention caps the sinks at 34.4 GB; it fitted, and the
    # rebuild that re-ran both gates was not needed.
    run11 = dict(in_bytes_per_rec=210.6, backlog=200_000_000, sink_bytes_per_in=607.0, partitions=8,
                 n_out_topics=2, ckpt_bytes=1e9)
    expect("disk: run 11's build A fitted (must not fire)",
           lambda: L.disk_verdict(103e9, **run11), "", should_fire=False)
    expect("disk: the suite would not fit", lambda: L.disk_verdict(60e9, **run11), "would not fit")

    def chain():
        # in its own directory: the first version wrote its fake chain into the
        # live results/ (phases.log, all.json and a DONE saying "FAIL at c")
        # while a real `all` was running the live self-test around it
        ran = []
        fake = [(n, (lambda n=n: (ran.append(n), 0)[1])) for n in ("a", "b")]
        fake += [("c", lambda: (ran.append("c"), 1)[1]), ("d", lambda: (ran.append("d"), 0)[1])]
        tmp = tempfile.mkdtemp(prefix="prove-all-selftest-")
        try:
            rc = cmd_all(steps=fake, results=tmp)
            done = open(os.path.join(tmp, "DONE")).read().strip()
            allj = json.load(open(os.path.join(tmp, "all.json")))
            stray = [f for f in ("DONE", "all.json") if os.path.exists(os.path.join(c.results, f))
                     and os.path.getmtime(os.path.join(c.results, f)) > t_self]
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
        if rc != 1 or ran != ["a", "b", "c"] or not done.startswith("FAIL at c") or allj["verdict"] != "FAIL at c":
            raise Exception(f"rc={rc} ran={ran} DONE={done!r} verdict={allj.get('verdict')}")
        if stray:
            raise Exception(f"the self-test wrote into the live results directory: {stray}")
        raise Refusal("rig", f"chain stopped at c, d never ran, DONE says {done!r}")
    expect("all: the chain stops at the first failing step", chain, "stopped at c")

    def warm():
        ok, d = L.warmup_verdict([325e3, 259e3, 241e3, 340e3], 120)
        if ok:
            raise Exception("a 40% scatter was called flat")
        raise Refusal("case", f"scatter {d['scatter']:.0%}")
    expect("warm-up accepts a noisy ramp", warm, "scatter")

    def warm_ok():
        ok, d = L.warmup_verdict([400e3, 405e3, 398e3, 402e3], 120)
        if not ok:
            raise Refusal("case", f"flat plateau rejected: {d}")
    expect("warm-up accepts a flat plateau (must not fire)", warm_ok, "", should_fire=False)

    def good_case():
        L.check_case(dict(good), 4, False)
    expect("a valid case record passes (must not fire)", good_case, "", should_fire=False)

    if live:
        print("live guards (the rig, broken on purpose):")
        rest("/overview")  # stack must be up

        def wrong_cap():
            L.stop_tm()
            sh(f"docker run -d --name {c.tm} --network {c.net} --cpus 2 --entrypoint sleep alpine 60")
            try:
                L.assert_cap(c.tm, 4)
            finally:
                sh(f"docker rm -f {c.tm}", check=False)
        expect("resource cap was not applied", wrong_cap, "cap did not apply")

        def busy():
            L.start_tm(2)
            try:
                L.assert_cluster_idle()
            finally:
                L.stop_tm()
        expect("cluster still busy from the last case", busy, "still registered")

        def truncated():
            # a real topic against a manifest that is one record off
            t = topic or c.topic_in
            L.verify_backlog({c.count_field: L.log_end(t)[0] - 1}, topic=t)
        expect("backlog does not match its manifest", truncated, "manifest says")

        def disk_reads():
            # the projection's two measurements against the real broker and a real volume;
            # the first live 2.6 test died here on awk quoting the pure self-test never ran
            t = topic or c.topic_in
            n = L.log_end(t)[0]
            b = L.topic_bytes([t])
            if n and b < n:
                raise Exception(f"topic {t}: {n} records but only {b} bytes on the broker")
            vb = L.volume_bytes(c.ckpt_vol)
            if vb < 0:
                raise Exception(f"volume {c.ckpt_vol}: {vb}")
        expect("disk: broker and volume sizes read (must not fire)", disk_reads, "", should_fire=False)

        def dead_sampler():
            sh(f"docker rm -f {c.sampler}", check=False)
            sh(f"docker run -d --name {c.sampler} --network {c.net} --entrypoint sh alpine -c 'echo nope; exit 1'")
            try:
                for _ in range(20):
                    lg = sh(f"docker logs {c.sampler}", check=False)
                    if '"sampler":"up"' in lg.stdout:
                        return
                    if not sh(f"docker ps -q -f name=^{c.sampler}$", check=False).stdout.strip():
                        raise Refusal("rig", f"offset sampler died at startup:\n{lg.stdout}")
                    time.sleep(0.5)
            finally:
                sh(f"docker rm -f {c.sampler}", check=False)
        expect("monitor launched unattended is dead", dead_sampler, "died at startup")

        def no_job():
            L.wait_running("0" * 32, 2, timeout=3)
        expect("no job is actually running", no_job, "never reached RUNNING")

    save_json("selftest.json", {"build": build_hash() if os.path.exists(c.jar) else None,
                                "results": results, "at": time.strftime("%Y-%m-%d %H:%M:%S")})
    bad = [r for r in results if not r["ok"]]
    print(f"{len(results)-len(bad)}/{len(results)} guards fired as expected")
    return 1 if bad else 0


# ------------------------------------------------------------------- preflight

def cmd_preflight():
    c = cfg()
    rows = []
    extra = {}

    def check(name, fn):
        try:
            detail = fn()
            rows.append((name, "PASS", detail)); print(f"PASS  {name:52s} {detail}", flush=True)
        except Exception as e:
            rows.append((name, "FAIL", str(e))); print(f"FAIL  {name:52s} {e}", flush=True)

    def arch():
        host = sh("uname -m").stdout.strip()
        out = []
        for img in (c.kafka_img, c.flink_img):
            a = sh(f"docker image inspect {img} -f '{{{{.Architecture}}}}'").stdout.strip()
            if a != host:
                raise Exception(f"{img} is {a}, host is {host} — emulated CPU numbers")
            out.append(f"{img.split(':')[0]}={a}")
        return f"host={host} " + " ".join(out)

    def jdk():
        v = sh(f"{c.java} -version 2>&1 | head -1").stdout.strip()
        inside = sh(f"docker run --rm --entrypoint java {c.flink_img} -version 2>&1 | head -1").stdout.strip()
        import re
        mh = re.search(r'"(\d+)', v); mi = re.search(r'"(\d+)', inside)
        if not mh or not mi or mh.group(1) != mi.group(1):
            raise Exception(f"host JDK {v!r} vs engine image {inside!r}: major versions differ")
        return f"host {v} | image {inside}"

    def statedir():
        sh(f"docker run --rm --user 9999:9999 -v {c.ckpt_vol}:/ckpt --entrypoint sh {c.flink_img} "
           f"-c 'mkdir -p /ckpt/.probe/shared && rmdir /ckpt/.probe/shared /ckpt/.probe'")
        return "uid 9999 (flink) can mkdir under /ckpt"

    def reporter():
        lib = sh(f"docker run --rm --entrypoint sh {c.flink_img} -c 'ls /opt/flink/lib'").stdout.split()
        plug = sh(f"docker run --rm --entrypoint sh {c.flink_img} -c 'ls -R /opt/flink/plugins'").stdout
        dup = [l for l in lib if "metrics" in l and l in plug]
        if dup:
            raise Exception(f"reporter jar in both lib/ and plugins/: {dup}")
        return "no reporter jar copied into lib/; slf4j reporter used from plugins/"

    def disk():
        in_bytes = c.backlog * 120                                     # generous bytes/record until the manifest says
        per_case_out = T["sinkRetentionBytes"] * c.partitions * len(c.topics_out)
        need = in_bytes + per_case_out + 2 * 1024 ** 3 + 10 * 1024 ** 3
        free = L.host_free_bytes()
        if free < need:
            raise Exception(f"host free {free/1e9:.1f} GB < budget {need/1e9:.1f} GB")
        return (f"host free {free/1e9:.1f} GB >= budget {need/1e9:.1f} GB "
                f"(backlog {in_bytes/1e9:.1f} + sinks at retention {per_case_out/1e9:.1f} + ckpt 2 + slack 10)")

    def retention():
        L.recreate_output_topics()
        return f"retention.bytes={T['sinkRetentionBytes']} set and read back on {c.topics_out} (a periodic sweep, not a bound)"

    def determinism():
        if not c.manifest_cmd:
            raise Exception("pipeline.json has no generator.manifestCmd (manifest-only mode)")
        hs = []
        for name in ("det-a.json", "det-b.json"):
            p = os.path.join(c.results, name)
            sh(c.fmt(c.manifest_cmd, count=200_000, seed=c.seed, manifest=p, topic=c.topic_in), timeout=600)
            hs.append(sh(f"shasum -a 256 {p}").stdout.split()[0])
        if hs[0] != hs[1]:
            raise Exception("two manifests from one seed are NOT identical")
        return f"identical sha256={hs[0][:12]} for 200,000 records at seed {c.seed}"

    def capmech():
        probe = f"{c.project}-capprobe"
        sh(f"docker rm -f {probe}", check=False)
        sh(f"docker run -d --name {probe} --cpus 2 alpine sleep 60")
        try:
            nano = L.assert_cap(probe, 2)
        finally:
            sh(f"docker rm -f {probe}", check=False)
        return f"--cpus throughout; read back NanoCpus={nano} and cgroup cpu.max = 2.0 cores"

    def memory_budget():
        """Worker at its largest case, broker and job manager must fit the VM with
        room to spare. Runs 20 and 21 each lost attempts discovering this by
        refusal instead: the broker's page cache grows into whatever cap it is
        given, and paying for that cap out of the worker drove 1-core GC to 26%."""
        info = L.sh("docker info --format '{{.MemTotal}}'", check=False).stdout.strip()
        vm = int(info) if info.isdigit() else 0
        top = max(c.cases)
        worker = L._mib(L.mem_for(c.tm_mem_per_core, top, c.tm_mem_base)) if c.tm_mem_per_core else L._mib(c.tm_mem)
        broker = L._mib(c.kafka_mem)
        jm = 1024.0
        need = worker + broker + jm
        # Reported, not enforced. A rule refusing need > VM - 1 GB was added on
        # 2026-09-07 and removed the same day: runs 20 and 21 both passed with a
        # 6,144m broker on a 7,838 MiB VM, which that rule refuses, and run 22
        # spent two chains discovering that it contradicts the broker-memory
        # hint (which asked for 5,632m where the rule allowed 3,613m). The
        # broker's page cache is elastic; over-committing it against the VM is
        # normal and the cases that matter are caught by the cap floor and the
        # broker's own limit-hit guard.
        over = " (over-committed, which is normal — the broker's cache is elastic)" if vm and need > vm / 1048576.0 else ""
        return (f"worker {worker:.0f}m at {top} cores + broker {broker:.0f}m + job manager {jm:.0f}m "
                f"= {need:.0f}m of {vm / 1048576:.0f}m VM{over}" if vm
                else f"{need:.0f}m requested, VM size unknown")

    def backlog_sizing_hint():
        """Preflight cannot know the rate yet, but it can say what the guess must
        cover so the tiny proof does not have to refuse it."""
        secs = T["warmupMinS"] + T["minWindowS"] + 3 * c.ckpt_ms / 1000.0
        top = max(c.cases)
        return (f"backlog {c.backlog:,} covers {secs:.0f}s x 1.5 at the {top}-core rate, so up to "
                f"{c.backlog / (secs * 1.5):,.0f} rec/s; the tiny proof checks this against the "
                f"measured rate")

    def memory_per_subtask():
        """Each case must give its subtasks the same memory, or the largest case
        measures memory pressure rather than cores (2026-09-07: a flat 2048m read
        2->4 = 1.645 with GC at 9.3%; the same build with memory scaled per core
        read 1.910 with GC at 2.3%, and the 2-core figure did not move)."""
        if not c.tm_mem_per_core:
            raise Exception("caps.tmMemoryPerCore is not set")
        return (f"{c.tm_mem_base} base + {c.tm_mem_per_core} per subtask: "
                + ", ".join(f"{L.mem_for(c.tm_mem_per_core, n, c.tm_mem_base)} at {n}"
                            for n in sorted(c.cases)))

    def partitions_per_subtask():
        """Every case must divide the input evenly across its subtasks. Fewer
        partitions than subtasks leaves some with nothing to read; an uneven
        split makes the busiest subtask set the pace, and either one shows up
        as the largest case sitting below its CPU cap — which is a scaling
        shortfall the table cannot tell apart from a real one."""
        bad = [n for n in c.cases if c.partitions % n]
        if bad:
            raise Exception(f"{c.partitions} partitions do not divide evenly by parallelism {bad}: "
                            f"subtasks would read {c.partitions // max(bad)} or "
                            f"{c.partitions // max(bad) + 1} partitions each")
        return (f"{c.partitions} partitions / parallelism {sorted(c.cases)} = "
                + ", ".join(f"{c.partitions // n} per subtask at {n}" for n in sorted(c.cases)))

    def slots():
        m = max(c.cases)
        L.start_tm(m)
        try:
            o = rest("/overview")
            if o["slots-total"] < m:
                raise Exception(f"slots {o['slots-total']} < parallelism {m}")
            return f"slots-total={o['slots-total']} >= parallelism {m} x 1 job"
        finally:
            L.stop_tm()

    def scoping():
        return f"consumer group = {c.project}-<runid>-c<cores>-<pass>; sink is at-least-once (no txn ids)"

    def bp_endpoint():
        v = rest("/config")["flink-version"]
        return f"Flink {v}: busy/idle/backPressured read from the worker's slf4j reporter; REST path deprecated"

    def trim():
        return "docker run --rm --privileged --pid=host alpine nsenter -t 1 -m -u -n -i -- fstrim -v /var/lib/docker"

    def jar():
        if not os.path.exists(c.jar):
            raise Exception(f"{c.jar} does not exist")
        return f"build {build_hash()}"

    os.makedirs(c.results, exist_ok=True)
    check("every image is native to the host arch", arch)
    check("the JDK the engine needs resolves, pinned", jdk)
    check("the engine can write its state directory", statedir)
    check("the metrics reporter is not duplicated", reporter)
    check("disk budget on the HOST, not the container", disk)
    check("retention on every topic written but never drained", retention)
    check("the generator is deterministic", determinism)
    check("CPU cap mechanism chosen once", capmech)
    check("slots >= parallelism x jobs", slots)
    check("partitions divide evenly by every parallelism", partitions_per_subtask)
    check("worker memory is per subtask, not per container", memory_per_subtask)

    def host_ceiling():
        """No pipeline beats its machine. Measured here so a missed claim can be
        read against what this host does at all (run 24: alu 2->4 = 0.980,
        mem 2->4 = 0.690 -- a memory-touching pipeline could not reach 95%)."""
        h = L.host_scaling(seconds=5.0, cases=sorted(set(c.cases)))
        extra["hostScaling"] = h
        if not h or h.get("error"):
            return f"not measured ({(h or {}).get('error', 'no probe')})"
        parts = []
        for mode in ("alu", "mem"):
            steps = h["ofLinear"].get(mode, {})
            parts.append(mode + " " + ", ".join(f"{k} {v:.0%}" for k, v in steps.items()))
        return "; ".join(parts)
    check("what this host's own cores do", host_ceiling)
    check("backlog covers warm-up, window and headroom", backlog_sizing_hint)
    check("worker, broker and job manager against the VM (reported)", memory_budget)
    check("group / txn-id prefix scoped per run", scoping)
    check("back-pressure counters exist on the endpoint read", bp_endpoint)
    check("the VM trim command is known", trim)
    check("the job jar exists and hashes", jar)
    save_json("preflight.json", {"checks": [{"check": a, "result": b, "detail": d} for a, b, d in rows],
                                 **extra})
    fails = [r for r in rows if r[1] == "FAIL"]
    print(f"\n{len(rows)-len(fails)}/{len(rows)} PASS")
    return 1 if fails else 0


# ------------------------------------------------------------------ tiny proof

def cmd_tinyproof():
    """Two cases on a small backlog, a short checkpoint interval so a window fits,
    ratio bounded, then every guard broken on purpose."""
    c = cfg()
    topic = f"{c.topic_in}-tiny"
    man = L.fill(topic, c.tiny, c.seed + 1, "manifest-tiny.json")
    L._CFG.topic_in = topic  # the case measures the tiny topic
    out = {"build": build_hash(), "records": c.tiny, "cases": [], "at": time.strftime("%Y-%m-%d %H:%M:%S")}
    lo, hi = min(c.cases), max(c.cases)
    T_save = dict(T)
    # the pipeline's own checkpoint interval: at 2 s checkpoints the worker read
    # 83-94% of its cap where 10 s read 100% (harness live test, 2 cores, same
    # 30 s window). Three boundaries, a 2 s reporter so the window holds samples.
    T.update(warmupMinS=20.0, minBoundaries=3, minWindowS=30.0, reporterS=2)
    rc = 0
    try:
        recs = {}
        shape_ref = None
        for cores in (lo, hi):
            log(f"---- tiny case {cores} cores ----")
            try:
                rec, shape_ref = L.run_case(cores, "tiny", "tiny", shape_ref, cores == lo, man,
                                            warmup_max_s=120.0, reporter_s=2)
                recs[cores] = rec
                out["cases"].append(rec)
                log(f"  {cores}c: {rec['recordsPerSec']:,.0f} rec/s, tm {rec['tmCapFrac']:.1%} of cap, "
                    f"vantage {rec['vantageDisagreement']:.2%}")
            except CaseRefused as e:
                out["cases"].append(e.rec)
                log(f"  REFUSED ({e.refusal.scope}): {e.refusal.msg}")
                out["result"] = "FAIL"
                rc = 1
        if rc == 0:
            # GUARD: the suite's disk, projected from the measured shape, before the fill
            try:
                out["disk"] = L.disk_projection(topic, c.tiny, recs[hi])
            except Refusal as e:
                out["disk"] = getattr(e, "detail", None)
                out["result"] = "FAIL"
                log(f"  REFUSED ({e.scope}): {e.msg}")
                rc = 1
        if rc == 0:
            d = out["disk"]
            log(f"  disk: input {d['inputBytesPerRecord']:.0f} B/record x {c.backlog:,} = {d['inputBytes']/1e9:.1f} GB; "
                f"sinks {d['sinkBytesPerInput']:.0f} B/input -> {d['sinkBytesUnbounded']/1e9:.1f} GB, "
                f"retention caps them at {d['sinkRetentionCapBytes']/1e9:.1f} GB; checkpoints {d['checkpointBytes']/1e9:.2f} GB; "
                f"need {d['neededBytes']/1e9:.1f} GB incl. the {d['floorBytes']/1e9:.0f} GB floor, "
                f"{d['hostFreeBytesNow']/1e9:.1f} GB free now + {d['reclaimableBytes']/1e9:.1f} GB the tiny proof gives back "
                f"= {d['hostFreeBytes']/1e9:.1f} GB: FITS")
            warm = (recs[hi].get("warmup") or {}).get("seconds")
            want = L.size_backlog(recs[hi]["recordsPerSec"], hi, c.ckpt_ms / 1000.0,
                                  warmup_max_s=warm)
            out["backlogNeeded"] = want
            out["backlogConfigured"] = c.backlog
            if c.backlog < want:
                out["result"] = "FAIL"
                log(f"  REFUSED (rig): backlog {c.backlog:,} is short of the {want:,} records the "
                    f"{hi}-core case needs at its measured {recs[hi]['recordsPerSec']:,.0f} rec/s "
                    f"(warm-up + window + headroom, x1.5); set backlog.count to at least that")
                rc = 1
            else:
                log(f"  backlog: {c.backlog:,} configured, {want:,} needed at the measured "
                    f"{recs[hi]['recordsPerSec']:,.0f} rec/s")
            ratio = recs[hi]["recordsPerSec"] / recs[lo]["recordsPerSec"]
            ideal = hi / lo
            out["ratio"] = round(ratio, 3)
            bound = (T["tinyRatioLo"] * ideal / 2, T["tinyRatioHi"] * ideal / 2)
            print(f"\ntiny proof {lo} -> {hi} ratio: {ratio:.3f}x (bounds {bound[0]:.2f}-{bound[1]:.2f})")
            if not (bound[0] <= ratio <= bound[1]):
                out["result"] = "FAIL"
                print("STOPPING: ratio outside bounds. Superlinear is a defect report, sublinear at this "
                      "size means the rig is not what you think it is.")
                rc = 1
            else:
                out["result"] = "PASS"
    finally:
        T.clear(); T.update(T_save)
        L._CFG.topic_in = c.raw["topics"]["in"]
    save_json("tinyproof.json", out)
    if rc == 0:
        print("\nguard self-test:")
        rc = cmd_selftest(live=True, topic=topic)
        out["selftest"] = "PASS" if rc == 0 else "FAIL"
        save_json("tinyproof.json", out)
    L.delete_topic(topic)  # the disk budget did not include it
    print("TINY PROOF " + ("PASSED" if rc == 0 else "FAILED"))
    return rc


# ------------------------------------------------------------------------ fill

def cmd_fill():
    c = cfg()
    man = L.fill(c.topic_in, c.backlog, c.seed, "manifest.json")
    print(f"backlog {c.backlog:,} records on {c.topic_in}; manifest results/manifest.json")
    return 0


# ---------------------------------------------------------------- completeness

def cmd_completeness():
    """Drain a small backlog twice — clean, and with the worker killed mid-drain —
    and compare the sinks to the generator manifest with no tolerances."""
    c = cfg()
    topic = f"{c.topic_in}-small"
    cores = c.baseline
    man = L.fill(topic, c.small, c.seed + 2, "manifest-small.json")
    man_path = os.path.join(c.results, "manifest-small.json")
    L._CFG.topic_in = topic
    out = {"build": build_hash(), "records": c.small, "cores": cores, "arms": [],
           "at": time.strftime("%Y-%m-%d %H:%M:%S")}

    def drain(group, kill_at=None):
        jid, killed, killed_at = None, False, None
        try:
            L.recreate_output_topics()
            L.assert_cluster_idle()
            L.delete_group(group)
            L.start_tm(cores)
            L.start_sampler(group)
            jid = L.submit_job(cores, group)
            L.wait_running(jid, cores)
            t0 = time.time()
            while True:
                ticks = L.sampler_tail(4)
                cm = max([t.get("committed", -1) for t in ticks] or [-1])
                if kill_at and not killed and cm >= c.small:
                    # run 5: a kill after the drain has finished proves nothing
                    raise Refusal("rig", f"the drain finished ({cm} committed) before the kill at {kill_at:.0%} could land: "
                                         f"smallCount must span several checkpoint intervals at the baseline rate")
                if kill_at and not killed and cm >= c.small * kill_at:
                    log(f"KILL: committed={cm}, killing the task manager mid-drain")
                    sh(f"docker kill {c.tm}")
                    if L.tm_running():
                        raise Refusal("rig", "docker kill reported success but the container is alive")
                    killed, killed_at = True, cm
                    time.sleep(3)
                    L.start_tm(cores)
                    L.wait_running(jid, cores, timeout=240)
                    log("KILL: job RUNNING again on the replacement worker")
                if cm >= c.small:
                    time.sleep(c.ckpt_s + 2)
                    log(f"drained to the last record in {time.time()-t0:.1f}s" + (" (worker killed mid-drain)" if killed else ""))
                    return {"group": group, "killed": killed, "drainS": round(time.time() - t0, 1),
                            "killedAtCommitted": killed_at}
                if time.time() - t0 > 1800:
                    raise Refusal("rig", f"drain did not finish: committed={cm} of {c.small}")
                time.sleep(0.5)
        finally:
            try:
                L.cancel_job(jid)
            finally:
                L.stop_sampler(); L.stop_tm()

    def verify(label):
        cmd = c.fmt(c.verify_cmd, manifest=man_path, topic=topic)
        r = sh(cmd, check=False, timeout=3600)
        print(r.stdout)
        if r.returncode != 0:
            print(r.stderr[-3000:])
            raise Refusal("rig", f"COMPLETENESS FAILED ({label}): verifier exit {r.returncode}")
        return r.stdout

    try:
        a = drain(f"{c.project}-complete-clean"); a["verify"] = verify("clean drain"); out["arms"].append(a)
        b = drain(f"{c.project}-complete-kill", kill_at=c.kill_frac); b["verify"] = verify("worker killed"); out["arms"].append(b)
        out["result"] = "PASS"
    except Refusal as e:
        out["result"] = "FAIL"; out["error"] = e.msg
        save_json("completeness.json", out)
        print("COMPLETENESS FAILED:", e.msg)
        return 1
    finally:
        L._CFG.topic_in = c.raw["topics"]["in"]
    save_json("completeness.json", out)
    print(f"COMPLETENESS PASSED FOR BUILD {out['build']} (clean drain, and a worker killed at {c.kill_frac:.0%})")
    return 0


# ----------------------------------------------------------------------- suite

def passes_plan(cases, n, baseline=None):
    """Alternating order, then the sentinel: the baseline case once more at the
    very end, so the suite's first and last measurements are the same case. A
    rig that drifts across the suite shows up as baseline spread instead of
    hiding inside the alternation (plan 12: 4c read 688k-776k across a suite
    and 801k ten minutes later)."""
    plan = []
    for i in range(n):
        asc = (i % 2 == 0)
        plan.append((f"p{i+1}-{'asc' if asc else 'desc'}", list(cases) if asc else list(reversed(cases))))
    if baseline is not None:
        plan.append(("sentinel", [baseline]))
    return plan


def cmd_suite():
    c = cfg()
    man = load_json("manifest.json")
    comp = load_json("completeness.json")
    bh = build_hash()
    # GUARD: no table for a build that has not passed completeness
    if comp.get("result") != "PASS" or comp.get("build") != bh:
        raise Refusal("rig", f"completeness has not passed for build {bh} "
                             f"(completeness.json: {comp.get('result')} for {comp.get('build')})")
    tp = load_json("tinyproof.json") if os.path.exists(os.path.join(c.results, "tinyproof.json")) else {}
    if tp.get("result") != "PASS" or tp.get("selftest") != "PASS":
        raise Refusal("rig", "the tiny proof (with guard self-test) has not passed; run `prove.py tinyproof`")
    plan = passes_plan(c.cases, c.passes, c.baseline)
    out = {"axis": c.axis, "apiLevel": c.api_level, "guarantee": c.guarantee,
           "checkpointIntervalMs": c.ckpt_ms, "buildHash": bh, "completenessBuild": comp.get("build"),
           "passesPerCase": c.passes, "quickLook": L.QUICK, "publishable": not L.QUICK,
           "cases": c.cases, "baseline": c.baseline,
           "backlogRecords": int(man[c.count_field]), "partitions": c.partitions,
           "outputsPerInput": c.out_per_in,
           "heldStill": {"kafkaCap": c.kafka_cap, "jobManagerCap": c.jm_cap, "partitions": c.partitions,
                         "checkpointMs": c.ckpt_ms, "sinkRetentionBytesPerPartition": T["sinkRetentionBytes"],
                         "tmProcessMemory": c.tm_mem},
           "thresholds": dict(T), "harness": harness_version(),
           "startedAt": time.strftime("%Y-%m-%d %H:%M:%S %Z"), "runs": [], "refusals": []}

    def save():
        out["savedAt"] = time.strftime("%Y-%m-%d %H:%M:%S %Z")
        out["table"] = build_table(out["runs"], quick=out.get("quickLook", False))
        save_json("suite.json", out)

    shape_ref, stop = None, None
    run_id = time.strftime("%m%d%H%M")
    for pass_id, order in plan:
        for cores in order:
            log(f"---- case {cores} cores, pass {pass_id} ----")
            try:
                rec, shape_ref = L.run_case(cores, pass_id, run_id, shape_ref, cores == c.baseline, man)
                out["runs"].append(rec)
                log(f"  {cores}c {pass_id}: {rec['recordsPerSec']:,.0f} rec/s  tm {rec['tmCores']:.2f}/{cores} "
                    f"({rec['tmCapFrac']:.1%})  kafka {rec['kafkaCores']:.2f}/{c.kafka_cap:g}  "
                    f"srcIdle {rec['sourceIdle']:.1%}  srcBP {rec['sourceBackpressured']:.1%}  "
                    f"headroom {rec['headroomS']:.0f}s  vantage {rec['vantageDisagreement']:.2%}")
            except CaseRefused as e:
                out["runs"].append(e.rec)
                kind = "CEILING" if e.refusal.scope == "ceiling" else "REFUSED"
                out.setdefault("ceilings" if kind == "CEILING" else "refusals", []).append(
                    {"case": cores, "pass": pass_id, "scope": e.refusal.scope, "message": e.refusal.msg})
                log(f"  {kind}: {e.refusal.msg}")
                if e.refusal.scope == "rig":
                    stop = ("rig refusal", e.refusal.msg)
            save()
            if stop:
                break
        if stop:
            break
    if stop:
        out["stoppedEarly"] = {"reason": stop[0], "message": stop[1],
                               "note": "stopping here: the remaining cases would fail the same way"}
    save()
    cmd_report()
    print(render_table(out))
    return 1 if stop else 0


def harness_version():
    r = sh(f"shasum -a 256 {L.HERE}/lib.py {L.HERE}/prove.py", check=False)
    return {"lib.py": r.stdout.split()[0][:16] if r.stdout else None,
            "prove.py": r.stdout.split()[2][:16] if len(r.stdout.split()) > 2 else None}


# --------------------------------------------------------------------- ceiling

def cmd_ceiling():
    """Hold the component under test at its largest size; cap the broker in
    steps. The handover is where the broker pins and the worker falls off its cap."""
    c = cfg()
    man = load_json("manifest.json")
    top = max(c.cases)
    steps = [float(x) for x in c.raw.get("ceilingBrokerCaps", [c.kafka_cap, 1.0, 0.5])]
    out = {"cores": top, "steps": [], "buildHash": build_hash()}
    run_id = time.strftime("%m%d%H%M")
    try:
        for cap in steps:
            sh(f"docker update --cpus {cap} {c.kafka}")
            L.assert_cap(c.kafka, cap)
            log(f"---- ceiling: broker capped at {cap} cores, worker at {top} ----")
            try:
                rec, _ = L.run_case(top, f"k{cap:g}", run_id, None, False, man, kafka_cap=cap)
                rec["brokerCap"] = cap
                rec["brokerCapFrac"] = rec["kafkaCapFrac"]
                out["steps"].append(rec)
                log(f"  broker {rec['kafkaCores']:.2f}/{cap:g} ({rec['brokerCapFrac']:.0%})  worker {rec['tmCapFrac']:.1%}  "
                    f"{rec['recordsPerSec']:,.0f} rec/s  srcIdle {rec['sourceIdle']:.1%}")
            except CaseRefused as e:
                e.rec["brokerCap"] = cap
                if "kafkaCapFrac" in e.rec:
                    e.rec["brokerCapFrac"] = e.rec["kafkaCapFrac"]
                out["steps"].append(e.rec)
                log(f"  at broker cap {cap:g}: {e.refusal.msg}")
            save_json("ceiling.json", out)
    finally:
        sh(f"docker update --cpus {c.kafka_cap} {c.kafka}", check=False)
        L.assert_cap(c.kafka, c.kafka_cap)
    return 0


# ---------------------------------------------------------------------- report

def cmd_report():
    out = load_json("suite.json")
    out["table"] = build_table(out["runs"], quick=out.get("quickLook", False))
    c = cfg()
    short = [r for r in out["table"]["stepRatios"] if r.get("meetsClaim") is False]
    with open(os.path.join(c.results, "suite.txt"), "w") as f:
        f.write(render_table(out) + "\n")
    with open(os.path.join(c.results, "suite.md"), "w") as f:
        f.write(render_markdown(out))
    save_json("suite.json", out)
    print("wrote results/suite.txt and results/suite.md")
    if short:
        t = out["table"]
        for r in short:
            print(f"CLAIM NOT MET: {r['step']} returned {r['ratio']:.3f}x of an ideal {r['idealRatio']:.0f}x "
                  f"— {r['efficiency']:.1%} of linear, floor {T['scalingFloor']:.0%}. The table stands; "
                  f"the pipeline did not scale on this rig.")
            a, b = t["cases"].get(r["from"]), t["cases"].get(r["to"])
            if a and b:
                pa = a["meanRecordsPerSec"] / r["from"]
                pb = b["meanRecordsPerSec"] / r["to"]
                print(f"  per core        {pa:>12,.0f} -> {pb:>12,.0f}   ({pb / pa - 1:+.1%})")
                for k, label in (("tmCapFrac", "% of cap"), ("sourceIdle", "source idle"),
                                 ("gcFracOfCapacity", "GC"), ("sourceBackpressured", "back-pressure")):
                    if a.get(k) is not None and b.get(k) is not None:
                        print(f"  {label:<15} {a[k]:>11.1%} -> {b[k]:>11.1%}")
        try:
            pf = load_json("preflight.json")
            h = (pf.get("hostScaling") if isinstance(pf, dict) else None) or {}
            for mode, label in (("alu", "register-only"), ("mem", "memory-bound")):
                steps = (h.get("ofLinear") or {}).get(mode) or {}
                if steps:
                    print(f"  this host, {label:<13} " + "  ".join(f"{k} {v:.0%}" for k, v in steps.items()))
            if h:
                print("  A pipeline cannot beat its machine: judge the shortfall against those two bounds.")
        except Exception:
            pass
        print("  What this rig has shown: worker memory that does not scale per subtask costs about 14%;"
              "\n  a broker starved of page cache costs about 13%; four subtasks instead of two costs about"
              "\n  8% on the same cores, of which ~3 points is the source idling. Partition count and network"
              "\n  buffers were tested and changed nothing. See harness/README.md.")
        return 1
    return 0


# ------------------------------------------------------------------------ all

def cmd_all(steps=None, results=None):
    """The whole chain as one command. Run 11 spent 20 minutes of its 1.97 h in
    the gaps between commands an agent typed by hand, and wrote phases.log by
    hand; here the harness writes it, and DONE is the file to wait on.
    `results` is where phases.log, all.json and DONE go — the self-test passes
    its own directory so a fake chain never lands in the live one."""
    c = cfg()
    steps = steps or [("up", COMMANDS["up"]), ("preflight", cmd_preflight), ("completeness", cmd_completeness),
                      ("tinyproof", cmd_tinyproof), ("fill", cmd_fill), ("suite", cmd_suite), ("report", cmd_report)]
    results = results or c.results
    os.makedirs(results, exist_ok=True)
    phases = os.path.join(results, "phases.log")
    done = os.path.join(results, "DONE")
    if os.path.exists(done):
        os.remove(done)
    out = {"build": build_hash() if os.path.exists(c.jar) else None, "steps": []}
    quick0 = L.QUICK   # GUARD: a phase that leaves the flag different from how it
                       # found it mis-stamps every table after it (2026-09-05: the
                       # tiny proof's self-test cleared it and the suite voided
                       # itself as "1 pass < 2" while still running one pass).

    def mark(line):
        with open(phases, "a") as f:
            f.write(time.strftime("%Y-%m-%d %H:%M:%S ") + line + "\n")
        if results == c.results:  # the self-test's fake chain stays out of harness.log too (run 12)
            log(line)

    def save_all():
        with open(os.path.join(results, "all.json"), "w") as f:
            json.dump(out, f, indent=2, default=str)

    t_all = time.time()
    mark("phase=all start")
    verdict = "PASS"
    for name, fn in steps:
        t0 = time.time()
        mark(f"phase={name} start")
        try:
            rc = fn()
        except Refusal as e:
            log(f"REFUSED ({e.scope}): {e.msg}")
            rc = 1
        finally:
            if name in ("completeness", "tinyproof", "suite"):
                try:
                    L.stop_sampler(); L.stop_tm()
                except Exception:
                    pass
        if L.QUICK != quick0:
            log(f"phase {name} left the quick flag {L.QUICK} (it was {quick0}); restoring")
            L.QUICK = quick0
            rc = rc or 1
        if name == "report" and rc:
            log("the chain measured a valid table whose step ratios do not meet the claim")
        out["steps"].append({"step": name, "rc": rc, "seconds": round(time.time() - t0, 1)})
        mark(f"phase={name} end rc={rc} {time.time() - t0:.0f}s")
        save_all()
        if rc:
            verdict = f"FAIL at {name}"
            break
    out["verdict"] = verdict
    out["seconds"] = round(time.time() - t_all, 1)
    save_all()
    mark(f"phase=all end {verdict} {out['seconds']/60:.1f} min")
    with open(done, "w") as f:
        f.write(f"{verdict} {out['seconds']/60:.1f} min\n")
    return 0 if verdict == "PASS" else 1


# ---------------------------------------------------------------------- main

COMMANDS = {
    "replay": lambda: cmd_replay(),
    "selftest": lambda: cmd_selftest(live=True),
    "selftest-pure": lambda: cmd_selftest(live=False),
    "up": lambda: (L.stack_up(), 0)[1],
    "preflight": cmd_preflight,
    "tinyproof": cmd_tinyproof,
    "fill": cmd_fill,
    "completeness": cmd_completeness,
    "suite": cmd_suite,
    "ceiling": cmd_ceiling,
    "report": cmd_report,
    "down": lambda: (L.stack_down(), 0)[1],
    "all": cmd_all,
}

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        print(__doc__); sys.exit(2)
    name = sys.argv[1]
    if "--quick" in sys.argv[2:]:
        L.QUICK = True
        print(f"QUICK LOOK: {L.T['quickPasses']} passes per case; the table it writes is marked unpublishable")
    if name not in ("replay",):
        cfg()  # validate pipeline.json first
    if name not in ("replay", "selftest-pure", "report"):
        rc = cmd_replay()
        if rc:
            print("refusing to run with a threshold that disagrees with the record")
            sys.exit(rc)
    try:
        rc = COMMANDS[name]()
    except Refusal as e:
        print(f"REFUSED ({e.scope}): {e.msg}")
        rc = 1
    except KeyboardInterrupt:
        rc = 130
    finally:
        if name in ("suite", "tinyproof", "completeness", "ceiling", "selftest", "all"):
            try:
                L.stop_sampler(); L.stop_tm()
            except Exception:
                pass
    sys.exit(rc)
