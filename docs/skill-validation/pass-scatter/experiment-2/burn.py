"""Load that imitates macOS background services: about 80% of one core, at
background priority (run under `taskpolicy -b`), switched on during odd-numbered
suite cases and off during even ones. Stops when the suite writes its report,
or after 70 minutes. Logs each switch with a timestamp."""
import sys, time, re
log, out = sys.argv[1], sys.argv[2]
deadline, state, last = time.time() + 70 * 60, None, 0
with open(out, "a") as o:
    while time.time() < deadline:
        now = time.time()
        if now - last > 1:
            last = now
            try:
                text = open(log).read()
            except FileNotFoundError:
                text = ""
            if "wrote results/suite.txt" in text:
                o.write(f"{now:.0f} done\n"); break
            n = text.count("---- case ")
            want = (n % 2 == 1)
            if want != state:
                state = want
                o.write(f"{now:.0f} case {n} load {'on' if want else 'off'}\n"); o.flush()
        if state:
            end = time.time() + 0.08
            while time.time() < end:
                pass
            time.sleep(0.02)
        else:
            time.sleep(0.1)
