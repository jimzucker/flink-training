# Step 00 — Scaffold: review

## Round 1

### Asked

1. **Flink 1.20 vs 2.x** — does the AWS-compatibility argument hold for how you
   plan to demo, or is AWS going to be self-managed on EC2/EKS where 2.x is fine?
2. **`groupId`** — keep `io.github.jimzucker.flinktraining`, or something else?
3. **Should `.claude/settings.json` be tracked?**
4. **Scala variant** — confirm nothing downstream needs the `scala_2.12` image.

Plus flagged: `main` unpushed, and no CI.

### Feedback

> 1 flink 1.2 / 2 ok / 3 dont track / 4 ok / push changes / add CI to plan

### Actions

| Feedback | Action |
|---|---|
| Flink 1.20 confirmed | No change — `flink.version` stays 1.20.4 |
| `groupId` ok | No change |
| Don't track `.claude/` | Added `.claude/` to `.gitignore` |
| Scala variant ok | No change — scala-free `flink:1.20.4-java17` stays |
| Push changes | `main` and `step-00-scaffold` pushed to origin |
| Add CI to plan | New **step 03 — CI**; steps 03–10 shifted to 04–11 |

### Outcome

Approved. Squash-merged to `main`, tagged `step-00`.
