# Post — senior managers

Draft. Links to [skill-article.md](skill-article.md). Figures from
`docs/skill-validation/demo-under-harness.md` and the runs table in
`docs/skill-validation/README.md`.

---

Every vendor says their system scales. Very few can show it in a way that survives one skeptical question.

That matters, because capacity decisions — how many servers to buy, what a platform will cost at twice the load — often rest on a single test run or a dashboard screenshot.

Last week I shared six rules for supervising AI. The sixth was: make important rules enforceable. This is what that looks like.

I built a Claude skill that builds a data pipeline and then proves whether it scales. It measures each step up in capacity, and it refuses to publish a number when the test itself was flawed — the machine wasn't really the limit, the runs disagreed, or data was lost along the way.

The first thing it did was report a result I didn't want. The pipelines Claude built ran two to three times faster than my hand-written version. Mine scaled closer to linearly. The tool reported both, instead of picking the flattering one.

It's free and open. Details for engineers in the article: [link]

---

**First comment:** "The skill, the harness and all 29 validation runs are public: https://github.com/jimzucker/flink-training"
