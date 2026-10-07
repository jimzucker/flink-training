# Plan

1. Build job/ (EnrichJob, OrdersGenerator, EnrichVerify, enrich.sql).
2. Laptop: `prove.py up`, `preflight`, `completeness`, `tinyproof` with pipeline.json. Chrome closed.
3. Cloud: `prove.py all` with pipeline-cloud.json — local first, up, preflight, completeness, tiny
   proof, fill, suite at 5, 10 and 20 CFU, report — within the $350 budget the harness checks.
