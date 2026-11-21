# Shopping Sessions (Builders Buildathon, Databricks track)

Goal: implement `rules.md` exactly in `pipeline/pipeline.py` (DuckDB starter). Score = container run time; any wrong row scores zero. Correctness first, then speed.

- `rules.md` is the spec and wins over the starter/README if they disagree.
- Output: `/output/user_daily/` and `/output/campaign_daily/` as Parquet, exact columns (see pipeline.py), `day` as DATE.
- Limits: linux/amd64, 8 vCPU, 24 GB RAM, no network at run time, read-only root (only /tmp 64 MB), uid 1000, /scratch for spills.
- Target: under ~60 s on 20M-event practice data (final is ~60M events, 3-min hard cap).
- Watch out: DuckDB's CSV reader doesn't match the rules' dialect (`""` vs null, malformed lines); `\d` must mean ASCII digits; ID linking can have long chains (prefer union-find over recursive SQL).

Commands (Windows: run in WSL or Git Bash with Docker):
- No Docker: `pip install duckdb==1.5.6 polars` then `python3 scripts/run_fixtures.py fixtures -- python3 pipeline/pipeline.py`
- Docker: `make fixtures`, `make dev` (needs `generate` from `generator/`), `make run-dev`, `make check-dev`

Layout: `dev-answer/` = expected dev output, `generator/` = Rust data generator, `_other-starters/` = Polars/PySpark starters for reference. These are gitignored.
Deadline: Sun Nov 22, 11 AM ET. 2 submissions/hour.
