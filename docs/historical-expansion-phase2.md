# Phase 2 — Historical archive ingestion (staging only)

Related: #194. Phase 1 audit PR #195 remains **unverified** until a real database snapshot is audited.

## Contract

The importer stores raw CFBD game records in **db/historical_staging.db**, completely separate from production `db/league.db`. It retains original school names, scores, ties, source JSON, neutral-site unknowns, classification unknowns, source IDs, and SHA-256 payload fingerprints. **No modern FBS eligibility is inferred.** The staging DB must never be used directly by production Elo or Merits.

The importer fetches both regular and postseason for each requested year, uses API authorization from `CFBD_API_KEY`, retries transient HTTP failures, and checkpoints each successful season/season type. Re-running skips completed checkpoints; `--force` refetches. Failed imports do not mark a season complete.

## Commands

```bash
python -m pytest tests/test_historical_archive_import.py -q
CFBD_API_KEY=... python scripts/import_historical_archive.py --start 1946 --end 1947
CFBD_API_KEY=... python scripts/import_historical_archive.py --start 1946 --end 1979
```

Do not put API keys in logs, issues, or source code. The first command requires no API key.

## Data-quality gates before promoting any games

1. Phase 1 production baseline approved and archived.
2. All requested fetch checkpoints complete, with source totals reconciled to staging rows and known empty seasons documented.
3. Resolve historical team identity and era-specific major-college eligibility **in separate reviewed tables**; no silent defaults.
4. Confirm neutral site, dates, ties, and incomplete results are represented without fabricated values.
5. Document all CFBD historical coverage limitations and compare season totals with an independent reference when available.
6. Run an explicit staging-to-production promotion PR with before/after model diff; **this importer does not promote data**.

## Operational caveats

- CFBD may limit API access, request volume, or data availability; earliest advertised season is not proof of completeness.
- Empty responses are recorded as completed fetches, **not** proof that no games were played.
- A forced refresh updates matching source IDs but does not delete records that disappeared upstream; investigate differences before promotion.
- A network failure halts the job and records the failed checkpoint, so the next invocation can resume.
- Never use `--db db/league.db`; the CLI explicitly refuses it.
