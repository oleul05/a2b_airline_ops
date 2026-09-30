## Environment Findings

| Capability | Result | Notes |
|---|---|---|
| BTS TranStats outbound access | PASS | HTTP 200 |
| OurAirports outbound access | PASS | HTTP 200 |
| NOAA CDO API v2 | REACHABLE | HTTP 400 from smoke test; host reachable |
| NOAA Access Data Service | REACHABLE / TEMPORARILY UNAVAILABLE | HTTP 503 during smoke test |
| PyPI outbound access | PASS | HTTP 200 |
| session_user() | PASS | Returned my workspace user |
| Secret scopes | NONE YET | Will create scope `a2` in Task 1.4 |
| Row filters | PASS | Returned only row `A` as expected |
| Governed tags | AVAILABLE | Create governed tag dialog is available |
| Invite teammate/user | AVAILABLE | Successfully added and removed a test user |
| Service principal | AVAILABLE | Add service principal dialog is available |
| BTS local cache download | IN PROGRESS | `python3 tools/drop_files.py --download-only` |

## Ingestion Decisions

| Source | Method | Why |
|---|---|---|
| BTS monthly flight files | Auto Loader | Monthly incremental files arrive continuously and need schema evolution support |
| BTS lookup tables | COPY INTO | Small reference files; simple and safely re-runnable |
| OurAirports reference | COPY INTO | Small batch reference dataset |
| NOAA weather | REST API to Volume | Source is exposed through a REST API and raw JSON should be retained |

## Task 1.4 - NOAA REST Ingestion

- NOAA API token stored in Databricks secret scope `a2`
- `ref_airport_station` created with 10 airport-to-GHCND mappings
- Weather data fetched for 2018-2025
- 80 raw JSON files landed in `/Volumes/workspace/dev_landing/raw/weather/`
- Secret redaction verified in notebook output