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
### Task 2.1 - Schema Evolution Drill

- Added a drift file containing 1,000 real flight rows plus a new `feed_version` column.
- First pipeline update stopped after detecting the schema change.
- Second pipeline update succeeded using the evolved schema.
- `feed_version` appeared in `bronze_flights`.
- Historical rows have NULL for `feed_version`; drift rows contain `v2`.
- `_rescued_data` was checked for unexpected parse/schema issues.
### Bronze rescued data finding

Auto Loader populated `_rescued_data` with an unnamed trailing CSV field
`_c109`. The value is empty for all rows, while `_file_path` records the
originating file. Bronze preserves this raw structure as delivered.
The empty trailing field is excluded during Silver conformance.

## Silver Data Quality Rules

| Rule | Behaviour | Threshold / Expression | Reason | Rows affected |
|---|---|---|---|---:|
| has_source_file | fail | `_source_file IS NOT NULL` | Every row must retain lineage | 0 |
| period_matches_file | drop | `period = file_period` | Monthly file must contain its own period | 0 |
| flags_present | drop | cancelled/diverted not NULL | Required for operational rates | 0 |
| origin_ne_dest | drop | `origin <> dest` | Origin and destination should differ | 0 |
| distance_positive | drop | `distance_mi > 0` | Non-positive distance is invalid | 0 |
| crs_time_valid | drop | scheduled time 0000–2400 | Schedule must be parseable | 0 |
| cancel_code_when_cancelled | warn | cancelled flight has cancellation code | Needed for cancellation analysis | <your result> |
| arr_delay_when_completed | warn | completed flight has arrival delay | Completed flights should have arrival timing | <your result> |

### Silver Reconciliation

- Bronze rows: 1,067,492
- Silver clean rows: 1,067,492
- Quarantined rows: 0
- Clean + quarantine = Bronze: PASS
- De-duplicated Silver rows: 1,066,492
- Duplicates removed: 1,000
### expect_or_fail proof

A temporary development expectation `distance_mi < 0` was added to
`silver_flights_clean`.

A normal incremental pipeline update did not fail because the streaming table
had no new flight records to process, so the new expectation was not evaluated
against historical rows.

A full refresh reset the streaming state and reprocessed the source data.
The temporary expectation then failed on real records, proving that
`expect_or_fail` stops the update when violating rows are processed.

The temporary rule was removed and the pipeline was successfully refreshed again.

## Gold Object Design

| Object | Type | Reason |
|---|---|---|
| gold_otp_carrier_month | Materialized View | Reusable carrier-month KPI aggregation for BI queries |
| gold_delay_causes | Materialized View | Precomputes expensive yearly cause aggregations |
| gold_airport_hour | Materialized View | Supports repeated airport/hour performance analysis |
| gold_weather_impact | Materialized View | Persists flight-weather join and daily aggregation |
| gold_aircraft_rotation | Materialized View | Reusable governed aircraft-level operational view |
| gold_disruptions | Materialized View | Maintains a filtered operational exception dataset |
| gold_tail_delays | Materialized View | Precomputes aircraft monthly reliability metrics |

## Task 2.3 - Gold Layer

All seven Gold materialized views refreshed successfully in dev.

| Object | Rows |
|---|---:|
| gold_aircraft_rotation | 273,310 |
| gold_otp_carrier_month | 30 |
| gold_delay_causes | 15 |
| gold_airport_hour | 3,779 |
| gold_weather_impact | 19,395 |
| gold_disruptions | 37,659 |
| gold_tail_delays | 11,197 |

All Gold objects are built from Silver-layer objects or reference mappings,
not from other Gold objects.

- `gold_otp_carrier_month`: materialized because carrier-month KPIs are reused repeatedly in BI.
- `gold_delay_causes`: materialized because cause aggregations are expensive and repeatedly queried.
- `gold_airport_hour`: materialized for repeated airport/hour operational analysis.
- `gold_weather_impact`: materialized because it persists a flight-weather join and daily aggregation.
- `gold_aircraft_rotation`: materialized because it provides a governed reusable aircraft-level operational dataset.
- `gold_disruptions`: materialized because it provides a reusable filtered exception dataset.
- `gold_tail_delays`: materialized because it precomputes aircraft reliability metrics.