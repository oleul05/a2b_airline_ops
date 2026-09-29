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