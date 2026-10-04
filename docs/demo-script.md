# Client Demo — US Airline Operations Intelligence

**Story in one line:** we built a production-style Databricks platform on ~53M US airline flights (2018–2025) and
NOAA weather. It processes the data, and it also tells an operations team *who* is performing, *why* flights are
delayed, *where and when* to intervene, and *why the numbers can be trusted*.

Dashboard file: [`src/dashboards/airline_ops_client.lvdash.json`](../src/dashboards/airline_ops_client.lvdash.json)
(6 pages, reads `workspace.prd_gold` and `workspace.prd_ops`).

---

## 1. Import the dashboard (5 minutes, do this the day before)

1. Databricks sidebar → **Dashboards** → arrow next to **Create dashboard** → **Import dashboard from file**.
2. Pick `airline_ops_client.lvdash.json`. It opens in **Draft** mode.
3. At the top, choose the warehouse **Serverless Starter Warehouse**.
4. Click through all 6 pages and check that every widget renders. If one shows an error, open the **Data** tab,
   run that dataset, and send me the error text.
5. Click **Publish** → keep **Embed credentials** on (viewers then query as you) → **Publish**.
6. Open the published view, which is the one you present from. Draft mode shows edit handles.

Optional polish in the editor (2 min): make the **Airline Reliability** line chart thicker, and on
**Delay Root Cause** sort the carrier bar by total value.

> Want to add an interactive Year filter? Most datasets already have a `year` column. In the editor, click
> **Add a filter** → field `year` → select the datasets to link.

---

## 2. Pre-demo checklist (30 minutes before)

| Check | How |
|---|---|
| Warehouse is warm | Open the dashboard once. The first query wakes the serverless warehouse (≈10–20 s) |
| Latest release is certified | Page 1 → **Latest certified data release** shows a recent timestamp |
| A new file is held back for the live drop | See §4. Without a new month, the trigger does nothing |
| Tabs pre-opened | ① Dashboard (published) ② Jobs → `a2b_airline_ops_build_prd` ③ Catalog → `prd_gold.gold_aircraft_rotation` ④ GitHub → Actions |
| Terminal ready | `databricks auth` works and the `drop_files.py` command is typed out |
| Write down your real numbers | Fill the `[...]` placeholders below from the dashboard, so you quote facts and do not improvise |

---

## 3. Talk track — 10 minutes

Use the same three-step pattern for every chart: **Observation → Business implication → Possible action.**
Don't claim cost savings or revenue impact. The dataset has no cost or revenue fields. Use phrases like
*"supports prioritisation"*, *"replaces manual analysis"*, *"enables operational decisions"*.

### 0:00 – 0:30 · Start the live file drop first

The file-arrival trigger waits for the upload to settle (≈2 min) and then runs the full pipeline. Start it now so
the run is finished by minute 5:

```bash
python tools/drop_files.py --env prd --match <held-back-file>.zip
```

> "I've just delivered a new month of flight data into the landing zone, the same way the upstream system would.
> We'll come back in a few minutes to see what the platform did with it automatically."

### 0:30 – 1:30 · Page 1 — Executive Overview

> "Airline operations teams usually ask three questions: who is performing well, why are flights delayed, and
> where should we intervene? This dashboard answers all three."

> "Across 2018–2025 we processed **52.97 million** scheduled flights. **51.7 million** operated, with an
> on-time arrival rate of **80.8%** and a cancellation rate of **2.08%**. That's the network baseline everything
> else is measured against."

Point at the monthly trend: *"You can see the 2020 disruption, and how the network looked once traffic recovered."*
Point at **Certified releases**: *"And this card shows the numbers come from a validated, certified release.
I'll show you what that means in a moment."*

### 1:30 – 3:00 · Page 2 — Airline Reliability

- **Observation:** "[Carrier A] is consistently above [x]% every year. [Carrier B] dropped from [x]% to [y]%."
- **Implication:** "So this is a structural reliability gap, not one bad month."
- **Action:** "A network or partnership manager can drill into that carrier's routes, turnaround times and
  schedule design, or use it in codeshare and partner discussions."

Ranking table plus the Δ bar: *"In the latest year, these carriers improved the most and these slipped. That's
where management attention should go first."*

### 3:00 – 4:00 · Page 3 — Delay Root Cause

- **Observation:** "[Late-arriving aircraft] accounts for [x]% of all delay minutes, and carrier-caused delay for
  another [y]%. Extreme weather is only [z]%."
- **Implication:** "Most delay is operational and propagates through the day. Investing in weather tooling
  alone won't fix it."
- **Action:** "Look at aircraft rotation, turnaround buffers and recovering from upstream delays. The carrier
  chart shows which airlines have the biggest controllable share."

### 4:00 – 5:00 · Page 4 — Airport & Time Bottlenecks

- **Observation:** "Network delay climbs from about [x] min in the early morning to [y] min in the evening.
  At [airport], the worst hour is [hh:00] at [x] min vs [y] min at its best hour."
- **Implication:** "Delay is time-of-day driven. Morning flights leave clean, and evening flights inherit the
  day's problems."
- **Action:** "Schedule planners can add peak-hour buffers, adjust ground staffing, or move banks of departures.
  The peak-gap table ranks where that matters most."

### 5:00 – 5:45 · Page 5 — Weather Impact

- **Observation:** "On snow days, cancellations run at **[x]×** the dry-day rate, and departures are [y] min later
  on average. Heavy rain raises it [z]×."
- **Implication:** "We can now *quantify* weather risk per airport rather than guess."
- **Action:** "Weather contingency planning, pre-emptive schedule thinning, and passenger re-accommodation
  staffing at the most sensitive airports (heatmap)."

Caveat to say out loud: *"Weather is joined for the 10 airports where we mapped a NOAA station, so this is a
representative sample, not every airport."*

### 5:45 – 7:00 · Page 6 — Data Trust, then back to the live run

> "Why should you trust these numbers? Every monthly file is reconciled: rows expected from the source must equal
> Bronze, and Bronze must equal clean Silver plus quarantine. Then a quality gate checks mismatches, the
> quarantine rate (≤2%) and duplicates. Only then is a release certified, and only certified data reaches
> this dashboard."

Then switch to the **Jobs** tab → the run triggered at 0:00:

```text
file arrival → prepare → has_new → pipeline (Bronze → Silver → Gold) → reconcile (per month)
→ quality_check → gate → certify → release_job (table-update trigger) → release_status refreshed
```

> "Nobody pressed a button. The file arrived, and the platform ingested, validated, reconciled, certified and
> published it."

Back to the dashboard → refresh → **Latest certified release** now shows the new timestamp and period.
This is the strongest moment of the demo.

If the gate fails, that's also a good story: *"Look, the gate blocked it and raised an incident instead of
publishing bad data."* (`raise_incident` branch → **Incidents raised** counter.)

Architecture, if they ask:

```text
BTS + NOAA + OurAirports → Landing Volume → Bronze (Auto Loader) → Silver (clean / quarantine, de-dup)
→ Reconciliation + quality gate → Gold (materialized views) → AI/BI Dashboard
```

### 7:00 – 9:00 · Governance (Unity Catalog)

Catalog Explorer → `prd_gold.gold_aircraft_rotation`:

1. **Lineage tab:** follow the path from the raw files through Bronze and Silver to this Gold table and the
   dashboard. *"Every number is traceable to its source file."*
2. **Row filter + column mask:** the table's definition carries `rf_scope` and `mask_text`. Live demo (always
   revert it afterwards):

   ```sql
   -- Pretend I'm a partner airline analyst: only Delta rows, no tail numbers
   UPDATE workspace.prd_ops.entitlements
   SET scope_value = 'DL', can_see_sensitive = false
   WHERE user_email = session_user();

   SELECT carrier_code, tail_number, legs FROM workspace.prd_gold.gold_aircraft_rotation LIMIT 10;

   -- Restore full access
   UPDATE workspace.prd_ops.entitlements
   SET scope_value = '*', can_see_sensitive = true
   WHERE user_email = session_user();
   ```

> "Different partner airlines can share one platform. Each sees only the rows they're entitled to, and
> sensitive identifiers like tail numbers are masked. That's enforced in the catalog, not in each report."

### 9:00 – 10:00 · Delivery (CI/CD)

GitHub → Actions → the latest `bundle-ci` run:

```text
Pull request → validate (dev + prod) → merge → deploy dev → deploy prod → run setup job
```

> "Pipelines, jobs, triggers and this dashboard's data layer are all code in Git and deployed through Databricks
> Asset Bundles. A change goes through review and validation before it reaches production."

**Close:**

> "So this isn't an analytics notebook. It's an automated, governed, reconciled data product, from ingestion all the
> way to the business decision."

---

## 4. Live file-drop: make sure it will actually run

`prepare` skips any file already in `prd_ops.file_manifest`, so you need a month that has **not** been loaded yet:

```sql
SELECT period FROM workspace.prd_ops.file_manifest ORDER BY period DESC LIMIT 5;
```

- If a month is still missing, use it as `<held-back-file>` (e.g. `_2025_12.zip`).
- If all 96 months are loaded, re-dropping an existing file is a no-op (`has_new = false`). You can present that
  as idempotency (*"re-delivering the same file never double-counts"*). For the full end-to-end story, show the
  most recent successful run's graph in the Jobs UI instead.
- Timing: the trigger fires after up to ≈2–7 minutes (`wait_after_last_change 120s`,
  `min_time_between_triggers 300s`), then the pipeline runs. That's why you drop the file at 0:00.

---

## 5. Likely client questions

| Question | Answer |
|---|---|
| How fresh is the data? | Event-driven: a new BTS month is processed as soon as it lands. NOAA weather is polled daily at 05:30. |
| What if the source sends a bad file? | Rows that break a rule go to quarantine. If quarantine exceeds 2%, counts mismatch, or duplicates appear, the gate blocks the release and raises an incident. The dashboard keeps showing the last certified data. |
| What if the source adds a column? | Auto Loader schema evolution, which we tested with a drift drill (`feed_version` column). |
| Can it show cost impact? | Not from this data, which has no cost or revenue fields. If you provide cost-per-delay-minute, we can add it as a Gold measure. |
| Can each airline see only its own data? | Yes. Unity Catalog row filters and column masks are defined on the Gold object itself. |
| How is it deployed? | Databricks Asset Bundles plus GitHub Actions: PR → validate → dev → prod. |

## 6. Caveats to state honestly

- On-time = arrival delay < 15 min among **operated** flights (the BTS definition).
- Cause minutes are as reported by carriers to BTS. They only cover flights delayed ≥15 min.
- Airport/hour delay averages are weighted by scheduled departures (a close approximation).
- Weather covers the 10 airports with mapped NOAA stations. "Snow" = any snowfall recorded that day.
