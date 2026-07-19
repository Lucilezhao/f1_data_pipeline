# Formula 1 Data Pipeline — Azure Databricks + Delta Lake

An end-to-end ELT pipeline built on **medallion architecture** (bronze / silver / gold), processing Formula 1 race data into analytics-ready dimensional models.

Built to practise production-grade data engineering patterns — idempotent loads, schema enforcement, layered transformation, and incremental processing — the same decisions faced at any scale.

## Architecture

```
Raw files (CSV / JSON)
      │
      ▼
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   BRONZE    │ ──▶ │   SILVER    │ ──▶ │    GOLD     │
│  raw, as-is │     │  cleaned &  │     │ dimensional │
│  + batch_id │     │ conformed   │     │   models    │
└─────────────┘     └─────────────┘     └─────────────┘
```

| Layer | Purpose | Load pattern |
|---|---|---|
| **Bronze** | Raw data landed as-is, partitioned by `batch_id` | Idempotent overwrite via `replaceWhere` |
| **Silver** | Cleaned, standardised, deduplicated, business-key validated | Idempotent `MERGE` (upsert) |
| **Gold** | Dimensional models (facts & dimensions) for analytics | Full overwrite per run |

## Tech Stack

- **Azure Databricks** (Unity Catalog enabled)
- **PySpark** for all transformations
- **Delta Lake** — ACID transactions, schema enforcement, time travel
- **ADLS Gen2** for storage
- **Git** for version control

## Key Design Decisions

### 1. Idempotent bronze loads — `batch_id` + `replaceWhere`

Every ingestion run stamps rows with a `batch_id` and writes with:

```python
(df.write
   .mode("overwrite")
   .format("delta")
   .partitionBy("batch_id")
   .option("replaceWhere", f"batch_id = '{v_batch_id}'")
   .saveAsTable("formula1.bronze.results"))
```

**Why:** re-running the same batch replaces only that batch's data — no duplicates, no clobbering of other batches. A new `batch_id` appends; a re-run of an old one overwrites in place. This makes the pipeline safe to re-run after failures or bug fixes, which is the baseline requirement for any production pipeline.

### 2. MERGE in silver vs full overwrite in gold

- **Silver** uses `MERGE` keyed on business keys — incoming batches upsert against existing data, so late-arriving corrections update rather than duplicate.
- **Gold** is rebuilt with a full overwrite each run — gold tables are small aggregated/dimensional outputs derived entirely from silver, so recomputing them is cheap and guarantees consistency. Incremental logic here would add complexity without benefit at this scale.

**Trade-off acknowledged:** at much larger scale, gold would move to incremental materialisation; the layering makes that change local to gold without touching upstream.

### 3. Schema enforcement and validation in silver

Silver applies, per source table:

- Explicit column renaming to `snake_case` (e.g. `constructorId → constructor_id`, `positionText → finish_position_text`)
- Business-key null validation (e.g. rows dropped where `season`, `round`, `constructor_id`, or `driver_id` is null)
- Deduplication (`dropDuplicates`, keyed where a business key exists)
- Type and semantic renames for clarity (`grid → grid_position`, `laps → completed_laps`)
- Nested struct flattening (e.g. `concat_ws` on `name.givenName` / `name.familyName` → `driver_name`)

**Why:** raw sources are inconsistent (mixed casing, nulls in keys, nested structures). Fixing this once in silver means every downstream consumer inherits clean, consistent data — the "conform once, use everywhere" principle.

### 4. Delta Lake over plain Parquet

Chosen for: ACID guarantees on concurrent writes, `MERGE` support (plain Parquet has none), schema enforcement on write, and time travel for debugging bad loads.

## Gold Layer Models

Dimensional models joining conformed silver tables, e.g.:

```python
dim_races = (races
    .join(circuits, races.circuit_id == circuits.circuit_id, "inner")
    .select(races.season, races.round, races.race_name, races.race_date,
            circuits.circuit_name, circuits.locality, circuits.country))
```

## Repository Structure

```
├── 00_common/           # Shared config (catalog/schema names, environment)
├── 0X_ingest_*.py       # Bronze ingestion notebooks (per source)
├── 0X_transform_*.py    # Silver transformation notebooks (per table)
├── 0X_gold_*.py         # Gold dimensional model builds
└── README.md
```

Notebooks follow a shared template: config → read → transform → validate → write, so adding a new source table is a copy-and-adapt exercise.

## Roadmap

- [ ] Automated ingestion from the public Ergast API (replacing manual file drops)
- [ ] Data quality test suite (dbt-style tests: not-null, uniqueness, referential checks)
- [ ] CI with GitHub Actions (lint + unit tests on PR)
- [ ] dbt + Snowflake port of the transformation layer

## Running

1. Import notebooks into a Databricks workspace (Unity Catalog enabled)
2. Update `00_common/01_environment_config` with your catalog / schema / storage paths
3. Run ingestion → transform → gold notebooks in order, or orchestrate via Databricks Workflows
