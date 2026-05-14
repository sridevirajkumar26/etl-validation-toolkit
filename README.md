# ETL Validation Toolkit

A production-style Python toolkit for validating ETL pipelines and BI reports at scale. Built on Pandas and NumPy, designed to run on millions of records, and structured the way a senior data QA engineer actually works in financial services.

## What this demonstrates

This is the codebase behind the resume claim: *"Developed Python automation scripts to validate large-volume datasets (5M+ records) across database tables and BI reports — reduced manual validation effort by ~40%, cutting regression cycle time from 3 days to under 1 day."*

Specifically, it shows:

- **Source-to-target reconciliation** with row count, key presence, aggregate checksum, and column-level value comparison
- **Column-level data quality** checks: nulls, uniqueness, allowed values, numeric ranges, date sanity, regex patterns, conditional business rules
- **Schema drift detection** with baseline snapshots and configurable tolerance thresholds
- **Memory-efficient chunked validation** using Welford's online algorithm for streaming statistics — built to handle 5M+ rows on a laptop
- **BI dashboard reconciliation** against database ground truth, catching the classic "dashboard says X but database says Y" defect
- **Stakeholder HTML reports** with release-readiness verdicts that a CFO can read in 90 seconds

## Repo structure

```
etl-validation-toolkit/
├── scripts/
│   ├── 00_generate_sample_data.py            # Synthetic data with injected defects
│   ├── 01_source_to_target_reconciliation.py # Row count, keys, aggregates, value diffs
│   ├── 02_data_quality_checks.py             # Nulls, dupes, ranges, business rules
│   ├── 03_schema_drift_detection.py          # Daily monitoring vs baseline
│   ├── 04_chunked_validation_for_large_data.py # 5M+ row streaming validation
│   ├── 05_bi_dashboard_reconciliation.py     # BI export vs DB ground truth
│   └── 06_html_stakeholder_report.py         # Release-readiness HTML report
├── sample_data/                              # Synthetic CSV fixtures
├── reports/                                  # Generated HTML reports
└── README.md
```

## Quick start

```bash
pip install pandas numpy
python scripts/00_generate_sample_data.py
python scripts/01_source_to_target_reconciliation.py
python scripts/02_data_quality_checks.py
python scripts/03_schema_drift_detection.py
python scripts/04_chunked_validation_for_large_data.py
python scripts/05_bi_dashboard_reconciliation.py
python scripts/06_html_stakeholder_report.py
open reports/reconciliation_report.html
```

## Sample dataset

A synthetic treasury-trades dataset (100K rows by default, configurable to 5M+) with **intentionally injected defects** so each validation script has something realistic to catch:

| Defect type | Count | Caught by |
|---|---|---|
| Dropped rows | 12 | Script 01 (row count) |
| NULL currency codes | 50 | Script 02 (nulls), 06 (report) |
| Notional precision loss | 100 | Script 01 (column-level), 02 |
| Duplicate trade IDs | 20 | Script 02, 04, 06 |
| Invalid status enum values | 30 | Script 02, 04, 06 |
| Future-dated trades | 5 | Script 02, 04, 06 |
| Inflated dashboard KPI | 1 slice | Script 05 |

## Engineering notes

**Scaling to 5M+ rows.** Script 04 uses `pd.read_csv(chunksize=...)` to stream the file, and Welford's online algorithm for single-pass mean/variance computation. On commodity hardware this processes ~300K rows/sec — a 5M row reconcile finishes in ~15 seconds. Memory footprint stays bounded regardless of file size.

**NaN-safe comparisons.** Script 01's column-level diff treats `NaN == NaN` as equal, which is what a business user expects even though Pandas defaults to `NaN != NaN`. This eliminates a class of false-positive defects.

**Configurable tolerance.** Aggregate reconciliations accept a percentage tolerance (default 0.01% on sums, zero on counts). This matches how finance teams actually sign off on reports — exact match on counts, rounding-tolerant match on dollars.

**Stakeholder output.** Script 06 produces an HTML report with color-coded pass/fail pills, a release-readiness verdict, and a "next steps" section. Designed to be attached to a release sign-off email and read in 90 seconds.

## What this is not

- Not a replacement for [Great Expectations](https://greatexpectations.io/), [Soda Core](https://www.soda.io/core), or [dbt tests](https://docs.getdbt.com/docs/build/data-tests) — those are excellent and you should use them for production. This toolkit is a teaching artifact and a starting point.
- Not connected to a real warehouse — sample data is synthetic. In production, the `pd.read_csv` calls become `snowflake.connector` or `google.cloud.bigquery` calls.
- Not a UI framework — outputs are CLI text and a static HTML report. CI/CD integration (failing a GitHub Actions job on FAIL) is a 10-line addition.

## Extensions worth building

If you fork this, consider:

- Wire the checks into pytest so each becomes a test case that CI can fail on
- Replace synthetic data with a real Snowflake connection via `snowflake-snowpark-python`
- Port the checks to a Great Expectations suite for direct comparison
- Add a Slack webhook for failure alerts
- Build a daily scheduled run via GitHub Actions or Airflow

## Author

Built by Sridevi Rajkumar, Senior QA Engineer specializing in ETL and BI testing. Calgary, AB.

[LinkedIn](https://www.linkedin.com/in/sridevi-rajkumar/)
