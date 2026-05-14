# Data Quality Checks in Python

A focused, dependency-light data quality validation suite built with Pandas. Runs a battery of column-level checks against a target dataset and reports what passed, what failed, and where the defects are.

This is the first published module in a larger ETL validation toolkit I'm building incrementally as I deepen my Python skills. Patterns reflect ETL/BI test work I've done on enterprise programs for U.S. clients including the World Bank, TIAA. 

## What this checks

Each function is one self-contained data quality check. The suite covers the validation categories every ETL/BI test program needs:

| Category | Check | What it catches |
|---|---|---|
| **Integrity** | `check_uniqueness` | Duplicate primary keys |
| **Integrity** | `check_null_count` | NULLs in required columns |
| **Referential** | `check_allowed_values` | Enum / lookup-list violations |
| **Business rules** | `check_numeric_range` | Values outside acceptable bounds |
| **Business rules** | `check_date_range` | Future dates, ancient dates, sentinel values |
| **Format** | `check_regex_pattern` | Code-format violations (currencies, tickers, IDs) |
| **Conditional logic** | `check_conditional_rule` | Cross-column business rules (e.g. *"if settled, settlement date must not be null"*) |

## Quick start

```
python scripts/00_generate_sample_data.py
python scripts/02_data_quality_checks.py
```

The first script generates a synthetic treasury-trades dataset (100K rows) with **deliberately injected defects** so the validation script has something realistic to catch. The second script runs the data quality suite against that dataset.

## Sample output

```
======================================================================
DATA QUALITY SUITE
======================================================================

Check                               Result   Detail
----------------------------------------------------------------------
uniqueness:trade_id                 FAIL     20 duplicates
null_count:trade_id                 PASS     0 nulls
null_count:currency_code            FAIL     50 nulls
null_count:notional_amount          PASS     0 nulls
allowed_values:trade_status         FAIL     30 invalid ['UNKNOWN']
allowed_values:instrument_type      PASS     0 invalid []
numeric_range:notional_amount       PASS     0 out of range
date_range:trade_date               FAIL     5 out of range (max: 2099-12-31)
regex_pattern:currency_code         PASS     0 invalid pattern
SETTLED trades must have trade_dat  FAIL     3/84,915 violations

SUMMARY: 5/10 checks passed
```

Each failure corresponds to a real defect injected into the synthetic data — confirming the checks behave as designed.

## Repo structure

```
.
├── README.md
├── scripts/
│   ├── 00_generate_sample_data.py   # Synthetic data + injected defects
│   └── 02_data_quality_checks.py    # The DQ suite
└── sample_data/
    ├── source_trades.csv             # Clean baseline
    └── target_trades.csv             # Post-ETL with injected defects
```

## Design notes

**Each check returns a structured dict.** Easy to log, easy to fail a CI/CD pipeline on, easy to post to a Slack webhook or dashboard. No print-and-pray.

**Tolerance and limits are parameters, not hardcoded.** `max_nulls`, `min_val`, `max_val`, allowed-value lists — everything tuneable per check.

**Conditional rules are generic.** The `check_conditional_rule` function takes two Pandas Series — one for the condition, one for the requirement — so any cross-column business rule can be expressed without changing the check engine.

**NaN-aware.** Null handling is explicit at every step; no silent surprises from `NaN != NaN` comparisons.

## Status & roadmap

This repo currently focuses on column-level data quality validation. I'm extending it incrementally as I deepen my Python and modern-data-stack skills. Planned additions:

- Source-to-target reconciliation (row counts, key presence, aggregate checksums, value-level diffs)
- Schema drift detection against JSON baselines
- Memory-efficient chunked validation for 5M+ row datasets using streaming statistics
- BI dashboard reconciliation against database ground truth
- A Great Expectations port of the checks
- A pytest harness so each check fails CI on data quality regression
- Snowflake / BigQuery connector layer (currently uses CSV fixtures)

Feedback and pull requests welcome.

## Author

Built by **Sridevi Rajkumar**, Senior QA Engineer specializing in ETL and BI testing. Currently in Calgary, AB and open to data quality, QA leadership, and Business Systems Analyst roles in Canada / remote across North America.

🔗 [LinkedIn](https://www.linkedin.com/in/sridevi-rajkumar/)
