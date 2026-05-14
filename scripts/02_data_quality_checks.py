"""
Column-level data quality checks on a single dataset.

This is the kind of check you'd run on the target warehouse after ETL
or on a daily monitoring schedule. Catches issues that source-to-target
reconciliation misses, like:

  - NULLs where they shouldn't be
  - Duplicate primary keys
  - Values outside allowed enum lists
  - Numeric values outside business-rule ranges
  - Date sanity (no future dates, no impossibly old dates)
  - String pattern violations (regex)

Each check returns a structured result so you can fail a CI/CD pipeline,
post to a dashboard, or email stakeholders.
"""

import pandas as pd
import numpy as np
import re
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime

DATA_DIR = Path(__file__).parent.parent / "sample_data"


def check_null_count(
    df: pd.DataFrame, col: str, max_nulls: int = 0
) -> Dict:
    """Hard limit on NULLs in a column. Default is zero tolerance."""
    null_count = df[col].isna().sum()
    return {
        "check": "null_count",
        "column": col,
        "null_count": int(null_count),
        "null_pct": round(null_count / len(df) * 100, 4),
        "max_allowed": max_nulls,
        "passed": null_count <= max_nulls,
    }


def check_uniqueness(df: pd.DataFrame, col: str) -> Dict:
    """Primary key uniqueness check."""
    dup_count = df[col].duplicated().sum()
    dup_values = df.loc[df[col].duplicated(), col].unique()[:5].tolist()
    return {
        "check": "uniqueness",
        "column": col,
        "duplicate_count": int(dup_count),
        "sample_duplicates": dup_values,
        "passed": dup_count == 0,
    }


def check_allowed_values(
    df: pd.DataFrame, col: str, allowed: List
) -> Dict:
    """Referential integrity / enum check."""
    invalid_mask = ~df[col].isin(allowed) & df[col].notna()
    invalid_count = invalid_mask.sum()
    invalid_samples = df.loc[invalid_mask, col].unique()[:5].tolist()
    return {
        "check": "allowed_values",
        "column": col,
        "invalid_count": int(invalid_count),
        "invalid_values_sample": invalid_samples,
        "allowed_values": allowed,
        "passed": invalid_count == 0,
    }


def check_numeric_range(
    df: pd.DataFrame,
    col: str,
    min_val: Optional[float] = None,
    max_val: Optional[float] = None,
) -> Dict:
    """Business-rule range check on a numeric column."""
    violations = pd.Series([False] * len(df))
    if min_val is not None:
        violations |= df[col] < min_val
    if max_val is not None:
        violations |= df[col] > max_val
    return {
        "check": "numeric_range",
        "column": col,
        "min_allowed": min_val,
        "max_allowed": max_val,
        "violation_count": int(violations.sum()),
        "actual_min": float(df[col].min()),
        "actual_max": float(df[col].max()),
        "passed": violations.sum() == 0,
    }


def check_date_range(
    df: pd.DataFrame,
    col: str,
    min_date: Optional[str] = None,
    max_date: Optional[str] = None,
) -> Dict:
    """Date sanity check — no future dates, no ancient dates."""
    dates = pd.to_datetime(df[col], errors="coerce")
    violations = pd.Series([False] * len(df))
    if min_date:
        violations |= dates < pd.Timestamp(min_date)
    if max_date:
        violations |= dates > pd.Timestamp(max_date)
    return {
        "check": "date_range",
        "column": col,
        "min_allowed": min_date,
        "max_allowed": max_date,
        "violation_count": int(violations.sum()),
        "actual_min": str(dates.min()),
        "actual_max": str(dates.max()),
        "passed": violations.sum() == 0,
    }


def check_regex_pattern(
    df: pd.DataFrame, col: str, pattern: str
) -> Dict:
    """Pattern check — useful for codes like currency (3-letter), tickers, IDs."""
    regex = re.compile(pattern)
    non_null = df[col].dropna()
    match_mask = non_null.astype(str).str.match(regex)
    invalid_count = (~match_mask).sum()
    return {
        "check": "regex_pattern",
        "column": col,
        "pattern": pattern,
        "invalid_count": int(invalid_count),
        "passed": invalid_count == 0,
    }


def check_conditional_rule(
    df: pd.DataFrame,
    rule_name: str,
    condition: pd.Series,
    requirement: pd.Series,
) -> Dict:
    """
    Generic conditional business-rule check.
    Example: 'If trade_status == SETTLED, then settlement_date IS NOT NULL'.

    condition = df['trade_status'] == 'SETTLED'
    requirement = df['settlement_date'].notna()
    """
    relevant = condition.sum()
    violations = (condition & ~requirement).sum()
    return {
        "check": "conditional_rule",
        "rule_name": rule_name,
        "rows_evaluated": int(relevant),
        "violation_count": int(violations),
        "passed": violations == 0,
    }


def run_dq_suite(target_path: Path) -> List[Dict]:
    """The full data quality test suite for the target_trades dataset."""
    print(f"Loading {target_path.name}...")
    df = pd.read_csv(target_path, parse_dates=["trade_date"])
    print(f"  Loaded {len(df):,} rows\n")

    results = []

    print("=" * 70)
    print("DATA QUALITY SUITE")
    print("=" * 70)

    checks = [
        # Primary key checks
        check_uniqueness(df, "trade_id"),
        check_null_count(df, "trade_id", max_nulls=0),

        # Required-not-null fields
        check_null_count(df, "currency_code", max_nulls=0),
        check_null_count(df, "notional_amount", max_nulls=0),

        # Enum / allowed values
        check_allowed_values(
            df, "trade_status", ["SETTLED", "PENDING", "CANCELLED"]
        ),
        check_allowed_values(
            df, "instrument_type",
            ["BOND", "EQUITY", "FX_SWAP", "IR_SWAP", "LOAN"]
        ),

        # Range checks
        check_numeric_range(df, "notional_amount", min_val=0, max_val=1e10),
        check_date_range(
            df, "trade_date",
            min_date="2020-01-01",
            max_date=datetime.now().strftime("%Y-%m-%d"),
        ),

        # Pattern checks
        check_regex_pattern(df, "currency_code", r"^[A-Z]{3}$"),
    ]

    # Conditional rule: SETTLED trades shouldn't have future dates
    today = pd.Timestamp.now().normalize()
    checks.append(
        check_conditional_rule(
            df,
            rule_name="SETTLED trades must have trade_date <= today",
            condition=df["trade_status"] == "SETTLED",
            requirement=df["trade_date"] <= today,
        )
    )

    # Print results
    print(f"\n{'Check':<35} {'Result':<8} {'Detail'}")
    print("-" * 70)
    for r in checks:
        flag = "PASS" if r["passed"] else "FAIL"
        if r["check"] == "null_count":
            detail = f"{r['null_count']:,} nulls"
        elif r["check"] == "uniqueness":
            detail = f"{r['duplicate_count']:,} duplicates"
        elif r["check"] == "allowed_values":
            detail = f"{r['invalid_count']:,} invalid {r['invalid_values_sample']}"
        elif r["check"] == "numeric_range":
            detail = f"{r['violation_count']:,} out of range"
        elif r["check"] == "date_range":
            detail = f"{r['violation_count']:,} out of range (max: {r['actual_max'][:10]})"
        elif r["check"] == "regex_pattern":
            detail = f"{r['invalid_count']:,} invalid pattern"
        elif r["check"] == "conditional_rule":
            detail = f"{r['violation_count']:,}/{r['rows_evaluated']:,} violations"
        else:
            detail = ""
        label = r.get("rule_name") or f"{r['check']}:{r.get('column', '')}"
        print(f"{label[:34]:<35} {flag:<8} {detail}")
        results.append(r)

    failed = [r for r in results if not r["passed"]]
    print(f"\n{'='*70}")
    print(f"SUMMARY: {len(results) - len(failed)}/{len(results)} checks passed")
    print(f"{'='*70}")
    return results


if __name__ == "__main__":
    run_dq_suite(DATA_DIR / "target_trades.csv")
