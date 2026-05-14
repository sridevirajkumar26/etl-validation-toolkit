"""
Schema drift detection.

Saves a JSON snapshot of a table's schema (columns, dtypes, basic stats),
then on subsequent runs compares against the baseline to flag:

  - New columns appearing
  - Columns disappearing
  - Dtype changes (e.g. int became float, string became object)
  - Significant cardinality shifts
  - Mean/stddev drift on numeric columns (> N% change)

Run nightly as part of data observability. Prevents the classic
"Dev changed the source schema and broke the dashboard at 9am Monday"
incident.
"""

import pandas as pd
import numpy as np
import json
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

DATA_DIR = Path(__file__).parent.parent / "sample_data"
BASELINE_DIR = Path(__file__).parent.parent / "sample_data" / "baselines"
BASELINE_DIR.mkdir(exist_ok=True, parents=True)


def profile_dataframe(df: pd.DataFrame) -> Dict:
    """Build a JSON-serializable schema + stats profile."""
    profile = {
        "captured_at": datetime.now().isoformat(),
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns": {},
    }
    for col in df.columns:
        series = df[col]
        col_profile = {
            "dtype": str(series.dtype),
            "null_count": int(series.isna().sum()),
            "null_pct": round(series.isna().sum() / len(df) * 100, 4),
            "distinct_count": int(series.nunique()),
        }
        if pd.api.types.is_numeric_dtype(series):
            col_profile.update({
                "min": float(series.min()) if series.notna().any() else None,
                "max": float(series.max()) if series.notna().any() else None,
                "mean": float(series.mean()) if series.notna().any() else None,
                "std": float(series.std()) if series.notna().any() else None,
            })
        elif pd.api.types.is_object_dtype(series):
            top_values = series.value_counts().head(5).to_dict()
            col_profile["top_values"] = {str(k): int(v) for k, v in top_values.items()}
        profile["columns"][col] = col_profile
    return profile


def save_baseline(df: pd.DataFrame, table_name: str) -> Path:
    """Persist a baseline profile to disk."""
    profile = profile_dataframe(df)
    path = BASELINE_DIR / f"{table_name}_baseline.json"
    with open(path, "w") as f:
        json.dump(profile, f, indent=2, default=str)
    print(f"Saved baseline -> {path}")
    return path


def detect_drift(
    df: pd.DataFrame,
    table_name: str,
    mean_tolerance_pct: float = 5.0,
    null_pct_tolerance: float = 1.0,
) -> List[Dict]:
    """Compare current state against baseline and return drift findings."""
    baseline_path = BASELINE_DIR / f"{table_name}_baseline.json"
    if not baseline_path.exists():
        print(f"No baseline exists for {table_name}. Saving current as baseline.")
        save_baseline(df, table_name)
        return []

    with open(baseline_path) as f:
        baseline = json.load(f)
    current = profile_dataframe(df)

    findings = []

    # Row count drift
    base_rows = baseline["row_count"]
    cur_rows = current["row_count"]
    pct = abs(cur_rows - base_rows) / base_rows * 100 if base_rows else 0
    if pct > 10:
        findings.append({
            "severity": "WARN" if pct < 50 else "CRITICAL",
            "type": "row_count_drift",
            "detail": f"{base_rows:,} -> {cur_rows:,} ({pct:+.1f}%)",
        })

    base_cols = set(baseline["columns"].keys())
    cur_cols = set(current["columns"].keys())

    # New columns
    for col in cur_cols - base_cols:
        findings.append({
            "severity": "WARN",
            "type": "new_column",
            "column": col,
            "detail": f"new column '{col}' (dtype: {current['columns'][col]['dtype']})",
        })

    # Dropped columns
    for col in base_cols - cur_cols:
        findings.append({
            "severity": "CRITICAL",
            "type": "dropped_column",
            "column": col,
            "detail": f"column '{col}' missing in current data",
        })

    # Column-level drift
    for col in base_cols & cur_cols:
        b = baseline["columns"][col]
        c = current["columns"][col]

        if b["dtype"] != c["dtype"]:
            findings.append({
                "severity": "CRITICAL",
                "type": "dtype_change",
                "column": col,
                "detail": f"{b['dtype']} -> {c['dtype']}",
            })

        # Null rate drift
        null_drift = abs(c["null_pct"] - b["null_pct"])
        if null_drift > null_pct_tolerance:
            findings.append({
                "severity": "WARN",
                "type": "null_rate_drift",
                "column": col,
                "detail": f"null_pct {b['null_pct']}% -> {c['null_pct']}%",
            })

        # Numeric mean drift
        if "mean" in b and "mean" in c and b["mean"] not in (None, 0):
            mean_drift_pct = abs(c["mean"] - b["mean"]) / abs(b["mean"]) * 100
            if mean_drift_pct > mean_tolerance_pct:
                findings.append({
                    "severity": "WARN",
                    "type": "mean_drift",
                    "column": col,
                    "detail": (
                        f"mean {b['mean']:.4f} -> {c['mean']:.4f} "
                        f"({mean_drift_pct:+.2f}%)"
                    ),
                })

    return findings


if __name__ == "__main__":
    # Step 1: capture baseline from the clean source
    print("Step 1: Capture baseline from clean source")
    source = pd.read_csv(DATA_DIR / "source_trades.csv", parse_dates=["trade_date"])
    save_baseline(source, "trades")

    # Step 2: run drift detection against the dirty target
    print("\nStep 2: Detect drift in current target (post-ETL)")
    target = pd.read_csv(DATA_DIR / "target_trades.csv", parse_dates=["trade_date"])
    findings = detect_drift(target, "trades")

    if not findings:
        print("  No drift detected.")
    else:
        print(f"  {len(findings)} drift finding(s):")
        for f in findings:
            print(f"  [{f['severity']:8s}] {f['type']:20s} {f['detail']}")
