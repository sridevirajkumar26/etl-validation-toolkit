"""
BI report vs. database reconciliation.

The "dashboard says X, but the database says Y" defect is the most common
and most painful BI defect to catch. This script validates an exported
dashboard CSV (Power BI / Tableau export) against the underlying database
aggregates.

Designed around a real test pattern:
  1. QA exports the dashboard data to CSV (or uses the Power BI REST API)
  2. QA runs the same aggregation query against the database
  3. This script reconciles the two with configurable tolerance
  4. Failures get logged at the KPI level so business users can sign off
     on what's working and triage what isn't.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import List, Dict

DATA_DIR = Path(__file__).parent.parent / "sample_data"


def simulate_dashboard_export(source_df: pd.DataFrame) -> pd.DataFrame:
    """
    Simulate what a Power BI/Tableau dashboard would export:
    aggregated KPIs by currency and instrument type.

    Injects a couple of intentional discrepancies (one rounding-induced,
    one calculation bug) so the reconciliation script catches them.
    """
    dashboard = (
        source_df.groupby(["currency_code", "instrument_type"], dropna=False)
        .agg(
            trade_count=("trade_id", "count"),
            total_notional=("notional_amount", "sum"),
            avg_notional=("notional_amount", "mean"),
        )
        .reset_index()
    )
    # Round totals to whole dollars (dashboard rounds for display)
    dashboard["total_notional"] = dashboard["total_notional"].round(0)
    dashboard["avg_notional"] = dashboard["avg_notional"].round(2)

    # INTENTIONAL DEFECT: inflate USD/BOND total by 0.5%
    # (simulates a calculation bug in the dashboard's DAX/calc field)
    mask = (dashboard["currency_code"] == "USD") & (dashboard["instrument_type"] == "BOND")
    dashboard.loc[mask, "total_notional"] *= 1.005

    return dashboard


def compute_db_kpis(source_df: pd.DataFrame) -> pd.DataFrame:
    """Ground-truth KPI calculation from the database (the QA's SQL equivalent)."""
    db = (
        source_df.groupby(["currency_code", "instrument_type"], dropna=False)
        .agg(
            db_trade_count=("trade_id", "count"),
            db_total_notional=("notional_amount", "sum"),
            db_avg_notional=("notional_amount", "mean"),
        )
        .reset_index()
    )
    return db


def reconcile_bi_to_db(
    dashboard: pd.DataFrame,
    db: pd.DataFrame,
    key_cols: List[str],
    tolerance_pct: float = 0.001,
) -> Dict:
    """Compare dashboard KPIs to database ground truth."""
    merged = dashboard.merge(db, on=key_cols, how="outer", indicator=True)

    findings = []

    # Missing slices
    only_dashboard = merged[merged["_merge"] == "left_only"]
    only_db = merged[merged["_merge"] == "right_only"]
    if len(only_dashboard):
        findings.append({
            "severity": "WARN",
            "type": "slice_only_in_dashboard",
            "count": len(only_dashboard),
            "detail": only_dashboard[key_cols].to_dict(orient="records")[:5],
        })
    if len(only_db):
        findings.append({
            "severity": "WARN",
            "type": "slice_missing_from_dashboard",
            "count": len(only_db),
            "detail": only_db[key_cols].to_dict(orient="records")[:5],
        })

    matched = merged[merged["_merge"] == "both"].copy()

    # KPI-by-KPI reconciliation
    kpi_pairs = [
        ("trade_count", "db_trade_count", 0),  # zero tolerance on counts
        ("total_notional", "db_total_notional", tolerance_pct),
        ("avg_notional", "db_avg_notional", tolerance_pct),
    ]

    for bi_col, db_col, tol in kpi_pairs:
        matched["abs_diff"] = (matched[bi_col] - matched[db_col]).abs()
        matched["pct_diff"] = np.where(
            matched[db_col] != 0,
            matched["abs_diff"] / matched[db_col].abs(),
            0,
        )
        violations = matched[matched["pct_diff"] > tol]
        if len(violations):
            findings.append({
                "severity": "CRITICAL",
                "type": f"kpi_mismatch:{bi_col}",
                "count": len(violations),
                "tolerance": tol,
                "max_pct_diff": round(float(violations["pct_diff"].max()), 6),
                "sample": violations[
                    key_cols + [bi_col, db_col, "pct_diff"]
                ].head(5).to_dict(orient="records"),
            })

    return {
        "dashboard_rows": len(dashboard),
        "db_rows": len(db),
        "matched_rows": len(matched),
        "findings": findings,
        "passed": len([f for f in findings if f["severity"] == "CRITICAL"]) == 0,
    }


if __name__ == "__main__":
    print("Loading source data and simulating dashboard export...")
    source = pd.read_csv(DATA_DIR / "source_trades.csv", parse_dates=["trade_date"])
    dashboard = simulate_dashboard_export(source)
    db = compute_db_kpis(source)

    dashboard.to_csv(DATA_DIR / "dashboard_export.csv", index=False)
    print(f"  Dashboard slices: {len(dashboard)}")
    print(f"  DB slices:        {len(db)}")

    print("\nReconciling dashboard KPIs against database...")
    result = reconcile_bi_to_db(
        dashboard, db,
        key_cols=["currency_code", "instrument_type"],
        tolerance_pct=0.001,  # 0.1% tolerance
    )

    print(f"\n=== BI -> DB RECONCILIATION RESULT ===")
    print(f"  Matched slices: {result['matched_rows']}")
    print(f"  Total findings: {len(result['findings'])}")
    for f in result["findings"]:
        print(f"\n  [{f['severity']:8s}] {f['type']}")
        print(f"    Count: {f['count']}")
        if "max_pct_diff" in f:
            print(f"    Max % diff: {f['max_pct_diff']:.4%}")
        if "sample" in f:
            for row in f["sample"]:
                print(f"    {row}")

    print(f"\nOverall: {'PASS' if result['passed'] else 'FAIL'}")
