"""
Stakeholder-friendly HTML reconciliation report.

The QA-team output most teams skip and most business users actually want.
Combines all previous validation results into a single email-ready HTML
report with color-coded pass/fail, mismatch samples, and a release-readiness
verdict.

This is the artifact that turns "we ran some tests" into
"here's a one-page sign-off document the CFO can read in 90 seconds."
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from html import escape

DATA_DIR = Path(__file__).parent.parent / "sample_data"
REPORT_DIR = Path(__file__).parent.parent / "reports"
REPORT_DIR.mkdir(exist_ok=True)


CSS = """
<style>
  body { font-family: -apple-system, Segoe UI, Roboto, sans-serif;
         max-width: 980px; margin: 32px auto; padding: 0 24px; color: #1a1a1a; }
  h1 { font-size: 24px; margin-bottom: 4px; }
  .subtitle { color: #666; font-size: 14px; margin-bottom: 24px; }
  .verdict { padding: 16px; border-radius: 6px; font-size: 18px;
             font-weight: 600; margin: 20px 0; }
  .verdict.pass { background: #e6f4ea; color: #1e8449; border: 1px solid #b7dfb9; }
  .verdict.fail { background: #fdecea; color: #c0392b; border: 1px solid #f5c6cb; }
  .summary-grid { display: grid; grid-template-columns: repeat(4, 1fr);
                  gap: 12px; margin: 20px 0; }
  .stat-card { background: #f8f9fa; padding: 14px; border-radius: 6px;
               border-left: 4px solid #4a90e2; }
  .stat-card .label { font-size: 12px; color: #666; text-transform: uppercase; }
  .stat-card .value { font-size: 20px; font-weight: 600; margin-top: 4px; }
  h2 { font-size: 17px; margin-top: 32px; padding-bottom: 8px;
       border-bottom: 1px solid #eee; }
  table { width: 100%; border-collapse: collapse; margin: 12px 0; font-size: 14px; }
  th { text-align: left; background: #f5f5f5; padding: 8px 10px;
       border-bottom: 2px solid #ddd; }
  td { padding: 8px 10px; border-bottom: 1px solid #eee; }
  tr.pass td:last-child { color: #1e8449; font-weight: 600; }
  tr.fail td:last-child { color: #c0392b; font-weight: 600; }
  .pill { display: inline-block; padding: 2px 10px; border-radius: 12px;
          font-size: 12px; font-weight: 600; }
  .pill.pass { background: #e6f4ea; color: #1e8449; }
  .pill.fail { background: #fdecea; color: #c0392b; }
  .pill.warn { background: #fff3cd; color: #856404; }
  .small { font-size: 12px; color: #777; }
  .mono { font-family: SF Mono, Consolas, monospace; font-size: 12px; }
</style>
"""


def build_html_report(
    source_path: Path,
    target_path: Path,
    output_path: Path,
) -> Path:
    """Generate the stakeholder HTML report."""
    source = pd.read_csv(source_path, parse_dates=["trade_date"])
    target = pd.read_csv(target_path, parse_dates=["trade_date"])

    # Collect findings
    findings = []

    # 1. Row count
    src_n, tgt_n = len(source), len(target)
    findings.append({
        "category": "Row Counts",
        "check": "Source vs target row count",
        "detail": f"Source: {src_n:,} | Target: {tgt_n:,} | Diff: {tgt_n - src_n:+,}",
        "passed": src_n == tgt_n,
    })

    # 2. Key presence
    missing = set(source["trade_id"]) - set(target["trade_id"])
    extra = set(target["trade_id"]) - set(source["trade_id"])
    findings.append({
        "category": "Row Counts",
        "check": "Missing trade_ids in target",
        "detail": f"{len(missing):,} missing",
        "passed": len(missing) == 0,
    })

    # 3. Duplicates
    dup_count = target["trade_id"].duplicated().sum()
    findings.append({
        "category": "Data Integrity",
        "check": "Duplicate primary keys",
        "detail": f"{dup_count:,} duplicate trade_ids",
        "passed": dup_count == 0,
    })

    # 4. Nulls
    for col in ["currency_code", "notional_amount", "trade_status"]:
        nulls = target[col].isna().sum()
        findings.append({
            "category": "Data Integrity",
            "check": f"NULL values in {col}",
            "detail": f"{nulls:,} nulls ({nulls/len(target)*100:.4f}%)",
            "passed": nulls == 0,
        })

    # 5. Allowed values
    invalid_status = (~target["trade_status"].isin(
        ["SETTLED", "PENDING", "CANCELLED"]) & target["trade_status"].notna()).sum()
    findings.append({
        "category": "Business Rules",
        "check": "Allowed values: trade_status",
        "detail": f"{invalid_status:,} invalid status values",
        "passed": invalid_status == 0,
    })

    # 6. Future dates
    future = (pd.to_datetime(target["trade_date"]) > pd.Timestamp.now()).sum()
    findings.append({
        "category": "Business Rules",
        "check": "No future-dated trades",
        "detail": f"{future:,} future-dated rows",
        "passed": future == 0,
    })

    # 7. Aggregate reconciliation
    src_sum = source["notional_amount"].sum()
    tgt_sum = target["notional_amount"].sum()
    pct_diff = abs(tgt_sum - src_sum) / src_sum * 100
    findings.append({
        "category": "Aggregates",
        "check": "Total notional reconciliation (tolerance 0.01%)",
        "detail": f"src=${src_sum:,.2f} | tgt=${tgt_sum:,.2f} | diff={pct_diff:.6f}%",
        "passed": pct_diff <= 0.01,
    })

    # Stats
    pass_count = sum(1 for f in findings if f["passed"])
    fail_count = len(findings) - pass_count
    overall_pass = fail_count == 0

    # Build HTML
    rows_html = []
    for f in findings:
        row_cls = "pass" if f["passed"] else "fail"
        pill = "PASS" if f["passed"] else "FAIL"
        rows_html.append(
            f"<tr class='{row_cls}'>"
            f"<td>{escape(f['category'])}</td>"
            f"<td>{escape(f['check'])}</td>"
            f"<td class='mono'>{escape(f['detail'])}</td>"
            f"<td><span class='pill {row_cls}'>{pill}</span></td>"
            f"</tr>"
        )
    table_rows = "\n".join(rows_html)

    verdict_cls = "pass" if overall_pass else "fail"
    verdict_text = (
        "✅ RELEASE READY — All quality gates passed"
        if overall_pass
        else f"❌ NOT RELEASE READY — {fail_count} quality gate(s) failed. Triage required."
    )

    html = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>ETL Reconciliation Report</title>
  {CSS}
</head>
<body>
  <h1>ETL Reconciliation Report</h1>
  <div class="subtitle">
    Pipeline: <strong>treasury_trades</strong> &middot;
    Batch: <strong>BATCH_20260513_001</strong> &middot;
    Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
  </div>

  <div class="verdict {verdict_cls}">{verdict_text}</div>

  <div class="summary-grid">
    <div class="stat-card">
      <div class="label">Source Rows</div>
      <div class="value">{src_n:,}</div>
    </div>
    <div class="stat-card">
      <div class="label">Target Rows</div>
      <div class="value">{tgt_n:,}</div>
    </div>
    <div class="stat-card">
      <div class="label">Checks Passed</div>
      <div class="value">{pass_count} / {len(findings)}</div>
    </div>
    <div class="stat-card">
      <div class="label">Critical Findings</div>
      <div class="value">{fail_count}</div>
    </div>
  </div>

  <h2>Quality Gate Results</h2>
  <table>
    <thead>
      <tr><th>Category</th><th>Check</th><th>Detail</th><th>Result</th></tr>
    </thead>
    <tbody>
      {table_rows}
    </tbody>
  </table>

  <h2>Next Steps</h2>
  <p class="small">
    {'All checks have passed and the pipeline is recommended for promotion to UAT.' if overall_pass else 'Critical defects have been logged for Dev triage. Refer to the JIRA tracker for individual defect IDs. UAT should not begin until all CRITICAL items are resolved.'}
  </p>

  <hr style="margin-top: 40px; border: none; border-top: 1px solid #eee;">
  <p class="small">
    Report generated automatically by the ETL Validation Toolkit.<br>
    Contact the QA team for questions about specific findings.
  </p>
</body>
</html>"""

    output_path.write_text(html, encoding="utf-8")
    print(f"Report written to: {output_path}")
    print(f"Open in a browser to view, or attach to email for stakeholder sign-off.")
    return output_path


if __name__ == "__main__":
    build_html_report(
        source_path=DATA_DIR / "source_trades.csv",
        target_path=DATA_DIR / "target_trades.csv",
        output_path=REPORT_DIR / "reconciliation_report.html",
    )
