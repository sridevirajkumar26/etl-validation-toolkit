"""
Generates anonymized synthetic source and target datasets that mimic
a treasury / financial reporting ETL pipeline (loosely modeled on the
kind of data World Bank or TIAA programs handle, fully synthetic).

Run once to produce sample_data/source_trades.csv and target_trades.csv
with intentional data quality issues injected so the validation scripts
have something to find.
"""

import numpy as np
import pandas as pd
from pathlib import Path

np.random.seed(42)
N_ROWS = 100_000  # bump to 5_000_000 for the full-scale demo

OUT_DIR = Path(__file__).parent.parent / "sample_data"
OUT_DIR.mkdir(exist_ok=True)


def build_source(n: int) -> pd.DataFrame:
    """Simulate a source trading system extract."""
    trade_ids = np.arange(1_000_000, 1_000_000 + n)
    trade_dates = pd.to_datetime("2025-01-01") + pd.to_timedelta(
        np.random.randint(0, 365, n), unit="D"
    )
    instruments = np.random.choice(
        ["BOND", "EQUITY", "FX_SWAP", "IR_SWAP", "LOAN"], n, p=[0.3, 0.25, 0.2, 0.15, 0.1]
    )
    currencies = np.random.choice(
        ["USD", "EUR", "GBP", "JPY", "CAD"], n, p=[0.5, 0.2, 0.15, 0.1, 0.05]
    )
    notional = np.round(np.random.lognormal(mean=12, sigma=1.5, size=n), 2)
    counterparty_ids = np.random.randint(1, 2000, n)
    status = np.random.choice(
        ["SETTLED", "PENDING", "CANCELLED"], n, p=[0.85, 0.12, 0.03]
    )

    df = pd.DataFrame({
        "trade_id": trade_ids,
        "trade_date": trade_dates,
        "instrument_type": instruments,
        "currency_code": currencies,
        "notional_amount": notional,
        "counterparty_id": counterparty_ids,
        "trade_status": status,
        "source_system": "TRADE_CAPTURE_V1",
    })
    return df


def build_target_with_issues(source: pd.DataFrame) -> pd.DataFrame:
    """
    Simulate the target warehouse after ETL.
    Intentionally injects realistic data quality issues that QA should catch:
      - 12 rows dropped (row count mismatch)
      - 50 rows with NULL currency_code (data quality)
      - 100 rows with notional rounded incorrectly (precision loss)
      - 20 duplicate trade_ids (primary key violation)
      - 30 rows with status values not in the allowed list (referential integrity)
      - 5 future-dated trades (business rule violation)
    """
    target = source.copy()

    # Drop 12 random rows
    drop_idx = np.random.choice(target.index, 12, replace=False)
    target = target.drop(drop_idx).reset_index(drop=True)

    # NULL out currency on 50 rows
    null_idx = np.random.choice(target.index, 50, replace=False)
    target.loc[null_idx, "currency_code"] = None

    # Precision loss on notional (round to nearest 10)
    precision_idx = np.random.choice(target.index, 100, replace=False)
    target.loc[precision_idx, "notional_amount"] = (
        (target.loc[precision_idx, "notional_amount"] / 10).round() * 10
    )

    # Duplicate trade_ids
    dup_idx = np.random.choice(target.index, 20, replace=False)
    duplicates = target.loc[dup_idx].copy()
    target = pd.concat([target, duplicates], ignore_index=True)

    # Invalid status values
    invalid_idx = np.random.choice(target.index, 30, replace=False)
    target.loc[invalid_idx, "trade_status"] = "UNKNOWN"

    # Future-dated trades
    future_idx = np.random.choice(target.index, 5, replace=False)
    target.loc[future_idx, "trade_date"] = pd.Timestamp("2099-12-31")

    # Add audit columns the target warehouse would have
    target["etl_load_timestamp"] = pd.Timestamp.now()
    target["etl_batch_id"] = "BATCH_20260513_001"

    return target


if __name__ == "__main__":
    print(f"Generating {N_ROWS:,} source rows...")
    source = build_source(N_ROWS)
    source.to_csv(OUT_DIR / "source_trades.csv", index=False)
    print(f"  Wrote source_trades.csv ({source.shape})")

    print("Generating target with injected data quality issues...")
    target = build_target_with_issues(source)
    target.to_csv(OUT_DIR / "target_trades.csv", index=False)
    print(f"  Wrote target_trades.csv ({target.shape})")

    print("\nInjected issues:")
    print("  - 12 dropped rows (count mismatch)")
    print("  - 50 NULL currency_code values")
    print("  - 100 rows with precision loss on notional")
    print("  - 20 duplicate trade_ids")
    print("  - 30 invalid trade_status values")
    print("  - 5 future-dated trades")
