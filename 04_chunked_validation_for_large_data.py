"""
Memory-efficient chunked validation for 5M+ row datasets.

When the target table is too big to fit in memory, you stream through it
in chunks, accumulating aggregate stats and defect samples incrementally.

This is the pattern that backs the resume claim:
  "Developed Python automation scripts to validate large-volume datasets
   (5M+ records) ... reduced manual validation effort by ~40%."

Key techniques:
  - pd.read_csv(chunksize=...) for streaming
  - Welford's online algorithm for streaming mean / variance
  - Bounded reservoirs for sampling defects (don't blow up memory)
  - Single-pass over the data (I/O is the bottleneck, not CPU)
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List
from dataclasses import dataclass, field
from time import time

DATA_DIR = Path(__file__).parent.parent / "sample_data"


@dataclass
class StreamingStats:
    """Welford's online algorithm: streaming mean and variance in one pass."""
    n: int = 0
    mean: float = 0.0
    M2: float = 0.0  # for variance
    min_val: float = float("inf")
    max_val: float = float("-inf")
    sum_val: float = 0.0

    def update(self, values: pd.Series) -> None:
        values = values.dropna()
        if values.empty:
            return
        for v in values:
            self.n += 1
            delta = v - self.mean
            self.mean += delta / self.n
            self.M2 += delta * (v - self.mean)
        self.min_val = min(self.min_val, float(values.min()))
        self.max_val = max(self.max_val, float(values.max()))
        self.sum_val += float(values.sum())

    @property
    def variance(self) -> float:
        return self.M2 / (self.n - 1) if self.n > 1 else 0.0

    @property
    def std(self) -> float:
        return self.variance ** 0.5


@dataclass
class ChunkedValidator:
    """Accumulates validation findings across chunks."""
    total_rows: int = 0
    null_counts: Dict[str, int] = field(default_factory=dict)
    invalid_status_count: int = 0
    invalid_status_samples: List = field(default_factory=list)
    duplicate_trade_ids: Dict[int, int] = field(default_factory=dict)
    future_date_count: int = 0
    notional_stats: StreamingStats = field(default_factory=StreamingStats)
    seen_trade_ids: set = field(default_factory=set)
    allowed_status: tuple = ("SETTLED", "PENDING", "CANCELLED")
    today: pd.Timestamp = field(
        default_factory=lambda: pd.Timestamp.now().normalize()
    )

    def process_chunk(self, chunk: pd.DataFrame) -> None:
        self.total_rows += len(chunk)

        # Null counts per column
        for col in chunk.columns:
            self.null_counts[col] = self.null_counts.get(col, 0) + int(
                chunk[col].isna().sum()
            )

        # Invalid status values
        invalid_mask = ~chunk["trade_status"].isin(self.allowed_status) & chunk[
            "trade_status"
        ].notna()
        self.invalid_status_count += int(invalid_mask.sum())
        if invalid_mask.any() and len(self.invalid_status_samples) < 10:
            self.invalid_status_samples.extend(
                chunk.loc[invalid_mask, "trade_status"].unique().tolist()[
                    : 10 - len(self.invalid_status_samples)
                ]
            )

        # Duplicate trade_ids (across chunks)
        for tid in chunk["trade_id"]:
            if tid in self.seen_trade_ids:
                self.duplicate_trade_ids[tid] = (
                    self.duplicate_trade_ids.get(tid, 1) + 1
                )
            else:
                self.seen_trade_ids.add(tid)

        # Future date detection
        dates = pd.to_datetime(chunk["trade_date"], errors="coerce")
        self.future_date_count += int((dates > self.today).sum())

        # Streaming numeric stats
        self.notional_stats.update(chunk["notional_amount"])

    def report(self) -> Dict:
        """Final aggregated report."""
        return {
            "total_rows": self.total_rows,
            "null_counts": self.null_counts,
            "invalid_status_count": self.invalid_status_count,
            "invalid_status_samples": list(set(self.invalid_status_samples)),
            "duplicate_trade_id_count": len(self.duplicate_trade_ids),
            "future_date_count": self.future_date_count,
            "notional_min": round(self.notional_stats.min_val, 2),
            "notional_max": round(self.notional_stats.max_val, 2),
            "notional_mean": round(self.notional_stats.mean, 2),
            "notional_std": round(self.notional_stats.std, 2),
            "notional_sum": round(self.notional_stats.sum_val, 2),
        }


def run_chunked_validation(target_path: Path, chunksize: int = 50_000) -> Dict:
    """Stream through the file in chunks and accumulate findings."""
    print(f"Streaming validation on {target_path.name}")
    print(f"  Chunk size: {chunksize:,} rows")

    validator = ChunkedValidator()
    start = time()
    chunk_num = 0

    for chunk in pd.read_csv(target_path, chunksize=chunksize):
        chunk_num += 1
        validator.process_chunk(chunk)
        if chunk_num % 10 == 0 or chunk_num == 1:
            print(f"  Processed chunk {chunk_num} ({validator.total_rows:,} rows so far)")

    elapsed = time() - start
    report = validator.report()
    report["elapsed_seconds"] = round(elapsed, 2)
    report["throughput_rows_per_sec"] = (
        round(validator.total_rows / elapsed, 0) if elapsed else 0
    )

    print(f"\nCompleted in {elapsed:.2f}s")
    print(f"Throughput: {report['throughput_rows_per_sec']:,} rows/sec")
    print(f"\n=== CHUNKED VALIDATION REPORT ===")
    print(f"  Total rows:              {report['total_rows']:,}")
    print(f"  Duplicate trade_ids:     {report['duplicate_trade_id_count']}")
    print(f"  Invalid status values:   {report['invalid_status_count']}  "
          f"samples: {report['invalid_status_samples']}")
    print(f"  Future-dated trades:     {report['future_date_count']}")
    print(f"  Notional sum (streamed): {report['notional_sum']:,.2f}")
    print(f"  Notional mean (streamed):{report['notional_mean']:,.2f}")
    print(f"  Null counts:             "
          f"{ {k: v for k, v in report['null_counts'].items() if v > 0} }")
    return report


if __name__ == "__main__":
    run_chunked_validation(DATA_DIR / "target_trades.csv", chunksize=10_000)
