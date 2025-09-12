"""
csv_consumer_eevee_polars.py

Consume CSV lines from Kafka, parse to dicts, aggregate with Polars, and every N seconds:
  - print overall totals and a Top-5 leaderboard (by successes)

Env:
  EEVEE_CSV_TOPIC=eevee_csv_v1
  EEVEE_CSV_GROUP_ID=eevee_csv_g1
  EEVEE_LEADERBOARD_INTERVAL_SECONDS=15

Requires: polars, kafka-python, python-dotenv, loguru
"""

# Stdlib
import os, time, csv
from typing import Any, Dict, List

# External
import polars as pl
from dotenv import load_dotenv

# Local utils
from utils.utils_logger import logger
from utils.utils_consumer import create_kafka_consumer

# -------------------- ENV --------------------
load_dotenv()

def get_topic() -> str:
    t = os.getenv("EEVEE_CSV_TOPIC")
    if not t:
        raise RuntimeError("EEVEE_CSV_TOPIC not set in environment")
    return t

def get_group_id() -> str:
    return os.getenv("EEVEE_CSV_GROUP_ID", "eevee_csv_g1")

def get_interval_secs() -> float:
    try:
        return float(os.getenv("EEVEE_LEADERBOARD_INTERVAL_SECONDS", "15"))
    except Exception:
        return 15.0

# -------------------- CSV / POLARS HELPERS --------------------
COLUMNS = [
    "event","species","chosen_evolution","chosen_action",
    "is_valid","message","author","ts","event_id"
]

EMPTY_SCHEMA = {
    "chosen_evolution": pl.Utf8,
    "is_valid": pl.Boolean,
    "ts": pl.Utf8,
}

def parse_csv_line(line: str) -> Dict[str, Any]:
    """
    Parse a single CSV line into a dict using fixed fieldnames.
    """
    r = csv.DictReader([line], fieldnames=COLUMNS)
    row = next(r)
    # normalize key fields for analytics
    evo = (row.get("chosen_evolution","") or "").strip().lower().capitalize()
    row["chosen_evolution"] = evo
    # csv gives strings; cast to bool
    val = str(row.get("is_valid","")).strip().lower()
    row["is_valid"] = val in {"1","true","t","yes","y"}
    return row

def df_from_batch_csv(lines: List[str]) -> pl.DataFrame:
    if not lines:
        return pl.DataFrame(schema=EMPTY_SCHEMA)
    rows = [parse_csv_line(x) for x in lines if x is not None]
    if not rows:
        return pl.DataFrame(schema=EMPTY_SCHEMA)
    return pl.DataFrame(
        {
            "chosen_evolution": [r["chosen_evolution"] for r in rows],
            "is_valid": [r["is_valid"] for r in rows],
            "ts": [r.get("ts","") for r in rows],
        },
        schema=EMPTY_SCHEMA,
    )

def compute_rollup(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return pl.DataFrame({"chosen_evolution": [], "attempts": [], "successes": [], "success_rate_pct": []})
    return (
        df.group_by("chosen_evolution")
          .agg([
              pl.len().alias("attempts"),
              pl.col("is_valid").cast(pl.Int64).sum().alias("successes"),
          ])
          .with_columns(
              (pl.when(pl.col("attempts") > 0)
                   .then(pl.col("successes") * 100.0 / pl.col("attempts"))
                   .otherwise(0.0)
               ).round(1).alias("success_rate_pct")
          )
          .sort(["successes", "attempts"], descending=[True, True])
    )

def overall_stats(df: pl.DataFrame) -> Dict[str, Any]:
    total = df.height
    succ = 0 if total == 0 else int(df.select(pl.col("is_valid").cast(pl.Int64).sum()).item())
    fail = total - succ
    succ_pct = (succ * 100.0 / total) if total else 0.0
    return {
        "total": total,
        "success": succ,
        "fail": fail,
        "success_pct": round(succ_pct, 1),
        "fail_pct": round(100.0 - succ_pct, 1) if total else 0.0,
    }

def format_leaderboard(tbl: pl.DataFrame, k: int = 5) -> str:
    if tbl.is_empty():
        return "(no data yet)"
    out = []
    for r in tbl.head(k).to_dicts():
        out.append(
            f"- {r['chosen_evolution']:<9}  attempts={r['attempts']:<4}  "
            f"successes={r['successes']:<4}  rate={r['success_rate_pct']:>5.1f}%"
        )
    return "\n".join(out)

# -------------------- MAIN --------------------
def main() -> None:
    topic = get_topic()
    group_id = get_group_id()
    interval = get_interval_secs()

    logger.info(f"START Eevee CSV consumer | topic='{topic}' group='{group_id}' interval={interval}s")
    consumer = create_kafka_consumer(topic, group_id)

    df_all = pl.DataFrame(schema=EMPTY_SCHEMA)
    next_report = time.monotonic() + interval

    try:
        while True:
            records = consumer.poll(timeout_ms=1000, max_records=200)
            lines: List[str] = []

            if records:
                for _tp, batch in records.items():
                    for msg in batch:
                        val = msg.value
                        # utils_consumer typically sets value to str; handle bytes just in case
                        if isinstance(val, bytes):
                            try:
                                val = val.decode("utf-8", errors="ignore")
                            except Exception:
                                continue
                        if isinstance(val, str) and val.strip():
                            lines.append(val)

            if lines:
                df_batch = df_from_batch_csv(lines)
                if not df_batch.is_empty():
                    df_all = df_all.vstack(df_batch)

            now = time.monotonic()
            if now >= next_report:
                tbl = compute_rollup(df_all)
                stats = overall_stats(df_all)

                logger.info("=== Eevee Evolution Leaderboard (rolling) [CSV] ===")
                logger.info(
                    f"Overall: total={stats['total']}  "
                    f"success={stats['success']} ({stats['success_pct']}%)  "
                    f"fail={stats['fail']} ({stats['fail_pct']}%)"
                )
                logger.info("Top 5 by successes:")
                logger.info("\n" + format_leaderboard(tbl, k=5))

                next_report += interval
                if now > next_report + interval:
                    next_report = now + interval

    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        logger.info("Kafka consumer closed.")
        logger.info("END Eevee CSV consumer.")

if __name__ == "__main__":
    main()