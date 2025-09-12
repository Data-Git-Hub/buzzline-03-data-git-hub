"""
json_consumer_eevee_polars.py

Consume JSON events from the Eevee producer and print a rolling leaderboard.

Expected producer payload fields (from json_producer_eevee_polars.py):
- event               : "evolution_attempt"
- species             : "Eevee"
- chosen_evolution    : e.g., "Flareon"
- chosen_action       : e.g., "Leveled with a Fire Stone"
- is_valid            : bool (True if action matches the rule)
- message             : friendly outcome text
- author              : "Eevee Engine"
- ts                  : ISO timestamp
- event_id            : int (optional)
"""


# Stdlib
import os, json, time, math
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
    t = os.getenv("EEVEE_TOPIC")
    if not t:
        raise RuntimeError("EEVEE_TOPIC not set in environment")
    return t

def get_group_id() -> str:
    return os.getenv("EEVEE_GROUP_ID", "eevee_g1")

def get_interval_secs() -> float:
    try:
        return float(os.getenv("EEVEE_LEADERBOARD_INTERVAL_SECONDS", "15"))
    except Exception:
        return 15.0


# -------------------- POLARS HELPERS --------------------

EMPTY_SCHEMA = {
    "chosen_evolution": pl.Utf8,
    "is_valid": pl.Boolean,
    "ts": pl.Utf8,           # keep as string for speed; parse if you need time windows
}

def normalize_evo(x: str) -> str:
    # Title-case common names; leaves “Sylveon”, “Espeon” neatly formatted even if producer varies
    return (x or "").strip().lower().capitalize()


def df_from_batch(batch: List[Dict[str, Any]]) -> pl.DataFrame:
    if not batch:
        return pl.DataFrame(schema=EMPTY_SCHEMA)
    # Normalize selected fields
    rows = []
    for m in batch:
        try:
            evo = normalize_evo(str(m.get("chosen_evolution", "")))
            valid = bool(m.get("is_valid", False))
            ts = str(m.get("ts", ""))
            rows.append({"chosen_evolution": evo, "is_valid": valid, "ts": ts})
        except Exception:
            # skip malformed message
            continue
    if not rows:
        return pl.DataFrame(schema=EMPTY_SCHEMA)
    return pl.DataFrame(rows, schema=EMPTY_SCHEMA)


def compute_rollup(df: pl.DataFrame) -> pl.DataFrame:
    """
    Returns a per-evolution table with:
      attempts, successes, success_rate_pct
    Sorted by successes desc, then attempts desc.
    """
    if df.is_empty():
        return pl.DataFrame(
            {"chosen_evolution": [], "attempts": [], "successes": [], "success_rate_pct": []}
        )

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
    fail_pct = 100.0 - succ_pct if total else 0.0
    return {
        "total": total,
        "success": succ,
        "fail": fail,
        "success_pct": round(succ_pct, 1),
        "fail_pct": round(fail_pct, 1),
    }


def top_bottom_by_success(tbl: pl.DataFrame):
    """
    Returns (top_name, top_row_dict, bottom_name, bottom_row_dict) based on 'successes'.
    If multiple share the same successes, order is determined by the sort in compute_rollup.
    """
    if tbl.is_empty():
        return None, None, None, None
    top = tbl.row(0, named=True)
    bot = tbl.row(-1, named=True)
    return top["chosen_evolution"], top, bot["chosen_evolution"], bot


def format_leaderboard(tbl: pl.DataFrame, k: int = 5) -> str:
    if tbl.is_empty():
        return "(no data yet)"
    out_lines = []
    for r in tbl.head(k).to_dicts():
        out_lines.append(
            f"- {r['chosen_evolution']:<9}  attempts={r['attempts']:<4}  "
            f"successes={r['successes']:<4}  rate={r['success_rate_pct']:>5.1f}%"
        )
    return "\n".join(out_lines)


# -------------------- MAIN CONSUMER LOOP --------------------

def main() -> None:
    topic = get_topic()
    group_id = get_group_id()
    interval = get_interval_secs()

    logger.info(f"START Eevee Polars consumer | topic='{topic}' group='{group_id}' interval={interval}s")

    consumer = create_kafka_consumer(topic, group_id)

    # Rolling store
    df_all = pl.DataFrame(schema=EMPTY_SCHEMA)

    # Timer for periodic prints
    next_report = time.monotonic() + interval

    try:
        while True:
            # Poll a batch
            records = consumer.poll(timeout_ms=1000, max_records=100)
            batch_msgs: List[Dict[str, Any]] = []

            if records:
                for _tp, batch in records.items():
                    for msg in batch:
                        try:
                            payload = json.loads(msg.value)  # utils_consumer sets value to str already
                            if isinstance(payload, dict) and payload.get("event") == "evolution_attempt":
                                batch_msgs.append(payload)
                        except Exception:
                            # ignore malformed records
                            continue

            # Append into Polars table
            if batch_msgs:
                df_batch = df_from_batch(batch_msgs)
                if not df_batch.is_empty():
                    df_all = df_all.vstack(df_batch)

            # Time to report?
            now = time.monotonic()
            if now >= next_report:
                tbl = compute_rollup(df_all)
                stats = overall_stats(df_all)
                top_name, top_row, bot_name, bot_row = top_bottom_by_success(tbl)

                logger.info("=== Eevee Evolution Leaderboard (rolling) ===")
                logger.info(f"Overall: total={stats['total']}  "
                            f"success={stats['success']} ({stats['success_pct']}%)  "
                            f"fail={stats['fail']} ({stats['fail_pct']}%)")

                if top_row:
                    logger.info(f"Most successful:  {top_name}  "
                                f"(successes={top_row['successes']}, attempts={top_row['attempts']}, "
                                f"rate={top_row['success_rate_pct']}%)")
                else:
                    logger.info("Most successful:  (no data)")

                if bot_row:
                    logger.info(f"Least successful: {bot_name}  "
                                f"(successes={bot_row['successes']}, attempts={bot_row['attempts']}, "
                                f"rate={bot_row['success_rate_pct']}%)")
                else:
                    logger.info("Least successful: (no data)")

                logger.info("Top 5 by successes:")
                logger.info("\n" + format_leaderboard(tbl, k=5))

                # Schedule next report
                next_report += interval
                # Guard against drift if processing pauses
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
        logger.info("END Eevee Polars consumer.")


if __name__ == "__main__":
    main()