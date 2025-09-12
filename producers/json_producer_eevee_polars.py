"""
json_producer_eevee_polars.py

Streams JSON events for an Eevee evolution simulator.

- Reads rules from CSV using Polars: data/eevee_rules.csv
- Each tick:
  * Randomly choose a target evolution (Flareon, Jolteon, ...)
  * Randomly choose an action (correct or incorrect)
  * Check rule from CSV (via Polars) to decide outcome
  * Emit JSON to Kafka

Output JSON example:
{
  "event": "evolution_attempt",
  "species": "Eevee",
  "chosen_evolution": "Flareon",
  "chosen_action": "Leveled with a Fire Stone",
  "is_valid": true,
  "message": "Your Eevee is evolving into Flareon!",
  "author": "Eevee Engine",
  "ts": "2025-01-01T12:34:56Z"
}
"""

# Stdlib
import os, sys, time, json, random, pathlib
from datetime import datetime, timezone
from typing import Any, Dict

# External
import polars as pl
from dotenv import load_dotenv

# Local utils
from utils.utils_logger import logger
from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)

# ---------- Env / paths ----------
load_dotenv()

def get_topic() -> str:
    # Use a dedicated topic if present; fall back to BUZZ_TOPIC for compatibility
    topic = os.getenv("EEVEE_TOPIC") or os.getenv("BUZZ_TOPIC") or "eevee_topic"
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_interval() -> float:
    return float(os.getenv("EEVEE_INTERVAL_SECONDS", "1.0"))

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
DATA_FOLDER = PROJECT_ROOT / "data"
RULES_CSV = os.getenv("EEVEE_RULES_CSV", str(DATA_FOLDER / "eevee_rules.csv"))

# Some “idle” responses if the action doesn’t match
IDLE_RESPONSES = [
    "Eevee does nothing.",
    "Eevee is bored.",
    "Eevee is smelling a flower.",
    "Eevee chases its tail.",
    "Eevee tilts its head curiously.",
]

# Add a few plausible but wrong actions to increase variety
EXTRA_WRONG_ACTIONS = [
    "Threw a Poké Ball",
    "Ate a berry",
    "Took a nap",
    "Went for a walk",
    "Practiced Quick Attack",
]

def normalize(s: str) -> str:
    return (s or "").strip().casefold()

def load_rules(csv_path: str) -> pl.DataFrame:
    if not pathlib.Path(csv_path).exists():
        logger.error(f"Rules CSV not found: {csv_path}")
        sys.exit(1)
    df = pl.read_csv(csv_path, has_header=True, infer_schema_length=0)
    required = {"evolution", "correct_action"}
    if not required.issubset(set(df.columns)):
        logger.error(f"Rules CSV missing columns {required}. Found: {df.columns}")
        sys.exit(1)
    # Deduplicate & clean
    df = (
        df.select(
            pl.col("evolution").cast(pl.Utf8),
            pl.col("correct_action").cast(pl.Utf8),
        )
        .unique(subset=["evolution"], keep="first")
    )
    logger.info(f"Loaded {df.height} evolution rules from {csv_path}")
    return df

def build_action_pool(rules: pl.DataFrame) -> list[str]:
    correct_actions = rules["correct_action"].to_list()
    # Mix correct + deliberately wrong actions
    pool = list({*correct_actions, *EXTRA_WRONG_ACTIONS})
    random.shuffle(pool)
    return pool

def make_payload(
    species: str,
    evo: str,
    action: str,
    is_valid: bool,
) -> Dict[str, Any]:
    msg = (
        f"Your Eevee is evolving into {evo}!"
        if is_valid else random.choice(IDLE_RESPONSES)
    )
    return {
        "event": "evolution_attempt",
        "species": species,
        "chosen_evolution": evo,
        "chosen_action": action,
        "is_valid": is_valid,
        "message": msg,
        "author": "Eevee Engine",
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }

def main() -> None:
    logger.info("START Eevee producer (Polars).")
    verify_services()  # checks Kafka availability via admin client

    topic = get_topic()
    interval = get_interval()

    # Load rules via Polars
    rules = load_rules(RULES_CSV)
    evolutions = rules["evolution"].to_list()
    # Map evolution -> correct action (normalized lookup)
    lut = {
        normalize(row["evolution"]): row["correct_action"]
        for row in rules.to_dicts()
    }
    action_pool = build_action_pool(rules)

    # Create Kafka producer & topic
    producer = create_kafka_producer(
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )
    if not producer:
        logger.error("Could not create Kafka producer.")
        sys.exit(3)

    try:
        create_kafka_topic(topic)
    except Exception as e:
        logger.error(f"Topic setup failed for '{topic}': {e}")
        sys.exit(1)

    logger.info(f"Producing Eevee evolution attempts to '{topic}'...")
    try:
        i = 0
        while True:
            evo = random.choice(evolutions)
            action = random.choice(action_pool)

            correct = lut.get(normalize(evo), "")
            is_valid = normalize(action) == normalize(correct)

            payload = make_payload("Eevee", evo, action, is_valid)
            # (Optional) add a monotonically increasing id
            payload["event_id"] = i
            i += 1

            producer.send(topic, value=payload)
            logger.info(f"sent: {payload}")
            time.sleep(interval)

    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error during production: {e}")
    finally:
        try:
            producer.flush()
            producer.close(timeout=5)
        except Exception:
            pass
        logger.info("Kafka producer closed.")
        logger.info("END Eevee producer (Polars).")

if __name__ == "__main__":
    main()