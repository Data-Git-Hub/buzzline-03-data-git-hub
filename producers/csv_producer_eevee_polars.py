"""
csv_producer_eevee_polars.py

Stream CSV lines to Kafka for an Eevee evolution simulator.

Each tick:
  - Randomly choose a target evolution (Flareon, Jolteon, ...)
  - Randomly choose an action (sometimes correct, often not)
  - Check against rules loaded from CSV (via Polars)
  - Emit ONE CSV line per event (no header) to a Kafka topic
"""

# Stdlib
import os, sys, time, json, random, pathlib, io, csv
from datetime import datetime, timezone
from typing import Any, Dict, List

# External
import polars as pl
from dotenv import load_dotenv

# Local utils
from utils.utils_logger import logger
from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,   # NOTE: your version recreates topics; prefer unique topics.
)

# ---------------------- ENV / PATHS ----------------------

load_dotenv()

def get_topic() -> str:
    t = os.getenv("EEVEE_CSV_TOPIC")
    if not t:
        raise RuntimeError("EEVEE_CSV_TOPIC not set in environment")
    logger.info(f"Kafka topic: {t}")
    return t

def get_interval() -> float:
    try:
        return float(os.getenv("EEVEE_CSV_INTERVAL_SECONDS", "1.0"))
    except Exception:
        return 1.0

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
DATA_FOLDER = PROJECT_ROOT / "data"
RULES_CSV_PATH = os.getenv("EEVEE_RULES_CSV", str(DATA_FOLDER / "eevee_rules.csv"))

# ---------------------- CONSTANTS ------------------------

COLUMNS: List[str] = [
    "event", "species", "chosen_evolution", "chosen_action",
    "is_valid", "message", "author", "ts", "event_id"
]

IDLE_RESPONSES = [
    "Eevee does nothing.",
    "Eevee is bored.",
    "Eevee is smelling a flower.",
    "Eevee chases its tail.",
    "Eevee tilts its head curiously.",
]

EXTRA_WRONG_ACTIONS = [
    "Threw a Poké Ball",
    "Ate a berry",
    "Took a nap",
    "Went for a walk",
    "Practiced Quick Attack",
]

# ---------------------- HELPERS --------------------------

def to_csv_row(d: Dict[str, Any]) -> str:
    """Serialize a dict as a single CSV line (no header)."""
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=COLUMNS, extrasaction="ignore")
    w.writerow(d)                      # handles quoting safely
    return buf.getvalue().rstrip("\n") # one line per message

def normalize(s: str) -> str:
    return (s or "").strip().casefold()

def load_rules(csv_path: str) -> pl.DataFrame:
    p = pathlib.Path(csv_path)
    if not p.exists():
        logger.error(f"Rules CSV not found: {csv_path}")
        sys.exit(1)
    df = pl.read_csv(csv_path, has_header=True, infer_schema_length=0)
    required = {"evolution", "correct_action"}
    if not required.issubset(set(df.columns)):
        logger.error(f"Rules CSV missing columns {required}. Found: {df.columns}")
        sys.exit(1)
    df = (
        df.select(
            pl.col("evolution").cast(pl.Utf8),
            pl.col("correct_action").cast(pl.Utf8),
        )
        .unique(subset=["evolution"], keep="first")
    )
    logger.info(f"Loaded {df.height} evolution rules from {csv_path}")
    return df

def build_action_pool(rules: pl.DataFrame) -> List[str]:
    correct = rules["correct_action"].to_list()
    pool = list({*correct, *EXTRA_WRONG_ACTIONS})
    random.shuffle(pool)
    return pool

def make_payload(species: str, evo: str, action: str, is_valid: bool, i: int) -> Dict[str, Any]:
    msg = f"Your Eevee is evolving into {evo}!" if is_valid else random.choice(IDLE_RESPONSES)
    return {
        "event": "evolution_attempt",
        "species": species,
        "chosen_evolution": evo,
        "chosen_action": action,
        "is_valid": is_valid,  # will be written as True/False -> 'True'/'False' in CSV
        "message": msg,
        "author": "Eevee Engine",
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "event_id": i,
    }

# ---------------------- MAIN -----------------------------

def main() -> None:
    logger.info("START Eevee CSV producer (Polars).")
    verify_services()

    topic = get_topic()
    interval = get_interval()

    # Load rules
    rules = load_rules(RULES_CSV_PATH)
    evolutions = rules["evolution"].to_list()
    lut = {normalize(r["evolution"]): r["correct_action"] for r in rules.to_dicts()}
    action_pool = build_action_pool(rules)

    # Kafka
    producer = create_kafka_producer(value_serializer=lambda s: s.encode("utf-8"))
    if not producer:
        logger.error("Failed to create Kafka producer.")
        sys.exit(3)

    try:
        # WARNING: your create_kafka_topic recreates existing topics.
        create_kafka_topic(topic)
    except Exception as e:
        logger.error(f"Topic setup failed for '{topic}': {e}")
        sys.exit(1)

    logger.info(f"Producing CSV events to '{topic}'...")
    try:
        i = 0
        while True:
            evo = random.choice(evolutions)
            action = random.choice(action_pool)
            correct = lut.get(normalize(evo), "")
            is_valid = normalize(action) == normalize(correct)

            payload = make_payload("Eevee", evo, action, is_valid, i)
            i += 1

            row = to_csv_row(payload)
            producer.send(topic, value=row)
            logger.info(f"sent: {row}")
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
        logger.info("END Eevee CSV producer.")

if __name__ == "__main__":
    main()