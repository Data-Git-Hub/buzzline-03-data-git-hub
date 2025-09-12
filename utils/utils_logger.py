"""
Logger Setup Script
File: utils/utils_logger.py

This script provides logging functions for the project.
Logging is an essential way to track events and issues during execution.

Features:
- Logs information, warnings, and errors to a designated log file.
- Ensures the log directory exists.
- Sanitizes logs to remove personal/identifying information for GitHub sharing.
"""

#####################################
# Import Modules
#####################################

# Imports from Python Standard Library
import os
import pathlib
import getpass
import sys
from typing import Mapping, Any

# Imports from external packages
from loguru import logger

#####################################
# Default Configurations
#####################################

LOG_FOLDER: pathlib.Path = pathlib.Path("logs")
LOG_FOLDER.mkdir(parents=True, exist_ok=True)

APP_NAME = pathlib.Path(sys.argv[0]).stem or "app"
PID = os.getpid()
LOG_FILE: pathlib.Path = LOG_FOLDER / f"{APP_NAME}_{PID}.log"

#####################################
# Helper Functions
#####################################


def sanitize_message(record: Mapping[str, Any]) -> str:
    # ... (leave your existing sanitize_message exactly as-is)
    message = record["message"]
    try:
        current_user = getpass.getuser()
        message = message.replace(current_user, "USER")
    except Exception:
        pass
    try:
        home_path = str(pathlib.Path.home())
        message = message.replace(home_path, "~")
    except Exception:
        pass
    try:
        cwd = str(pathlib.Path.cwd())
        message = message.replace(cwd, "PROJECT_ROOT")
    except Exception:
        pass
    message = message.replace("\\", "/")
    message = message.replace("{", "{{").replace("}", "}}")
    return message

def format_sanitized(record: Mapping[str, Any]) -> str:
    # ... (unchanged)
    message = sanitize_message(record)
    time_str = record["time"].strftime("%Y-%m-%d %H:%M:%S")
    level_name = record["level"].name
    return f"{time_str} | {level_name} | {message}\n"

try:
    logger.remove()
    # File sink: per-process file, still rotated (safe because files are unique)
    logger.add(
        LOG_FILE,
        level="INFO",
        rotation="5 MB",     # adjust if you want smaller/larger
        retention=2,         # keep a couple of rotated files
        compression=None,
        enqueue=True,        # good across processes, but rename still per-file
        format=format_sanitized,
        backtrace=False,
        diagnose=False,
    )
    # Stderr sink
    logger.add(
        sys.stderr,
        level="INFO",
        enqueue=True,
        format=format_sanitized,
        backtrace=False,
        diagnose=False,
    )
    logger.info(f"Logging to file: {LOG_FILE}")
    logger.info("Log sanitization enabled, personal info removed")
except Exception as e:
    # If anything goes wrong, fall back to stderr-only
    try:
        logger.remove()
    except Exception:
        pass
    logger.add(sys.stderr, level="INFO", format=format_sanitized)
    logger.warning(f"File logging disabled due to error: {e}")