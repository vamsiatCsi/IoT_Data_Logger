"""Centralised application settings (dotenv + env overrides)."""
from __future__ import annotations
import os
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).parents[1]
load_dotenv(ROOT / ".env", override=False)

class settings:                            # pylint: disable=too-few-public-methods
    FRAPPE_URL       = os.getenv("FRAPPE_URL", "http://192.168.1.63:8000")
    FRAPPE_USER      = os.getenv("FRAPPE_USER", "Administrator")
    FRAPPE_PWD       = os.getenv("FRAPPE_PWD",  "manik0204")
    CACHE_TTL        = int(os.getenv("CACHE_TTL", 300))
    LOG_LEVEL        = os.getenv("LOG_LEVEL", "INFO").upper()
