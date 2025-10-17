#!/usr/bin/env python3
"""
fetch_rates.py

Features:
- Reads configuration from environment (.env expected)
- Checks API /status for monthly quota
- Finds earliest incomplete month since START_YEAR and attempts to fill missing days
- If ALLOW_PARTIAL_MONTH=true, will download as many missing days as quota allows;
  otherwise requires quota for entire month (plus safety buffer)
- Respects 10 requests/minute rate limit (client-side)
- Retries transient failures with exponential backoff
- Writes per-month CSV files YYYY-MM.csv, ensures dedupe & sort, uses file locking
- Logs to stdout
"""

from __future__ import annotations
import os
import sys
import time
import csv
import logging
import requests
import math
from datetime import date
from dotenv import load_dotenv
from pathlib import Path
from calendar import monthrange
from filelock import FileLock, Timeout

# Load .env
load_dotenv()

# ---------------------
# Configuration (from env)
# ---------------------
API_KEY = os.getenv("FREECURRENCY_API_KEY")
if not API_KEY:
    print("FREECURRENCY_API_KEY not set. Please put it in .env", file=sys.stderr)
    sys.exit(2)

API_BASE_URL = os.getenv("API_BASE_URL", "https://api.freecurrencyapi.com/v1")
BASE_CURRENCY = os.getenv("BASE_CURRENCY", "USD").upper()
START_YEAR = int(os.getenv("START_YEAR", "2023"))
DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
CRON_SCHEDULE = os.getenv("CRON_SCHEDULE", "0 3 * * *")  # used by run-cron.sh
MAX_PER_MINUTE = int(os.getenv("MAX_REQUESTS_PER_MINUTE", "10"))
SAFETY_BUFFER = int(os.getenv("SAFETY_BUFFER", "1"))  # extra requests to reserve
ALLOW_PARTIAL_MONTH = os.getenv("ALLOW_PARTIAL_MONTH", "false").lower() in ("1","true","yes")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "4"))
RETRY_BASE_SECONDS = float(os.getenv("RETRY_BASE_SECONDS", "1.0"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
CURRENCIES_ENV = os.getenv("CURRENCIES")
if CURRENCIES_ENV:
    CURRENCIES = [c.strip().upper() for c in CURRENCIES_ENV.split(",") if c.strip()]
else:
    CURRENCIES = [
        "USD", "EUR", "GBP", "JPY", "CNY", "INR", "AUD", "CAD", "CHF",
        "SEK", "NOK", "DKK", "SGD", "HKD", "KRW", "ZAR", "BRL", "MXN", "TRY"
    ]
# file lock timeout
FILELOCK_TIMEOUT = float(os.getenv("FILELOCK_TIMEOUT", "10.0"))

DATA_DIR.mkdir(parents=True, exist_ok=True)

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# Rate-limiter rolling timestamps (seconds since epoch)
_request_timestamps: list[float] = []

# ---------------------
# Helpers
# ---------------------
def wait_for_rate_slot():
    """
    Ensure we do not exceed MAX_PER_MINUTE requests in any 60-second window.
    If full, sleep until a slot is available.
    """
    global _request_timestamps
    now = time.time()
    # keep only timestamps in last 60s
    _request_timestamps = [t for t in _request_timestamps if now - t < 60.0]
    if len(_request_timestamps) < MAX_PER_MINUTE:
        return
    oldest = min(_request_timestamps)
    wait_seconds = 60.0 - (now - oldest) + 0.05
    logging.info("Rate limiter reached %d/min. Sleeping %.1fs...", MAX_PER_MINUTE, wait_seconds)
    time.sleep(wait_seconds)
    # cleanup after sleep
    now = time.time()
    _request_timestamps = [t for t in _request_timestamps if now - t < 60.0]


def record_request_timestamp():
    _request_timestamps.append(time.time())


def do_request(path: str, params: dict | None = None, count_against_rate: bool = True):
    """
    Perform GET with retries and exponential backoff.
    If count_against_rate is True, the call respects client-side per-minute rate limit.
    """
    url = f"{API_BASE_URL}{path}"
    headers = {"apikey": API_KEY}
    attempt = 0
    while True:
        attempt += 1
        if count_against_rate:
            wait_for_rate_slot()
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as e:
            logging.warning("Request exception on attempt %d: %s", attempt, e)
            if attempt <= MAX_RETRIES:
                backoff = RETRY_BASE_SECONDS * (2 ** (attempt - 1))
                logging.info("Retrying in %.1fs...", backoff)
                time.sleep(backoff)
                continue
            raise
        if count_against_rate:
            record_request_timestamp()
        # If service returned 5xx or 429, consider retrying (429 may be quota)
        if resp.status_code >= 500 and attempt <= MAX_RETRIES:
            backoff = RETRY_BASE_SECONDS * (2 ** (attempt - 1))
            logging.warning("Server error %d. Retry %d/%d in %.1fs", resp.status_code, attempt, MAX_RETRIES, backoff)
            time.sleep(backoff)
            continue
        # For 429 (rate/quota) don't endlessly retry; caller will handle
        return resp


# ---------------------
# API-specific helpers
# ---------------------
def get_status() -> dict | None:
    """
    Calls /status. Per API docs this should not count against quota.
    Returns dictionary with keys 'total','used','remaining' for month if present.
    """
    try:
        r = do_request("/status", params=None, count_against_rate=False)
    except Exception as e:
        logging.error("Failed to contact status endpoint: %s", e)
        return None
    if r.status_code != 200:
        logging.error("Status endpoint returned %d: %s", r.status_code, r.text)
        return None
    j = r.json()
    quotas = j.get("quotas", {})
    month = quotas.get("month", {})
    return {
        "total": month.get("total"),
        "used": month.get("used"),
        "remaining": month.get("remaining"),
        "raw": j
    }


def get_historical_for_date(date_iso: str) -> dict:
    """
    Fetch historical rates for a single date (YYYY-MM-DD).
    Returns a dict with keys: 'date' and currency codes.
    On non-200 responses raises RuntimeError (caller handles saving partials).
    """
    params = {
        "date": date_iso,
        "base_currency": BASE_CURRENCY,
        "currencies": ",".join(CURRENCIES)
    }
    resp = do_request("/historical", params=params, count_against_rate=True)
    if resp.status_code == 429:
        # quota/rate limit exceeded
        logging.error("API returned 429 for date %s: %s", date_iso, resp.text)
        raise RuntimeError("API 429: rate/quota exceeded")
    if resp.status_code != 200:
        logging.error("Non-200 from historical %s: %s", resp.status_code, resp.text)
        raise RuntimeError(f"HTTP {resp.status_code}")
    data = resp.json().get("data", {}) or {}
    day = data.get(date_iso) or {}
    # Ensure base currency present
    day[BASE_CURRENCY] = 1.0
    row = {"date": date_iso}
    for c in CURRENCIES:
        v = day.get(c)
        row[c] = "" if v is None else float(v)
    return row


# ---------------------
# CSV helpers
# ---------------------
def csv_path_for(year: int, month: int) -> Path:
    return DATA_DIR / f"{year:04d}-{month:02d}.csv"


def read_csv_rows(path: Path) -> dict:
    """
    Read existing CSV (if any) into dict: date -> row-dict (strings/floats)
    """
    result = {}
    if not path.exists():
        return result
    try:
        with open(path, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for r in reader:
                # Normalize rows to expected columns
                d = r.get("date")
                if not d:
                    continue
                # Convert numeric strings to float where possible, keep blank as ''
                normalized = {"date": d}
                for c in CURRENCIES:
                    val = r.get(c, "")
                    if val == "" or val is None:
                        normalized[c] = ""
                    else:
                        try:
                            normalized[c] = float(val)
                        except Exception:
                            normalized[c] = val
                result[d] = normalized
    except Exception as e:
        logging.warning("Failed to read CSV %s: %s", path, e)
    return result


def write_csv_rows(path: Path, rows_by_date: dict):
    """
    Write rows_by_date (date->row) to CSV, sorted by date ascending.
    Uses a temporary file and atomic rename.
    """
    header = ["date"] + CURRENCIES
    tmp = path.with_suffix(".tmp")
    dates_sorted = sorted(rows_by_date.keys())
    try:
        with open(tmp, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=header)
            writer.writeheader()
            for d in dates_sorted:
                writer.writerow(rows_by_date[d])
        # atomic move
        tmp.replace(path)
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except Exception:
                pass


# ---------------------
# Month iteration helpers
# ---------------------
def months_since(year_start: int):
    """
    Yield (year, month) tuples from year_start-01 up to current year/month inclusive
    """
    today = date.today()
    y = year_start
    m = 1
    while (y < today.year) or (y == today.year and m <= today.month):
        yield (y, m)
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1


def dates_in_month(year: int, month: int) -> list[str]:
    nd = monthrange(year, month)[1]
    return [date(year, month, d).isoformat() for d in range(1, nd + 1)]


# ---------------------
# Main logic
# ---------------------
def main():
    logging.info("Starting fetch_rates.py (base=%s). Allow partial=%s", BASE_CURRENCY, ALLOW_PARTIAL_MONTH)

    status = get_status()
    if status is None:
        logging.error("Cannot obtain status from API. Exiting.")
        return 1

    remaining = status.get("remaining")
    logging.info("Monthly quota: total=%s used=%s remaining=%s",
                 status.get("total"), status.get("used"), remaining)

    # find first month with missing days
    target = None
    for (y, m) in months_since(START_YEAR):
        path = csv_path_for(y, m)
        existing = read_csv_rows(path)
        all_dates = dates_in_month(y, m)
        missing = [d for d in all_dates if d not in existing]
        if missing:
            target = (y, m, missing, existing)
            break

    if not target:
        logging.info("No missing data found since %d. Nothing to do.", START_YEAR)
        return 0

    year, month, missing_dates, existing_rows = target
    missing_dates.sort()
    ndays = len(missing_dates)
    logging.info("Targeting %04d-%02d with %d missing days", year, month, ndays)

    # Decide how many days we can fetch based on quota and ALLOW_PARTIAL_MONTH
    need = ndays
    if remaining is None:
        logging.error("Status does not expose remaining monthly quota. Exiting.")
        return 1

    if remaining < SAFETY_BUFFER:
        logging.info("Remaining quota (%d) below safety buffer (%d). Exiting.", remaining, SAFETY_BUFFER)
        return 0

    # available for fetch
    available = remaining - SAFETY_BUFFER
    if available <= 0:
        logging.info("No available requests after buffer. Exiting.")
        return 0

    if available < need:
        if not ALLOW_PARTIAL_MONTH:
            logging.info("Not enough quota (%d) to fetch %d days and partial not allowed. Exiting.", available, need)
            return 0
        else:
            logging.info("Partial allowed: will fetch %d of %d missing days based on quota.",
                         available, need)
            # choose earliest N days
            missing_dates = missing_dates[:available]
            need = len(missing_dates)

    # Acquire file lock for the month CSV so concurrent runs don't clash
    path = csv_path_for(year, month)
    lock_path = str(path) + ".lock"
    lock = FileLock(lock_path, timeout=FILELOCK_TIMEOUT)
    try:
        with lock:
            logging.info("Acquired file lock %s", lock_path)
            # Re-load existing file inside lock to avoid race
            existing_rows = read_csv_rows(path)
            # recompute missing (in case another run filled some)
            all_dates = dates_in_month(year, month)
            missing_dates_current = [d for d in all_dates if d not in existing_rows]
            # intersect with desired missing_dates
            missing_dates = [d for d in missing_dates if d in missing_dates_current]
            missing_dates.sort()
            if not missing_dates:
                logging.info("Nothing to fetch after re-check inside lock (another run filled it).")
                return 0

            rows_fetched = {}
            try:
                for dt in missing_dates:
                    logging.info("Fetching %s", dt)
                    row = get_historical_for_date(dt)
                    rows_fetched[dt] = row
                    # decrement local available counter (not strictly needed but helpful)
                    available -= 1
            except Exception as e:
                logging.error("Error during fetch: %s", e)
                # Save partial results if any
                if rows_fetched:
                    logging.info("Saving partial results (%d rows).", len(rows_fetched))
                    existing_rows.update(rows_fetched)
                    write_csv_rows(path, existing_rows)
                return 1

            # Merge fetched rows with existing rows, dedupe, sort & write
            existing_rows.update(rows_fetched)
            write_csv_rows(path, existing_rows)
            logging.info("Saved %d rows to %s", len(rows_fetched), path)
    except Timeout:
        logging.error("Could not acquire lock on %s after %.1fs. Exiting.", lock_path, FILELOCK_TIMEOUT)
        return 1
    except Exception as e:
        logging.exception("Unexpected error while handling file lock: %s", e)
        return 1

    logging.info("Finished processing %04d-%02d", year, month)
    return 0


if __name__ == "__main__":
    sys.exit(main())
