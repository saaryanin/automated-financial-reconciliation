"""
Copyright 2026 Saar Yanin
Licensed under the Apache License 2.0 with Commons Clause
See LICENSE for details.

Script: utils.py
Description: This utility module provides a collection of helper functions for data cleaning, logging setup, file handling, date manipulation, and API interactions used throughout the financial reconciliation project. It supports tasks such as normalizing strings and currencies, loading and concatenating data files safely, fetching and caching UK bank holidays, categorizing regulations based on site names, extracting dates from filenames, and calculating previous business days while skipping weekends and holidays.

Key Features:
- Data cleaning: Functions like clean_field, normalize_string, clean_amount, clean_last4, and normalize_currency handle string stripping, quote removal, negative amount parsing in accounting format, zero-padding for last4 digits, and currency standardization (e.g., 'US Dollar' to 'USD').
- Logging: setup_logger configures a logger with stream handler for consistent logging across scripts.
- File handling: load_excel_if_exists and load_csv_if_exists load files only if they exist; safe_concat concatenates non-empty DataFrames to avoid errors with empty data.
- Specialized row creation: create_cancelled_row generates a dictionary for cancelled withdrawal entries with default values.
- Column management: drop_cols safely drops columns if they exist, ignoring errors.
- API and caching: fetch_uk_holidays_from_api retrieves UK bank holidays from GOV.UK API; load_uk_holidays uses caching (JSON file) to avoid repeated API calls, with a one-year freshness check.
- Regulation categorization: categorize_regulation maps site names to regulatory categories (e.g., 'uk', 'row', 'mauritius') with fallback to 'unknown'.
- Date utilities: extract_date_from_filename parses dates from various filename formats (e.g., YYYY-MM-DD, DD.MM.YYYY, DD_MM_YYYY); get_previous_business_day computes the prior business day, skipping weekends and cached holidays, with logging for skipped dates.
- Edge cases: Handles non-string inputs, NaN values, lists in cleaning functions; robust error handling in API fetches and date parsing; supports multiple date formats in filenames.

Dependencies:
- pandas (for DataFrame operations)
- logging (for logger setup)
- pathlib (for path handling)
- re (for regular expressions in date extraction and amount cleaning)
- requests (for API calls to fetch holidays)
- json (for caching holiday data)
- datetime and timedelta (for date calculations)
- numpy (for handling arrays and NaN checks)
"""
import pandas as pd
import logging
from pathlib import Path
import re
import requests
import json
from datetime import datetime, timedelta
import numpy as np


def clean_field(s):
    if isinstance(s, list):
        if not s:
            return None
        s = s[0]  # Take first item if list
    if not isinstance(s, str):
        return s
    s = s.strip()
    if s.startswith("[") and s.endswith("]"):
        s = s[1:-1].strip()
    if s.startswith("'") and s.endswith("'"):
        s = s[1:-1]
    elif s.startswith('"') and s.endswith('"'):
        s = s[1:-1]
    return s


def setup_logger(name, level=logging.INFO):
    """Create and return a logger with the specified name and level."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        ch = logging.StreamHandler()
        ch.setLevel(level)
        ch.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        logger.addHandler(ch)
    logger.setLevel(level)
    return logger


def load_excel_if_exists(filepath):
    """Load Excel file if it exists, else return None."""
    if Path(filepath).exists():
        return pd.read_excel(filepath)
    return None


def load_csv_if_exists(filepath):
    """Load CSV file if it exists, else return None."""
    if Path(filepath).exists():
        return pd.read_csv(filepath)
    return None


def safe_concat(dfs, **kwargs):
    """Concatenate non-empty DataFrames."""
    dfs = [df for df in dfs if df is not None and not df.empty]
    if dfs:
        return pd.concat(dfs, **kwargs)
    return pd.DataFrame()


def normalize_currency(cur):
    """Standardize currency strings like 'US Dollar' to 'USD'."""
    if isinstance(cur, str):
        return cur.replace("US Dollar", "USD").upper().strip()
    return cur


def create_cancelled_row(row):
    return {
        "crm_date": row.get("crm_date", None),
        "crm_email": row.get("crm_email", ""),
        "crm_firstname": row.get("crm_firstname", ""),
        "crm_lastname": row.get("crm_lastname", ""),
        "crm_last4": row.get("crm_last4", ""),
        "crm_currency": row.get("crm_currency", ""),
        "crm_amount": row.get("crm_amount", 0),
        "crm_processor_name": row.get("crm_processor_name", ""),
        "proc_date": None,
        "proc_email": None,
        "proc_firstname": None,
        "proc_lastname": None,
        "proc_last4": None,
        "proc_currency": None,
        "proc_amount": None,
        "proc_amount_crm_currency": None,
        "proc_processor_name": None,
        "email_similarity_avg": 0,
        "last4_match": False,
        "name_fallback_used": False,
        "exact_match_used": False,
        "match_status": 0,
        "payment_status": 0,
        "comment": "Withdrawal cancelled with no matching withdrawal found",
        "matched_proc_indices": [],
    }


def drop_cols(df, cols):
    """Drop columns from df if they exist."""
    return df.drop(columns=cols, errors="ignore")


def normalize_string(value, is_last4=False):
    """Normalize a string field by converting to string, stripping, and optionally formatting as last4."""
    if pd.isna(value):
        return ""
    value = str(value).strip()
    if value.endswith(".0"):
        value = value[:-2]
    if is_last4 and value.isdigit():
        value = value.zfill(4)  # Zero-pad last4 to 4 digits
    return value.lower() if not is_last4 else value


def clean_amount(val):
    """Convert accounting-style amounts like '(100.00)' to -100.00, or plain strings to numbers."""
    s = str(val).replace(",", "").strip()
    if re.match(r"^\(\s*-?[\d,\.]+\s*\)$", s):  # Handles (100.00) or (-100.00)
        s = s.strip("()")
        try:
            return -float(s)
        except ValueError:
            return None
    try:
        return float(s)
    except ValueError:
        return None


def clean_last4(v):
    if v is None:
        return ""
    if isinstance(v, (list, np.ndarray)):
        if len(v) == 0:
            return ""
        v = v[0]
    if pd.isna(v):
        return ""
    try:
        return str(int(float(v))).zfill(4)
    except (ValueError, TypeError):
        return str(v).strip()


# Path for caching fetched holidays (in data/ dir, assuming DATA_DIR from config)
HOLIDAYS_CACHE_FILE = Path(
    "data/uk_holidays_cache.json"
)  # Adjust to full path if needed, e.g., DATA_DIR / 'uk_holidays_cache.json'

# Module-level cache so disk is only read once per process
_holidays_cache = None


def fetch_uk_holidays_from_api(division="england-and-wales"):
    """Fetch UK bank holidays from GOV.UK API and return dates for the specified division."""
    url = "https://www.gov.uk/bank-holidays.json"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        if division in data:
            return [event["date"] for event in data[division]["events"]]
        else:
            logging.error(f"Division '{division}' not found in API response")
            return []
    except requests.RequestException as e:
        logging.error(f"Error fetching UK holidays from API: {e}")
        return []


def load_uk_holidays(use_cache=True):
    """Load UK holidays: from in-memory cache first, then disk cache, then API."""
    global _holidays_cache
    if _holidays_cache is not None:
        return _holidays_cache
    if use_cache and HOLIDAYS_CACHE_FILE.exists():
        with open(HOLIDAYS_CACHE_FILE, "r") as f:
            data = json.load(f)
            # Check if cache is recent (e.g., <1 year old); refetch if not
            cache_date = datetime.fromisoformat(data.get("last_fetched", "1900-01-01"))
            if (datetime.now() - cache_date).days < 365:
                logging.info("Loaded UK holidays from cache")
                _holidays_cache = data["holidays"]
                return _holidays_cache
    logging.info("Fetching fresh UK holidays from API")
    holidays = fetch_uk_holidays_from_api()
    if holidays:
        cache_data = {"last_fetched": datetime.now().isoformat(), "holidays": holidays}
        HOLIDAYS_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(HOLIDAYS_CACHE_FILE, "w") as f:
            json.dump(cache_data, f)
        logging.info(f"Cached UK holidays to {HOLIDAYS_CACHE_FILE}")
    _holidays_cache = holidays
    return _holidays_cache


_SITE_TO_REG = {
    "fortrade.by": "belarus",
    "gcmasia by": "belarus",
    "kapitalrs by": "belarus",
    "kapitalrs au": "australia",
    "fortrade.au": "australia",
    "gcmasia asic": "australia",
    "fortrade.eu": "mauritius",
    "gcmforex": "mauritius",
    "gcmasia fsc": "mauritius",
    "fortrade fsc": "mauritius",
    "kapitalrs fsc": "mauritius",
    "fortrade dfsa": "dubai",
    "fortrade dsfa": "dubai",
    "fortrade.ca": "canada",
    "fortrade.cy": "cyprus",
    "fortrade.com": "uk",
    "kapitalrs": "uk",
}

_KNOWN_REGS = {"uk", "row", "mauritius", "cyprus", "australia", "belarus", "canada", "unknown", "dubai"}


def categorize_regulation(site):
    site = str(site).lower().strip()
    if site in _KNOWN_REGS:
        return site
    return _SITE_TO_REG.get(site, "unknown")


def extract_date_from_filename(filepath: str) -> str:
    match = re.search(r"(\d{4}-\d{2}-\d{2})", filepath)
    if match:
        return match.group(1)
    match_alt = re.search(r"(\d{2}\.\d{2}\.\d{4})", filepath)
    if match_alt:
        return datetime.strptime(match_alt.group(1), "%d.%m.%Y").strftime("%Y-%m-%d")
    match_slash = re.search(r"(\d{2}_\d{2}_\d{4})", filepath)
    if match_slash:
        return datetime.strptime(match_slash.group(1), "%d_%m_%Y").strftime("%Y-%m-%d")
    return "unknown_date"


def get_previous_business_day(current_date_str):
    current_date = datetime.strptime(current_date_str, "%Y-%m-%d")
    prev_date = current_date - timedelta(days=1)
    holidays = set(load_uk_holidays())
    skipped_dates = []  # Track skipped for logging
    while prev_date.weekday() >= 5 or prev_date.strftime("%Y-%m-%d") in holidays:
        skipped_dates.append(prev_date.strftime("%Y-%m-%d"))  # Log skipped date
        prev_date -= timedelta(days=1)
    if skipped_dates:
        logging.info(f"Skipped dates for {current_date_str}: {skipped_dates} (weekends/holidays)")
    else:
        logging.info(
            f"No skips for {current_date_str}; using direct previous: {prev_date.strftime('%Y-%m-%d')}"
        )
    return prev_date.strftime("%Y-%m-%d")