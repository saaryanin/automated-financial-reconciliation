"""Utility functions for financial reconciliation."""
import pandas as pd
import logging
import re
import requests
import json
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Any, Dict
from dataclasses import dataclass, asdict

# ============================================================================
# CONSTANTS
# ============================================================================

CURRENCY_NORMALIZATION = {
    'US Dollar': 'USD',
    'Euro': 'EUR',
    'Canadian Dollar': 'CAD',
    'Australian Dollar': 'AUD'
}

REGULATION_MAPPING = {
    'belarus': ['fortrade.by', 'gcmasia by', 'kapitalrs by'],
    'australia': ['kapitalrs au', 'fortrade.au', 'gcmasia asic'],
    'mauritius': ['fortrade.eu', 'gcmforex', 'gcmasia fsc', 'fortrade fsc', 'kapitalrs fsc'],
    'canada': ['fortrade.ca'],
    'cyprus': ['fortrade.cy']
}

HOLIDAYS_CACHE_FILE = Path('data/uk_holidays_cache.json')
CACHE_VALIDITY_DAYS = 365

# ============================================================================
# DATACLASSES
# ============================================================================

@dataclass
class CancelledRow:
    """Represents a cancelled withdrawal row."""
    crm_date: Optional[datetime] = None
    crm_email: str = ''
    crm_firstname: str = ''
    crm_lastname: str = ''
    crm_last4: str = ''
    crm_currency: str = ''
    crm_amount: float = 0.0
    crm_processor_name: str = ''
    proc_date: None = None
    proc_email: None = None
    proc_firstname: None = None
    proc_lastname: None = None
    proc_last4: None = None
    proc_currency: None = None
    proc_amount: None = None
    proc_amount_crm_currency: None = None
    proc_processor_name: None = None
    email_similarity_avg: float = 0.0
    last4_match: bool = False
    name_fallback_used: bool = False
    exact_match_used: bool = False
    match_status: int = 0
    payment_status: int = 0
    comment: str = 'Withdrawal cancelled with no matching withdrawal found'
    matched_proc_indices: List = None

    def __post_init__(self):
        if self.matched_proc_indices is None:
            self.matched_proc_indices = []

# ============================================================================
# LOGGER SETUP
# ============================================================================

def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Create and return a logger with the specified name and level."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setLevel(level)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger

# ============================================================================
# FILE LOADING
# ============================================================================

def load_excel_if_exists(filepath: Path) -> Optional[pd.DataFrame]:
    """Load Excel file if it exists, else return None."""
    if Path(filepath).exists():
        return pd.read_excel(filepath)
    return None

def load_csv_if_exists(filepath: Path) -> Optional[pd.DataFrame]:
    """Load CSV file if it exists, else return None."""
    if Path(filepath).exists():
        return pd.read_csv(filepath)
    return None

# ============================================================================
# DATAFRAME OPERATIONS
# ============================================================================

def safe_concat(dfs: List[pd.DataFrame], **kwargs) -> pd.DataFrame:
    """Concatenate non-empty DataFrames."""
    dfs = [df for df in dfs if df is not None and not df.empty]
    return pd.concat(dfs, **kwargs) if dfs else pd.DataFrame()

def drop_cols(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """Drop columns from df if they exist."""
    return df.drop(columns=cols, errors='ignore')

# ============================================================================
# STRING NORMALIZATION
# ============================================================================

def normalize_currency(cur: Any) -> str:
    """Standardize currency strings."""
    if isinstance(cur, str):
        normalized = CURRENCY_NORMALIZATION.get(cur, cur)
        return normalized.upper().strip()
    return str(cur) if cur is not None else ''

def normalize_string(value: Any, is_last4: bool = False) -> str:
    """Normalize a string field by converting to string, stripping, and optionally formatting as last4."""
    if pd.isna(value):
        return ''

    value = str(value).strip()

    # Remove trailing .0 from numeric strings
    if value.endswith('.0'):
        value = value[:-2]

    # Handle last4 zero-padding
    if is_last4 and value.isdigit():
        value = value.zfill(4)
        return value

    return value.lower() if not is_last4 else value

# ============================================================================
# AMOUNT CLEANING
# ============================================================================

def clean_amount(val: Any) -> Optional[float]:
    """Convert accounting-style amounts like '(100.00)' to -100.00, or plain strings to numbers."""
    s = str(val).replace(',', '').strip()

    # Handle parentheses (accounting format for negative)
    if re.match(r'^\(\s*-?[\d,\.]+\s*\)$', s):
        s = s.strip('()')
        try:
            return -float(s)
        except ValueError:
            return None

    try:
        return float(s)
    except ValueError:
        return None

def clean_last4(v: Any) -> str:
    """Clean and format last 4 digits of card number."""
    if pd.isna(v):
        return ''
    try:
        return str(int(float(v))).zfill(4)
    except ValueError:
        return str(v).strip()

# ============================================================================
# CANCELLED ROW CREATION
# ============================================================================

def create_cancelled_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Create a cancelled row dictionary from a CRM row."""
    cancelled = CancelledRow(
        crm_date=row.get('crm_date'),
        crm_email=row.get('crm_email', ''),
        crm_firstname=row.get('crm_firstname', ''),
        crm_lastname=row.get('crm_lastname', ''),
        crm_last4=row.get('crm_last4', ''),
        crm_currency=row.get('crm_currency', ''),
        crm_amount=row.get('crm_amount', 0),
        crm_processor_name=row.get('crm_processor_name', '')
    )
    return asdict(cancelled)

# ============================================================================
# UK HOLIDAYS
# ============================================================================

def fetch_uk_holidays_from_api(division: str = 'england-and-wales') -> List[str]:
    """Fetch UK bank holidays from GOV.UK API."""
    url = 'https://www.gov.uk/bank-holidays.json'
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        if division in data:
            return [event['date'] for event in data[division]['events']]

        logging.error(f"Division '{division}' not found in API response")
        return []
    except requests.RequestException as e:
        logging.error(f"Error fetching UK holidays from API: {e}")
        return []

def load_uk_holidays(use_cache: bool = True) -> List[str]:
    """Load UK holidays from cache or fetch from API."""
    # Check cache if enabled
    if use_cache and HOLIDAYS_CACHE_FILE.exists():
        with open(HOLIDAYS_CACHE_FILE, 'r') as f:
            data = json.load(f)
            cache_date = datetime.fromisoformat(data.get('last_fetched', '1900-01-01'))

            # Use cache if less than CACHE_VALIDITY_DAYS old
            if (datetime.now() - cache_date).days < CACHE_VALIDITY_DAYS:
                logging.info("Loaded UK holidays from cache")
                return data['holidays']

    # Fetch fresh data
    logging.info("Fetching fresh UK holidays from API")
    holidays = fetch_uk_holidays_from_api()

    # Cache the results
    if holidays:
        cache_data = {
            'last_fetched': datetime.now().isoformat(),
            'holidays': holidays
        }
        HOLIDAYS_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(HOLIDAYS_CACHE_FILE, 'w') as f:
            json.dump(cache_data, f)
        logging.info(f"Cached UK holidays to {HOLIDAYS_CACHE_FILE}")

    return holidays

# ============================================================================
# REGULATION CATEGORIZATION
# ============================================================================

def categorize_regulation(site: str) -> str:
    """Categorize site into regulation jurisdiction."""
    site = str(site).lower().strip()

    for regulation, sites in REGULATION_MAPPING.items():
        if site in sites:
            return regulation

    return 'unknown'

# ============================================================================
# DATE EXTRACTION
# ============================================================================

def extract_date_from_filename(filepath: str) -> str:
    """Extract date from filename in various formats."""
    # Try YYYY-MM-DD format
    match = re.search(r"(\d{4}-\d{2}-\d{2})", filepath)
    if match:
        return match.group(1)

    # Try DD.MM.YYYY format
    match_alt = re.search(r"(\d{2}\.\d{2}\.\d{4})", filepath)
    if match_alt:
        return datetime.strptime(match_alt.group(1), "%d.%m.%Y").strftime("%Y-%m-%d")

    # Try DD_MM_YYYY format
    match_slash = re.search(r"(\d{2}_\d{2}_\d{4})", filepath)
    if match_slash:
        return datetime.strptime(match_slash.group(1), "%d_%m_%Y").strftime("%Y-%m-%d")

    return "unknown_date"
