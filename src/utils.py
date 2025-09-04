import pandas as pd
import logging
from pathlib import Path
import re
import requests
import json
from datetime import datetime

def setup_logger(name, level=logging.INFO):
    """Create and return a logger with the specified name and level."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        ch = logging.StreamHandler()
        ch.setLevel(level)
        ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
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
    """Standardize currency strings."""
    if isinstance(cur, str):
        return cur.replace('US Dollar', 'USD').upper().strip()
    return cur

def create_cancelled_row(row):
    return {
        'crm_date': row.get('crm_date', None),
        'crm_email': row.get('crm_email', ''),
        'crm_firstname': row.get('crm_firstname', ''),
        'crm_lastname': row.get('crm_lastname', ''),
        'crm_last4': row.get('crm_last4', ''),
        'crm_currency': row.get('crm_currency', ''),
        'crm_amount': row.get('crm_amount', 0),
        'crm_processor_name': row.get('crm_processor_name', ''),
        'proc_date': None,
        'proc_email': None,
        'proc_firstname': None,
        'proc_lastname': None,
        'proc_last4': None,
        'proc_currency': None,
        'proc_amount': None,
        'proc_amount_crm_currency': None,
        'proc_processor_name': None,
        'email_similarity_avg': 0,
        'last4_match': False,
        'name_fallback_used': False,
        'exact_match_used': False,
        'match_status': 0,
        'payment_status': 0,
        'comment': 'Withdrawal cancelled with no matching withdrawal found',
        'matched_proc_indices': []
    }


def drop_cols(df, cols):
    """Drop columns from df if they exist."""
    return df.drop(columns=cols, errors='ignore')

def normalize_string(value, is_last4=False):
    """Normalize a string field by converting to string, stripping, and optionally formatting as last4."""
    if pd.isna(value):
        return ''
    value = str(value).strip()
    if value.endswith('.0'):
        value = value[:-2]
    if is_last4 and value.isdigit():
        value = value.zfill(4)  # Zero-pad last4 to 4 digits
    return value.lower() if not is_last4 else value

def clean_amount(val):
    """Convert accounting-style amounts like '(100.00)' to -100.00, or plain strings to numbers."""
    s = str(val).replace(',', '').strip()
    if re.match(r'^\(\s*-?[\d,\.]+\s*\)$', s):  # Handles (100.00) or (-100.00)
        s = s.strip('()')
        try:
            return -float(s)
        except ValueError:
            return None
    try:
        return float(s)
    except ValueError:
        return None

def clean_last4(v):
    if pd.isna(v):
        return ''
    try:
        return str(int(float(v))).zfill(4)
    except ValueError:
        return str(v).strip()


# Path for caching fetched holidays (in data/ dir, assuming DATA_DIR from config)
HOLIDAYS_CACHE_FILE = Path(
    'data/uk_holidays_cache.json')  # Adjust to full path if needed, e.g., DATA_DIR / 'uk_holidays_cache.json'


def fetch_uk_holidays_from_api(division='england-and-wales'):
    """Fetch UK bank holidays from GOV.UK API and return dates for the specified division."""
    url = 'https://www.gov.uk/bank-holidays.json'
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        if division in data:
            return [event['date'] for event in data[division]['events']]
        else:
            logging.error(f"Division '{division}' not found in API response")
            return []
    except requests.RequestException as e:
        logging.error(f"Error fetching UK holidays from API: {e}")
        return []


def load_uk_holidays(use_cache=True):
    """Load UK holidays: from cache if exists/use_cache=True, else fetch from API and cache."""
    if use_cache and HOLIDAYS_CACHE_FILE.exists():
        with open(HOLIDAYS_CACHE_FILE, 'r') as f:
            data = json.load(f)
            # Check if cache is recent (e.g., <1 year old); refetch if not
            cache_date = datetime.fromisoformat(data.get('last_fetched', '1900-01-01'))
            if (datetime.now() - cache_date).days < 365:
                logging.info("Loaded UK holidays from cache")
                return data['holidays']

    logging.info("Fetching fresh UK holidays from API")
    holidays = fetch_uk_holidays_from_api()
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
