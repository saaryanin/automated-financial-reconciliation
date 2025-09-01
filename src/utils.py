import pandas as pd
import logging
from pathlib import Path
import re

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


