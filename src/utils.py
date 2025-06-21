import pandas as pd
import logging
from pathlib import Path

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
    """Build output dict for a 'Withdrawal Cancelled' CRM row."""
    return {
        'crm_date': row['Created On'],
        'crm_email': row['Email (Account) (Account)'],
        'crm_firstname': row.get('First Name (Account) (Account)', ''),
        'crm_lastname': row.get('Last Name (Account) (Account)', ''),
        'crm_tp': row.get('tp', ''),
        'crm_last4': str(row.get('CC Last 4 Digits', '')).zfill(4),
        'crm_currency': row.get('Currency', ''),
        'crm_amount': -abs(float(row['Amount'])),
        'crm_processor_name': row.get('PSP name', ''),
        'proc_dates': [],
        'proc_emails': [],
        'proc_firstnames': [],
        'proc_lastnames': [],
        'proc_last4_digits': [],
        'proc_currencies': [],
        'proc_total_amounts': [],
        'proc_processor_name': '',
        'converted_amount_total': None,
        'exchange_rates': [],
        'email_similarity_avg': None,
        'last4_match': False,
        'name_fallback_used': False,
        'exact_match_used': False,
        'converted': False,
        'combo_len': 0,
        'match_status': 0,
        'payment_status': 0,
        'comment': "Withdrawal Cancellation",
    }

def drop_cols(df, cols):
    """Drop columns from df if they exist."""
    return df.drop(columns=cols, errors='ignore')
