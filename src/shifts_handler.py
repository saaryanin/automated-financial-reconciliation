# shifts_handler.py

import logging
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
from dateutil.relativedelta import relativedelta

from config import LISTS_DIR

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def is_bst(date):
    """Determine if the given date is in BST (British Summer Time)."""
    bst_start = date.replace(month=3, day=31) - relativedelta(days=date.replace(month=3, day=31).weekday())
    bst_end = date.replace(month=10, day=31) - relativedelta(days=date.replace(month=10, day=31).weekday())
    return bst_start <= date <= bst_end

def get_cutoff_time(date_str):
    """Get the cutoff time for the given date."""
    date = datetime.strptime(date_str, '%Y-%m-%d')
    cutoff_hour = 21 if is_bst(date) else 22  # 9 PM BST or 10 PM GMT
    cutoff = date.replace(hour=cutoff_hour, minute=0, second=0, microsecond=0)
    return cutoff

def load_deposits_matching(date_str):
    """Load the deposits_matching.xlsx file for the given date."""
    matching_path = LISTS_DIR / date_str / 'deposits_matching.xlsx'
    if not matching_path.exists():
        logging.error(f"Deposits matching file not found for {date_str}")
        return pd.DataFrame()
    df = pd.read_excel(matching_path)
    if 'crm_date' not in df.columns:
        logging.error("No 'crm_date' column in deposits_matching file")
        return pd.DataFrame()
    df['crm_date'] = pd.to_datetime(df['crm_date'], errors='coerce')
    df = df.dropna(subset=['crm_date'])
    logging.info(f"Columns in {matching_path}: {df.columns.tolist()}")  # Add for debugging
    return df

def filter_shifted_deposits(df, cutoff):
    """Filter deposits after the cutoff time."""
    return df[df['crm_date'] > cutoff]

def calculate_matched_sum(shifted_df):
    """Calculate the sum of matched shifted deposits by crm_currency using crm_amount."""
    if 'crm_date' not in shifted_df.columns or 'match_status' not in shifted_df.columns or 'crm_currency' not in shifted_df.columns or 'crm_amount' not in shifted_df.columns:
        logging.warning("Required columns (crm_date, match_status, crm_currency, crm_amount) not found")
        return {}
    matched_shifted = shifted_df[(shifted_df['match_status'] == 1) & (shifted_df['crm_date'] > get_cutoff_time(shifted_df['crm_date'].min().strftime('%Y-%m-%d')))]
    if matched_shifted.empty:
        logging.info("No matched shifted deposits found")
        return {}
    matched_sum = matched_shifted.groupby('crm_currency')['crm_amount'].sum().to_dict()
    return matched_sum  # Return only the currency sums dictionary

def save_unmatched_shifted(shifted_df, date_str):
    """Save unmatched shifted deposits to a file in the dated folder."""
    unmatched_shifted = shifted_df[shifted_df['match_status'] == 0]
    if unmatched_shifted.empty:
        logging.info("No unmatched shifted deposits to save")
        return
    unmatched_path = LISTS_DIR / date_str / 'unmatched_shifted_deposits.xlsx'
    unmatched_shifted.to_excel(unmatched_path, index=False)
    logging.info(f"Unmatched shifted deposits saved to {unmatched_path}")

def main(date_str):
    """Main function to process unmatched shifted deposits for a given date."""
    cutoff = get_cutoff_time(date_str)
    deposits_df = load_deposits_matching(date_str)
    if deposits_df.empty:
        return None  # Return None if no data
    shifted_deposits = filter_shifted_deposits(deposits_df, cutoff)
    matched_sums = calculate_matched_sum(shifted_deposits)
    save_unmatched_shifted(shifted_deposits, date_str)
    return matched_sums  # Return only the matched sums