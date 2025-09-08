# shifts_handler.py

import logging
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
from dateutil.relativedelta import relativedelta
import numpy as np  # Added for np.nan

from src.config import LISTS_DIR, CRM_DIR
from src.preprocess import extract_crm_transaction_id  # Import the global function

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
    if shifted_df.empty:
        logging.info("Shifted DF is empty, returning empty dict")
        return {}
    min_date = shifted_df['crm_date'].min()
    if pd.isna(min_date):
        logging.warning("Min crm_date is NaT, skipping calculation")
        return {}
    min_date_str = min_date.strftime('%Y-%m-%d')
    cutoff = get_cutoff_time(min_date_str)
    matched_shifted = shifted_df[(shifted_df['match_status'] == 1) & (shifted_df['crm_date'] > cutoff)]
    if matched_shifted.empty:
        logging.info("No matched shifted deposits found")
        return {}
    matched_sum = matched_shifted.groupby('crm_currency')['crm_amount'].sum().to_dict()
    return matched_sum  # Return only the currency sums dictionary

def save_unmatched_shifted(shifted_df, date_str):
    """Save unmatched shifted deposits in the raw CRM file format using transaction IDs."""
    if shifted_df.empty:
        logging.info("No unmatched shifted deposits to save")
        return

    unmatched_shifted = shifted_df[shifted_df['match_status'] == 0]
    if unmatched_shifted.empty:
        logging.info("No unmatched shifted deposits to save")
        return

    # Extract transaction_ids from unmatched shifted deposits (crm_transaction_id column)
    transaction_ids = [tid for tid in unmatched_shifted['crm_transaction_id'] if pd.notna(tid)]

    # Load raw CRM file
    crm_file = CRM_DIR / f"crm_{date_str}.xlsx"
    if not crm_file.exists():
        logging.error(f"Raw CRM file not found for {date_str}")
        unmatched_shifted.to_excel(LISTS_DIR / date_str / 'unmatched_shifted_deposits.xlsx', index=False)
        return

    crm_df = pd.read_excel(crm_file)

    # Filter raw CRM for Name == "Deposit"
    deposit_df = crm_df[crm_df['Name'] == 'Deposit']

    # Extract transaction_id from Internal Comment using the processor from deposits_matching
    processor_name = unmatched_shifted['crm_processor_name'].iloc[0].lower() if 'crm_processor_name' in unmatched_shifted.columns and not unmatched_shifted.empty else 'crm'
    deposit_df.loc[:, 'transaction_id'] = deposit_df['Internal Comment'].apply(lambda x: extract_crm_transaction_id(x, processor_name) if pd.notna(x) else None)

    # Match transaction_ids
    unmatched_crm_rows = deposit_df[deposit_df['transaction_id'].isin(transaction_ids)]

    if unmatched_crm_rows.empty:
        logging.warning("No matching CRM rows found for unmatched shifted deposits")
        unmatched_shifted.to_excel(LISTS_DIR / date_str / 'unmatched_shifted_deposits.xlsx', index=False)
        return

    # Remove unwanted columns
    columns_to_remove = [
        "(Do Not Modify) Monetary Transaction",
        "(Do Not Modify) Row Checksum",
        "(Do Not Modify) Modified On",
        "transaction_id"
    ]
    unmatched_crm_rows = unmatched_crm_rows.drop(columns=[col for col in columns_to_remove if col in unmatched_crm_rows.columns])

    # Save in the raw CRM format without modification
    unmatched_path = LISTS_DIR / date_str / 'unmatched_shifted_deposits.xlsx'
    unmatched_crm_rows.to_excel(unmatched_path, index=False)
    logging.info(f"Unmatched shifted deposits saved to {unmatched_path} in raw CRM format")

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