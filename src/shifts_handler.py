# shifts_handler.py

import logging
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
from dateutil.relativedelta import relativedelta

from config import LISTS_DIR

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def is_bst():
    """Determine if the current date is in BST (British Summer Time)."""
    now = datetime.now()
    bst_start = now.replace(month=3, day=31) - relativedelta(days=now.replace(month=3, day=31).weekday())
    bst_end = now.replace(month=10, day=31) - relativedelta(days=now.replace(month=10, day=31).weekday())
    return bst_start <= now <= bst_end

def get_cutoff_time(date_str):
    """Get the cutoff time for the given date."""
    cutoff_hour = 21 if is_bst() else 22  # 9 PM BST or 10 PM GMT
    cutoff = datetime.strptime(f"{date_str} {cutoff_hour}:00", '%Y-%m-%d %H:%M')
    return cutoff

def load_deposits_matching(date_str):
    """Load the deposits_matching.xlsx file for the given date."""
    matching_path = LISTS_DIR / date_str / 'deposits_matching.xlsx'
    if not matching_path.exists():
        logging.error(f"Deposits matching file not found for {date_str}")
        return pd.DataFrame()
    df = pd.read_excel(matching_path)
    if 'Date' not in df.columns:
        logging.error("No 'Date' column in deposits_matching file")
        return pd.DataFrame()
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df = df.dropna(subset=['Date'])
    return df

def filter_shifted_deposits(df, cutoff):
    """Filter deposits after the cutoff time."""
    return df[df['Date'] > cutoff]

def calculate_matched_sum(shifted_df):
    """Calculate the sum of matched shifted deposits by currency."""
    matched_shifted = shifted_df[shifted_df['match_status'] == 1]  # Assuming 'match_status' column
    matched_sum = matched_shifted.groupby('Currency')['Amount'].sum()  # Assuming 'Currency' and 'Amount' columns
    logging.info("Sum of matched shifted deposits:")
    for currency, amount in matched_sum.items():
        logging.info(f"{currency}: {amount}")
    return matched_sum

def save_unmatched_shifted(shifted_df, date_str):
    """Save unmatched shifted deposits to a file."""
    unmatched_shifted = shifted_df[shifted_df['match_status'] == 0]  # Assuming 'match_status' column
    if unmatched_shifted.empty:
        logging.info("No unmatched shifted deposits to save")
        return
    unmatched_path = LISTS_DIR / date_str / 'unmatched_shifted_deposits.xlsx'
    unmatched_shifted.to_excel(unmatched_path, index=False)
    logging.info(f"Unmatched shifted deposits saved to {unmatched_path}")

def main(date_str):
    """Main function to process unmatched shifted deposits."""
    cutoff = get_cutoff_time(date_str)
    deposits_df = load_deposits_matching(date_str)
    if deposits_df.empty:
        return
    shifted_deposits = filter_shifted_deposits(deposits_df, cutoff)
    calculate_matched_sum(shifted_deposits)
    save_unmatched_shifted(shifted_deposits, date_str)

if __name__ == "__main__":
    # For standalone testing; in practice, call from reports_creator.py
    date_str = datetime.now().strftime('%Y-%m-%d')  # Example date
    main(date_str)