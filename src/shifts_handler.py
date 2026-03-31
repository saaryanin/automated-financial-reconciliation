"""
Copyright 2026 Saar Yanin
Licensed under the Apache License 2.0 with Commons Clause
See LICENSE for details.

Script: shifts_handler.py
Description: This script processes unmatched shifted deposits that occur after a specified cutoff time for both ROW and UK regulations on a given date. It loads deposit matching files, filters for post-cutoff deposits, calculates the sum of matched amounts by currency using crm_amount, extracts and saves unmatched shifted rows in their raw CRM format by pulling from the original CRM files via transaction_ids, and updates the matching files by removing those shifted rows.

Key Features:
- Calculates the US Eastern Time-aware cutoff time (9 PM EDT or 10 PM EST) based on the date.
- Loads deposits_matching.xlsx for each regulation, converts crm_date to datetime for accurate filtering of post-cutoff unmatched deposits (match_status==0).
- Computes sums of crm_amount for matched shifted deposits (match_status==1) grouped by crm_currency, providing a dictionary of matched sums per regulation and currency.
- Extracts original unmatched shifted rows from raw CRM files using transaction_ids, with processor-specific extraction for IDs like skrill_id or neteller_id; saves these rows without additional computed columns to unmatched_shifted_deposits.xlsx.
- Removes the shifted unmatched rows from the original matching file by dropping corresponding indices and saves the updated file.
- Handles logging for key actions, warnings for missing files or columns (e.g., crm_date, crm_amount), and errors during file operations.
- Edge cases: Skips processing if matching file is missing or empty; ensures date consistency by using the file's extracted date; supports both ROW and UK regulations independently.

Dependencies:
- logging (for detailed action and error logging)
- datetime and timedelta (for date and cutoff calculations)
- pandas (for DataFrame loading, filtering, grouping, and saving)
- relativedelta (from dateutil) (for potential date adjustments, though not directly used in core logic)
- src.config (for setup_dirs_for_reg and BASE_DIR constants)
- src.preprocess (for extract_crm_transaction_id function)
- src.utils (for categorize_regulation function)
"""
import logging
from datetime import datetime
import pandas as pd
from dateutil.relativedelta import relativedelta
from src.config import setup_dirs_for_reg, BASE_DIR  # Import BASE_DIR for CRM_DIR
from src.preprocess import extract_crm_transaction_id  # Adjusted import based on your file name
from src.utils import categorize_regulation
from zoneinfo import ZoneInfo

# Define shared CRM_DIR
CRM_DIR = BASE_DIR / "data" / "crm_reports"

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def is_us_dst(date):
    """Determine if the given date is in US Daylight Saving Time (EDT)."""
    # Create a naive date at noon and attach US Eastern Time
    dt = datetime(date.year, date.month, date.day, 12, tzinfo=ZoneInfo("America/New_York"))
    return bool(dt.dst())  # True = summer time (EDT), False = winter time (EST)


def get_cutoff_time(date_str):
    """Get the cutoff time for the given date — now follows US DST (21:00 EDT / 22:00 EST)."""
    date = datetime.strptime(date_str, "%Y-%m-%d")
    cutoff_hour = 21 if is_us_dst(date) else 22  # 9 PM summer / 10 PM winter
    cutoff = date.replace(hour=cutoff_hour, minute=0, second=0, microsecond=0)
    return cutoff


def load_deposits_matching(date_str, regulation):
    """Load the deposits_matching.xlsx file for the given date and regulation."""
    dirs = setup_dirs_for_reg(regulation.lower())
    if regulation.lower() == "uk":
        matching_path = dirs["lists_dir"] / date_str / "uk_deposits_matching.xlsx"
    else:  # 'row'
        matching_path = dirs["lists_dir"] / date_str / "row_deposits_matching.xlsx"
    if not matching_path.exists():
        logging.error(
            f"Deposits matching file not found for {regulation} on {date_str}"
        )
        return pd.DataFrame()
    df = pd.read_excel(matching_path)
    if "crm_date" not in df.columns:
        logging.error(f"No 'crm_date' column in {regulation} deposits_matching file")
        return pd.DataFrame()
    df["crm_date"] = pd.to_datetime(df["crm_date"], errors="coerce")
    df = df.dropna(subset=["crm_date"])
    logging.info(f"Columns in {matching_path}: {df.columns.tolist()}")  # Add for debugging
    return df


def filter_shifted_deposits(df, cutoff):
    """Filter deposits after the cutoff time."""
    return df[df["crm_date"] > cutoff]


def calculate_matched_sum(shifted_df):
    """Calculate the sum of matched shifted deposits by crm_currency using crm_amount."""
    if (
        "crm_date" not in shifted_df.columns
        or "match_status" not in shifted_df.columns
        or "crm_currency" not in shifted_df.columns
        or "crm_amount" not in shifted_df.columns
    ):
        logging.warning(
            "Required columns (crm_date, match_status, crm_currency, crm_amount) not found"
        )
        return {}
    if shifted_df.empty:
        logging.info("Shifted DF is empty, returning empty dict")
        return {}
    min_date = shifted_df["crm_date"].min()
    if pd.isna(min_date):
        logging.warning("Min crm_date is NaT, skipping calculation")
        return {}
    min_date_str = min_date.strftime("%Y-%m-%d")
    cutoff = get_cutoff_time(min_date_str)
    matched_shifted = shifted_df[
        (shifted_df["match_status"] == 1) & (shifted_df["crm_date"] > cutoff)
    ]
    if matched_shifted.empty:
        logging.info("No matched shifted deposits found")
        return {}
    matched_sum = matched_shifted.groupby("crm_currency")["crm_amount"].sum().to_dict()
    return matched_sum  # Return only the currency sums dictionary


def update_matching_file(matching_path, shifted_unmatched_ids):
    """Update the matching file by removing rows with crm_transaction_id in shifted_unmatched_ids."""
    if not shifted_unmatched_ids:
        logging.info("No shifted unmatched IDs to remove from matching file")
        return
    df = pd.read_excel(matching_path)
    if "crm_transaction_id" not in df.columns:
        logging.warning(
            "'crm_transaction_id' column not found in matching file; skipping update"
        )
        return
    # Remove rows where crm_transaction_id is in shifted_unmatched_ids
    updated_df = df[~df["crm_transaction_id"].isin(shifted_unmatched_ids)]
    # Save back to the same path
    updated_df.to_excel(matching_path, index=False)
    logging.info(
        f"Updated matching file {matching_path} by removing {len(shifted_unmatched_ids)} shifted unmatched rows"
    )


def save_unmatched_shifted(shifted_df, date_str, regulation):
    """Save unmatched shifted deposits in the raw CRM file format using transaction IDs and update matching file."""
    if shifted_df.empty:
        logging.info("No unmatched shifted deposits to save")
        return []
    unmatched_shifted = shifted_df[shifted_df["match_status"] == 0]
    if unmatched_shifted.empty:
        logging.info("No unmatched shifted deposits to save")
        return []
    # Extract transaction_ids from unmatched shifted shifted deposits (crm_transaction_id column)
    transaction_ids = [
        tid
        for tid in unmatched_shifted["crm_transaction_id"]
        if pd.notna(tid)
    ]
    # Load raw CRM file (shared)
    crm_file = CRM_DIR / f"crm_{date_str}.xlsx"
    if not crm_file.exists():
        logging.error(f"Raw CRM file not found for {date_str}")
        dirs = setup_dirs_for_reg(regulation.lower())
        unmatched_path = (
            dirs["lists_dir"]
            / date_str
            / f"{regulation.lower()}_unmatched_shifted_deposits.xlsx"
        )
        unmatched_shifted.to_excel(unmatched_path, index=False)
        return transaction_ids
    crm_df = pd.read_excel(crm_file)
    crm_df.columns = crm_df.columns.str.strip()
    crm_df["regulation"] = crm_df["Site (Account) (Account)"].apply(categorize_regulation)
    # Filter raw CRM for Name == "Deposit" and specific regulation
    deposit_df = crm_df[crm_df["Name"] == "Deposit"]
    if regulation.lower() == "uk":
        deposit_df = deposit_df[deposit_df["regulation"] == "uk"]
    else:  # 'row'
        row_regs = ["mauritius", "cyprus", "australia"]
        deposit_df = deposit_df[deposit_df["regulation"].isin(row_regs)]
    # Extract transaction_id from Internal Comment using the processor from deposits_matching
    processor_name = (
        unmatched_shifted["crm_processor_name"].iloc[0].lower()
        if "crm_processor_name" in unmatched_shifted.columns
        and not unmatched_shifted.empty
        else "crm"
    )
    deposit_df.loc[:, "transaction_id"] = deposit_df["Internal Comment"].apply(
        lambda x: extract_crm_transaction_id(x, processor_name) if pd.notna(x) else None
    )
    # Match transaction_ids
    unmatched_crm_rows = deposit_df[
        deposit_df["transaction_id"].isin(transaction_ids)
    ]
    if unmatched_crm_rows.empty:
        logging.warning("No matching CRM rows found for unmatched shifted deposits")
        dirs = setup_dirs_for_reg(regulation.lower())
        unmatched_path = (
            dirs["lists_dir"]
            / date_str
            / f"{regulation.lower()}_unmatched_shifted_deposits.xlsx"
        )
        unmatched_shifted.to_excel(unmatched_path, index=False)
        return transaction_ids
    # Remove unwanted columns
    columns_to_remove = [
        "(Do Not Modify) Monetary Transaction",
        "(Do Not Modify) Row Checksum",
        "(Do Not Modify) Modified On",
        "transaction_id",
    ]
    unmatched_crm_rows = unmatched_crm_rows.drop(
        columns=[
            col for col in columns_to_remove if col in unmatched_crm_rows.columns
        ]
    )
    # Drop computed columns like 'regulation' before saving
    unmatched_crm_rows = unmatched_crm_rows.drop(columns=["regulation"], errors="ignore")
    # Save in the raw CRM format without modification
    dirs = setup_dirs_for_reg(regulation.lower())
    unmatched_path = (
        dirs["lists_dir"]
        / date_str
        / f"{regulation.lower()}_unmatched_shifted_deposits.xlsx"
    )
    unmatched_crm_rows = unmatched_crm_rows.drop(columns=["regulation"], errors="ignore")
    unmatched_crm_rows.to_excel(unmatched_path, index=False)
    logging.info(f"Unmatched shifted deposits saved to {unmatched_path} in raw CRM format")
    # Update the matching file to remove these rows
    matching_path = (
        dirs["lists_dir"]
        / date_str
        / f"{regulation.lower()}_deposits_matching.xlsx"
    )
    update_matching_file(matching_path, transaction_ids)
    return transaction_ids


def main(date_str):
    """Main function to process unmatched shifted deposits for a given date, for both ROW and UK."""
    cutoff = get_cutoff_time(date_str)
    # Process UK
    uk_deposits_df = load_deposits_matching(date_str, "uk")
    uk_shifted_deposits = (
        filter_shifted_deposits(uk_deposits_df, cutoff)
        if not uk_deposits_df.empty
        else pd.DataFrame()
    )
    uk_matched_sums = calculate_matched_sum(uk_shifted_deposits)
    save_unmatched_shifted(uk_shifted_deposits, date_str, "uk")
    # Process ROW
    row_deposits_df = load_deposits_matching(date_str, "row")
    row_shifted_deposits = (
        filter_shifted_deposits(row_deposits_df, cutoff)
        if not row_deposits_df.empty
        else pd.DataFrame()
    )
    row_matched_sums = calculate_matched_sum(row_shifted_deposits)
    save_unmatched_shifted(row_shifted_deposits, date_str, "row")
    # Combine matched sums or return separately
    matched_sums = {"uk": uk_matched_sums, "row": row_matched_sums}
    return matched_sums  # Return dict with regulation keys