# raw_processor_renamer.py

import re
from pathlib import Path
from datetime import datetime
from shutil import move
import pandas as pd
import logging
from src.config import PROCESSOR_DIR, RAW_ATTACHED_FILES

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Directory for incoming raw files
INCOMING_DIR = RAW_ATTACHED_FILES

# Processor patterns dictionary
PROCESSOR_PATTERNS = {
    "safecharge": {
        "pattern": r"126728__transaction-search_(\d{8})(_[a-z0-9]+)?(\.csv|\.xlsx|\.xls)?",
        "date_format": "%Y%m%d",
        "type_group": None,
        "date_column": None
    },
    "bitpay": {
        "pattern": r"bitpay-export-[a-z]{3}-(\d{1,2}-\d{1,2}-\d{4})-_to_\d{1,2}-\d{1,2}-\d{4}(\.csv|\.xlsx|\.xls)?",
        "date_format": "%m-%d-%Y",
        "type_group": None,
        "date_column": None
    },
    "ezeebill": {
        "pattern": r"daily_transaction_report_(\d{4}-\d{2}-\d{2})_to_\d{4}-\d{2}-\d{2}(\.csv|\.xlsx|\.xls)?",
        "date_format": "%Y-%m-%d",
        "type_group": None,
        "date_column": None
    },
    "paypal": {
        "pattern": r"download\s*-\s*(\d{4}-\d{2}-\d{2})T\d{6}\.\d{3}(\.csv|\.xlsx|\.xls)?",
        "date_format": "%Y-%m-%d",
        "type_group": None,
        "date_column": None
    },
    "zotapay": {
        "pattern": r"export(\s*\(\d+\))?\.csv",
        "date_format": None,
        "type_group": None,
        "date_column": "Ended At"
    },
    "paymentasia": {
        "pattern": r"export_(transactions|payouts)_\d+(\.csv|\.xlsx|\.xls)?",
        "date_format": None,
        "type_group": 1,
        "date_column": "Completed Time"
    },
    "powercash": {
        "pattern": r"report-[a-zA-Z0-9]+(\.csv|\.xlsx|\.xls)?",
        "date_format": None,
        "type_group": None,
        "date_column": "Date"
    },
    "trustpayments": {
        "pattern": r"searchresults(\s*\(\d+\))?\.csv",
        "date_format": None,
        "type_group": None,
        "date_column": "transactionstartedtimestamp"
    },
    "paysafe": {  # Combined Skrill/Neteller
        "pattern": r"transactions_\d+(\.csv|\.xlsx|\.xls)?",
        "date_format": None,
        "type_group": None,
        "date_column": "Time (CET)"
    },
}

def extract_date_from_file(file_path: Path, date_column: str = None):
    """
    Extract date from the file content if no date in filename.
    - Returns YYYY-MM-DD or None if failed.
    - Logs available columns for debugging.
    """
    if not date_column:
        return None
    try:
        if file_path.suffix in ['.xls', '.xlsx']:
            try:
                df = pd.read_excel(file_path, engine='openpyxl')
            except Exception as e:
                logging.error(f"Initial read failed for {file_path} with openpyxl: {e}")
                df = pd.read_excel(file_path, engine='xlrd')  # Fallback engine
        else:
            df = pd.read_csv(file_path)
        logging.info(f"Columns in {file_path}: {df.columns.tolist()}")
        if date_column in df.columns:
            date_str = df[date_column].dropna().iloc[0]  # First non-null date
            return pd.to_datetime(date_str, errors='coerce').strftime('%Y-%m-%d')
        logging.warning(f"Column {date_column} not found in {file_path}")
        # Fallback for paysafe
        if "paysafe" in file_path.name and "Time (UTC)" in df.columns:
            date_str = df["Time (UTC)"].dropna().iloc[0]
            return pd.to_datetime(date_str, errors='coerce').strftime('%Y-%m-%d')
    except Exception as e:
        logging.error(f"Date extraction failed for {file_path}: {e}")
    return None

def rename_raw_file(file_path: Path):
    """
    Detect processor, extract date and type, rename, and move the file.
    - Returns True if renamed, False otherwise.
    """
    filename = file_path.name.lower()
    for processor, config in PROCESSOR_PATTERNS.items():
        match = re.match(config["pattern"], filename)
        if match:
            try:
                if config["date_format"]:
                    date_raw = match.group(1)
                    date_str = datetime.strptime(date_raw, config["date_format"]).strftime('%Y-%m-%d')
                else:
                    date_str = extract_date_from_file(file_path, config["date_column"])
                    if not date_str:
                        logging.warning(f"No date found for {filename}, skipping")
                        return False

                if processor == "paymentasia" and config["type_group"] and match.group(config["type_group"]):
                    type_str = 'deposits' if match.group(config["type_group"]) == 'transactions' else 'withdrawals'
                    new_name = f"{processor}_{type_str}_{date_str}{file_path.suffix}"
                elif processor == "paysafe":
                    new_name = f"{processor}_{date_str}{file_path.suffix}"
                else:
                    new_name = f"{processor}_{date_str}{file_path.suffix}"

                dest_path = PROCESSOR_DIR / new_name
                if dest_path.exists():
                    logging.warning(f"Destination {dest_path} exists, skipping {filename}")
                    return False
                move(str(file_path), str(dest_path))
                logging.info(f"Renamed {filename} to {new_name} for {processor}")
                return True
            except (ValueError, IndexError) as e:
                logging.error(f"Processing failed for {filename}: {e}")
                return False
    logging.warning(f"No pattern match for {filename}, leaving in {INCOMING_DIR}. Available patterns: {list(PROCESSOR_PATTERNS.keys())}")
    return False

def run_renamer(incoming_dir: Path = INCOMING_DIR):
    """
    Scan incoming_dir for raw files, rename, and move to PROCESSOR_DIR.
    """
    renamed_count = 0
    incoming_dir.mkdir(parents=True, exist_ok=True)
    for file in incoming_dir.glob("*.*"):
        if file.is_file() and file.suffix in ['.csv', '.xlsx', '.xls']:
            if rename_raw_file(file):
                renamed_count += 1
    logging.info(f"Renamed {renamed_count} files. Unrecognized files remain in {incoming_dir}.")

if __name__ == "__main__":
    run_renamer()