# raw_processor_renamer.py

import re
from pathlib import Path
from datetime import datetime
from shutil import move
import pandas as pd
import logging
from config import PROCESSOR_DIR, RAW_ATTACHED_FILES, CRM_DIR  # Added CRM_DIR

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Directory for incoming raw files
INCOMING_DIR = RAW_ATTACHED_FILES
logging.info(f"Scanning directory: {INCOMING_DIR}")

# Processor patterns dictionary
PROCESSOR_PATTERNS = {
    "safecharge": {
        "pattern": r"126728__transaction-search_[0-9]+_[a-z0-9]+(?i:\.csv|\.xlsx|\.xls)",
        "date_format": None,
        "type_group": None,
        "date_column": "Date",
        "header_row": 11
    },
    "bitpay": {
        "pattern": r"bitpay-export-[a-z]{3}-(\d{1,2}-\d{1,2}-\d{4})-_to_\d{1,2}-\d{1,2}-\d{4}(?:\s*\(\d+\))?(?i:\.csv|\.xlsx|\.xls)",
        "date_format": "%m-%d-%Y",
        "type_group": None,
        "date_column": "date",
        "header_row": 0
    },
    "ezeebill": {
        "pattern": r"daily_transaction_report_(\d{4}-\d{2}-\d{2})_to_\d{4}-\d{2}-\d{2}(?i:\.csv|\.xlsx|\.xls)",
        "date_format": "%Y-%m-%d",
        "type_group": None,
        "date_column": None,
        "header_row": 17
    },
    "paypal": {
        "pattern": r"(?:download|Download)\s+-\s+.*(?i:\.csv|\.xlsx|\.xls)",
        "date_format": None,
        "type_group": None,
        "date_column": "Date",
        "header_row": 0
    },
    "zotapay": {
        "pattern": r"export(\s*\(\d+\))?(?i:\.csv|\.xlsx|\.xls)",
        "date_format": None,
        "type_group": None,
        "date_column": "Ended At",
        "header_row": 1
    },
    "paymentasia": {
        "pattern": r"export_(transactions|payouts)_\d+(?i:\.csv|\.xlsx|\.xls)",
        "date_format": None,
        "type_group": 1,
        "date_column": "Completed Time",
        "header_row": 0
    },
    "powercash": {
        "pattern": r"report-[a-zA-Z0-9]+(?i:\.csv|\.xlsx|\.xls)",
        "date_format": "%d.%m.%Y",  # Specify input format for Date column
        "type_group": None,
        "date_column": "Date",
        "header_row": 0
    },
    "trustpayments": {
        "pattern": r"searchresults(\s*\(\d+\))?(?i:\.csv|\.xlsx|\.xls)",
        "date_format": None,
        "type_group": None,
        "date_column": "transactionstartedtimestamp",
        "header_row": 0
    },
    "skrill": {
        "pattern": r"transactions_\d+(?i:\.csv|\.xlsx|\.xls)",
        "date_format": None,
        "type_group": None,
        "date_column": "Time (CET)",
        "header_row": 0
    },
    "neteller": {
        "pattern": r"transactions_\d+(?i:\.csv|\.xlsx|\.xls)",
        "date_format": None,
        "type_group": None,
        "date_column": "Time (UTC)",
        "header_row": 0
    },
    "shift4": {
        "pattern": r"(?i)processingactivity_[0-9]{4}-[0-9]{2}-[0-9]{2}t[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]+(?i:\.csv|\.xlsx|\.xls)",
        "date_format": None,
        "type_group": None,
        "date_column": "Transaction Date",
        "header_row": 0
    },
    "crm": {
        "pattern": r"(?i)crm_[0-9]{4}-[0-9]{2}-[0-9]{2}(?i:\.xlsx|\.xls)",
        "date_format": None,
        "type_group": None,
        "date_column": None,
        "header_row": None,
        "dest_dir": CRM_DIR  # Move to crm_reports
    }
}

def extract_date_from_file(file_path: Path, date_column: str = None, header_row: int = 0, processor=None, config=None):
    """
    Extract the most recent date from the file content if no date in filename.
    - Returns YYYY-MM-DD or custom format for powercash, or None if failed.
    - Logs available columns for debugging.
    - For skrill/neteller, checks both Time (CET) and Time (UTC).
    """
    if not date_column:
        return None
    try:
        if file_path.suffix in ['.xls', '.xlsx']:
            try:
                df = pd.read_excel(file_path, engine='openpyxl', header=header_row)
            except Exception as e:
                logging.error(f"Initial read failed for {file_path} with openpyxl: {e}")
                df = pd.read_excel(file_path, engine='xlrd', header=header_row)  # Requires xlrd
        else:
            df = pd.read_csv(file_path, header=header_row)
        logging.info(f"Processing file: {file_path}")
        logging.info(f"Columns in {file_path}: {df.columns.tolist()}")
        if date_column in df.columns:
            date_format = config.get("date_format") if config else None  # Use date_format from config
            if date_format:
                dates = pd.to_datetime(df[date_column], format=date_format, errors='coerce')
            else:
                dates = pd.to_datetime(df[date_column], errors='coerce')
            max_date = dates.max()
            if pd.notna(max_date):
                # Special handling for powercash to output YYYY-MM-DD
                if processor == "powercash":
                    return max_date.strftime('%Y-%m-%d')
                return max_date.strftime('%Y-%m-%d')
            logging.warning(f"No valid maximum date found in {date_column} for {file_path}")
        logging.warning(f"Column {date_column} not found or no valid dates in {file_path}")
        if date_column in ["Time (CET)", "Time (UTC)"]:
            columns = df.columns
            if "Time (UTC)" in columns and date_column == "Time (UTC)":
                dates = pd.to_datetime(df["Time (UTC)"], errors='coerce')
                max_date = dates.max()
                if pd.notna(max_date):
                    logging.info(f"Using max Time (UTC) for {file_path}")
                    return max_date.strftime('%Y-%m-%d')
            elif "Time (CET)" in columns and date_column == "Time (CET)":
                dates = pd.to_datetime(df["Time (CET)"], errors='coerce')
                max_date = dates.max()
                if pd.notna(max_date):
                    logging.info(f"Using max Time (CET) for {file_path}")
                    return max_date.strftime('%Y-%m-%d')
        if "zotapay" in file_path.name:
            for col in ["Created At", "Completed Time", "Date"]:
                if col in df.columns:
                    dates = pd.to_datetime(df[col], errors='coerce')
                    max_date = dates.max()
                    if pd.notna(max_date):
                        return max_date.strftime('%Y-%m-%d')
    except Exception as e:
        logging.error(f"Date extraction failed for {file_path}: {e}")
    return None

def rename_raw_file(file_path: Path):
    """
    Detect processor, extract date and type, rename, and move the file to dest_dir if specified.
    - Returns True if renamed or moved, False otherwise.
    """
    filename = file_path.name.lower()
    potential_processors = []
    for processor, config in PROCESSOR_PATTERNS.items():
        match = re.match(config["pattern"], filename)
        if match:
            potential_processors.append((processor, config, match))

    if not potential_processors:
        logging.warning(f"No pattern match for {filename}, leaving in {INCOMING_DIR}. Available patterns: {list(PROCESSOR_PATTERNS.keys())}")
        return False

    for processor, config, match in potential_processors:
        try:
            if config["date_format"] and match and len(
                    match.groups()) > 0:  # Use date_format only if a capture group exists
                date_raw = match.group(1)
                date_str = datetime.strptime(date_raw, config["date_format"]).strftime('%Y-%m-%d')
            elif config["date_column"] and config["header_row"] is not None:  # Rename logic
                date_str = extract_date_from_file(file_path, config["date_column"], config["header_row"], processor,
                                                  config)
                if not date_str:
                    logging.warning(f"No date found for {filename} with {processor}, skipping")
                    continue
            else:  # Move-only logic (e.g., crm)
                date_str = None  # No renaming, keep original name

            if processor == "paymentasia" and config["type_group"] and match.group(config["type_group"]):
                new_name = f"{processor}_{'deposits' if match.group(config['type_group']) == 'transactions' else 'withdrawals'}_{date_str}{file_path.suffix.lower()}" if date_str else file_path.name
            else:
                new_name = f"{processor}_{date_str}{file_path.suffix.lower()}" if date_str else file_path.name

            dest_path = config.get("dest_dir", PROCESSOR_DIR) / new_name  # Use dest_dir if defined, else PROCESSOR_DIR
            if dest_path.exists():
                logging.warning(f"Destination {dest_path} exists, skipping {filename}")
                continue
            move(str(file_path), str(dest_path))
            action = "Renamed" if date_str else "Moved"
            logging.info(f"{action} {filename} to {dest_path} for {processor}")
            return True
        except (ValueError, IndexError) as e:
            logging.error(f"Processing failed for {filename} with {processor}: {e}")
            continue
    return False

def run_renamer(incoming_dir: Path = INCOMING_DIR):
    """
    Scan incoming_dir for raw files, rename where needed, and move to PROCESSOR_DIR or CRM_DIR.
    """
    renamed_count = 0
    incoming_dir.mkdir(parents=True, exist_ok=True)
    files_found = [str(f) for f in incoming_dir.glob("*.*")]  # Log full paths
    logging.info(f"Files found in {incoming_dir}: {files_found}")
    if not files_found:
        logging.info("No files found in the directory to process.")
    for file in incoming_dir.glob("*.*"):
        if file.is_file() and file.suffix.lower() in ['.csv', '.xlsx', '.xls']:
            logging.info(f"Checking file: {file} (suffix: {file.suffix}, lower: {file.suffix.lower()})")
            try:
                if rename_raw_file(file):
                    renamed_count += 1
            except Exception as e:
                logging.error(f"Unexpected error processing {file}: {e}")
    logging.info(f"{('Renamed' if any(p.get('date_column') for p in PROCESSOR_PATTERNS.values()) else 'Moved')} {renamed_count} files. Unrecognized files remain in {incoming_dir}.")

if __name__ == "__main__":
    run_renamer()