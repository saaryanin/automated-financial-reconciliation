"""
Copyright 2026 Saar Yanin
Licensed under the Apache License 2.0 with Commons Clause
See LICENSE for details.

Script: files_renamer.py
Description: This script scans the incoming raw files directory for processor and CRM files, identifies processors using regex patterns, extracts dates either from filenames or by inspecting file contents, renames files to a standardized format (e.g., processor_date.extension), and moves or copies them to appropriate regulation-specific directories (ROW or UK) for CRM or processors. It ensures CRM files are duplicated to both regulations, handles pre-renamed files, and allows for forced date overrides.

Key Features:
- Uses processor-specific regex patterns to match filenames, extract dates, transaction types (deposits/withdrawals), and identify processors like paypal, safecharge, barclays.
- Fallback date extraction: If date not in filename, loads file with pandas and finds the maximum date from processor-specific columns (e.g., 'Transaction Date' for paypal), handling various date formats and headers.
- Regulation determination: Classifies as UK for specific processors (barclays, barclaycard, safechargeuk), defaulting to ROW; uses categorize_regulation for site-based logic.
- File handling: Copies CRM files to both ROW and UK directories, moves processor files to the determined regulation directory, and removes originals only after successful operations.
- Supports already-renamed files by checking destination existence, skips duplicates, and logs all renames, moves, and errors.
- Forced date override: Allows specifying a date to use for renaming when automatic extraction fails or for testing.
- Edge cases: Handles multiple date formats in content (e.g., DD/MM/YYYY, YYYY-MM-DD), skips non-matching files, manages file extensions (xlsx, xls, csv), and warns on extraction failures.

Dependencies:
- re (for regex pattern matching in filenames and content)
- pathlib (for path manipulation and file existence checks)
- datetime (for date parsing and formatting)
- shutil (for file copying and moving)
- pandas (for loading and inspecting file contents for date extraction)
- logging (for detailed logging of actions and errors)
- src.config (for RAW_ATTACHED_FILES directory and setup_dirs_for_reg function)
"""
import re
from pathlib import Path
from datetime import datetime
from shutil import move, copyfile
import pandas as pd
import logging
from src.config import RAW_ATTACHED_FILES, setup_dirs_for_reg  # Removed CRM_DIR and PROCESSOR_DIR

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Directory for incoming raw files
INCOMING_DIR = RAW_ATTACHED_FILES
logging.info(f"Scanning directory: {INCOMING_DIR}")

# Processor patterns dictionary (formatted for readability)
PROCESSOR_PATTERNS = {
    "safecharge": {
        "pattern": r"126728__transaction-search_[0-9]+_[a-z0-9]+(?i:\.csv|\.xlsx|\.xls)",
        "date_format": None,
        "type_group": None,
        "date_column": "Date",
        "header_row": 11
    },
    "safechargeuk": {
        "pattern": r"149858__transaction-search_[0-9]+_[a-z0-9]+(?i:\.csv|\.xlsx|\.xls)",
        "date_format": None,
        "type_group": None,
        "date_column": "Date",
        "header_row": 11
    },
    "barclays": {
        "pattern": r"(?i)transactionreport\d{8}( ?\(\d+\))?\.(csv|xlsx|xls)",
        "date_format": None,
        "type_group": None,
        "date_column": "Date",  # Adjust based on actual file if needed
        "header_row": 0
    },
    "barclaycard": {
        "pattern": r"(?i)barclaycard.*(?i:\.csv|\.xlsx|\.xls)",
        "date_format": None,
        "type_group": None,
        "date_column": "Transaction Date",  # Adjust based on actual file
        "header_row": 0
    },
        "xbo": {
        "pattern": r"(?i)transactions report \[\d{2}\s*[̸/]\s*\d{2}\s*[̸/]\s*\d{4}\s*[‒-]\s*(\d{2}\s*[̸/]\s*\d{2}\s*[̸/]\s*\d{4})\](?i:\.csv|\.xlsx|\.xls)",
        "date_format": None,
        "date_group": 1,
        "type_group": None,
        "date_column": "processing_date",   # fallback if needed
        "header_row": 0
    },
    "bitpay": {
        "pattern": r"(?i)bitpay-export-[a-zA-Z]{3}-\d{1,2}-\d{1,2}-\d{4}-_to_(\d{1,2}-\d{1,2}-\d{4})(?:\s*\(\d+\))?(?i:\.csv|\.xlsx|\.xls)",
        "date_format": "%m-%d-%Y",
        "date_group": 1,
        "type_group": None,
        "date_column": "date",
        "header_row": 0
    },
    "ezeebill": {
        "pattern": r"daily_transaction_report_\d{4}-\d{2}-\d{2}_to_(\d{4}-\d{2}-\d{2})(?i:\.csv|\.xlsx|\.xls)",
        "date_format": "%Y-%m-%d",
        "type_group": None,
        "date_column": None,
        "header_row": 17
    },
    "paypal": {
        "pattern": r"(?:download|Download)(?:\s*-\s*(\d{4}-\d{2}-\d{2})[Tt]\d{6}\.\d{3})?(?i:\.csv|\.xlsx|\.xls)",
        "date_format": "%Y-%m-%d",
        "date_group": 1,
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
        "pattern": r"(report-[a-zA-Z0-9]+|transactionlog(\s*\(\d+\))?)(?i:\.csv|\.xlsx|\.xls)",
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
        "pattern": r"(?i)crm_(\d{4}-\d{2}-\d{2})(?i:\.csv|\.xlsx|\.xls)",
        "date_format": "%Y-%m-%d",
        "is_renamed": True,
        "type_group": None,
        "date_column": None,
        "header_row": None,
        "dest_dir": None  # Dynamic now
    }
}

# Add renamed patterns (updated to set dest_dir=None; handled dynamically)
PROCESSOR_PATTERNS.update({
    "bitpay_renamed": {
        "pattern": r"bitpay_(\d{4}-\d{2}-\d{2})(?i:\.csv|\.xlsx|\.xls)",
        "date_format": "%Y-%m-%d",
        "is_renamed": True,
        "processor": "bitpay",
        "type_group": None,
        "date_column": None,
        "header_row": None,
        "dest_dir": None
    },
    "ezeebill_renamed": {
        "pattern": r"ezeebill_(\d{4}-\d{2}-\d{2})(?i:\.csv|\.xlsx|\.xls)",
        "date_format": "%Y-%m-%d",
        "is_renamed": True,
        "processor": "ezeebill",
        "type_group": None,
        "date_column": None,
        "header_row": None,
        "dest_dir": None
    },
    "neteller_renamed": {
        "pattern": r"neteller_(\d{4}-\d{2}-\d{2})(?i:\.csv|\.xlsx|\.xls)",
        "date_format": "%Y-%m-%d",
        "is_renamed": True,
        "processor": "neteller",
        "type_group": None,
        "date_column": None,
        "header_row": None,
        "dest_dir": None
    },
    "paypal_renamed": {
        "pattern": r"paypal_(\d{4}-\d{2}-\d{2})(?i:\.csv|\.xlsx|\.xls)",
        "date_format": "%Y-%m-%d",
        "is_renamed": True,
        "processor": "paypal",
        "type_group": None,
        "date_column": None,
        "header_row": None,
        "dest_dir": None
    },
    "paymentasia_renamed": {
        "pattern": r"paymentasia_(deposits|withdrawals)_(\d{4}-\d{2}-\d{2})(?i:\.csv|\.xlsx|\.xls)",
        "date_format": "%Y-%m-%d",
        "is_renamed": True,
        "processor": "paymentasia",
        "type_group": 1,
        "date_column": None,
        "header_row": None,
        "dest_dir": None
    },
    "powercash_renamed": {
        "pattern": r"powercash_(\d{4}-\d{2}-\d{2})(?i:\.csv|\.xlsx|\.xls)",
        "date_format": "%Y-%m-%d",
        "is_renamed": True,
        "processor": "powercash",
        "type_group": None,
        "date_column": None,
        "header_row": None,
        "dest_dir": None
    },
    "safecharge_renamed": {
        "pattern": r"safecharge_(\d{4}-\d{2}-\d{2})(?i:\.csv|\.xlsx|\.xls)",
        "date_format": "%Y-%m-%d",
        "is_renamed": True,
        "processor": "safecharge",
        "type_group": None,
        "date_column": None,
        "header_row": None,
        "dest_dir": None
    },
    "safechargeuk_renamed": {
        "pattern": r"safechargeuk_(\d{4}-\d{2}-\d{2})(?i:\.csv|\.xlsx|\.xls)",
        "date_format": "%Y-%m-%d",
        "is_renamed": True,
        "processor": "safechargeuk",
        "type_group": None,
        "date_column": None,
        "header_row": None,
        "dest_dir": None
    },
    "shift4_renamed": {
        "pattern": r"shift4_(\d{4}-\d{2}-\d{2})(?i:\.csv|\.xlsx|\.xls)",
        "date_format": "%Y-%m-%d",
        "is_renamed": True,
        "processor": "shift4",
        "type_group": None,
        "date_column": None,
        "header_row": None,
        "dest_dir": None
    },
    "skrill_renamed": {
        "pattern": r"skrill_(\d{4}-\d{2}-\d{2})(?i:\.csv|\.xlsx|\.xls)",
        "date_format": "%Y-%m-%d",
        "is_renamed": True,
        "processor": "skrill",
        "type_group": None,
        "date_column": None,
        "header_row": None,
        "dest_dir": None
    },
    "trustpayments_renamed": {
        "pattern": r"trustpayments_(\d{4}-\d{2}-\d{2})(?i:\.csv|\.xlsx|\.xls)",
        "date_format": "%Y-%m-%d",
        "is_renamed": True,
        "processor": "trustpayments",
        "type_group": None,
        "date_column": None,
        "header_row": None,
        "dest_dir": None
    },
    "zotapay_renamed": {
        "pattern": r"zotapay_(\d{4}-\d{2}-\d{2})(?i:\.csv|\.xlsx|\.xls)",
        "date_format": "%Y-%m-%d",
        "is_renamed": True,
        "processor": "zotapay",
        "type_group": None,
        "date_column": None,
        "header_row": None,
        "dest_dir": None
    },
    "barclays_renamed": {
        "pattern": r"barclays_(\d{4}-\d{2}-\d{2})(?i:\.csv|\.xlsx|\.xls)",
        "date_format": "%Y-%m-%d",
        "is_renamed": True,
        "processor": "barclays",
        "type_group": None,
        "date_column": None,
        "header_row": None,
        "dest_dir": None
    },
    "barclaycard_renamed": {
        "pattern": r"barclaycard_(\d{4}-\d{2}-\d{2})(?i:\.csv|\.xlsx|\.xls)",
        "date_format": "%Y-%m-%d",
        "is_renamed": True,
        "processor": "barclaycard",
        "type_group": None,
        "date_column": None,
        "header_row": None,
        "dest_dir": None
    },
    "xbo_renamed": {
        "pattern": r"xbo_(\d{4}-\d{2}-\d{2})(?i:\.csv|\.xlsx|\.xls)",
        "date_format": "%Y-%m-%d",
        "is_renamed": True,
        "processor": "xbo",
        "type_group": None,
        "date_column": None,
        "header_row": None,
        "dest_dir": None
    }
})

UK_ONLY_PROCESSORS = {"barclays", "barclaycard", "safechargeuk"}


def detect_processor_from_name(filename):
    """Detect processor name from filename based on keywords."""
    filename_lower = filename.lower()
    if filename_lower.startswith("crm_"):
        return "crm"
    if "transactionlog" in filename_lower:
        return "powercash"
    if "transactionreport" in filename_lower:
        return "barclays"
    if "transactions report" in filename_lower:
        return "xbo"
    processors = [
        "safecharge", "safechargeuk", "bitpay", "ezeebill", "paypal", "zotapay", "paymentasia", "powercash",
        "trustpayments", "paysafe", "skrill", "neteller", "shift4", "barclays", "barclaycard", "xbo"
    ]
    for processor in processors:
        if processor in filename_lower:
            return processor
    return "unknown"


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


def get_regulation_from_processor(processor_name: str) -> str:
    """Determine regulation (UK or ROW) based on processor name."""
    processor_lower = processor_name.lower()
    if processor_lower in UK_ONLY_PROCESSORS:
        return "uk"
    return "row"  # Default to ROW for shared processors


def rename_raw_file(file_path: Path, forced_date: str = None):
    """
    Detect processor, extract date and type, rename, and move the file to regulation-specific dir.
    - For CRM, use copy to duplicate to both regulations, then remove original if successful.
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
        processor_original = config.get("processor", processor)
        try:
            date_str = None
            if config.get("is_renamed"):
                type_group = config.get("type_group")
                date_group = 1 if type_group is None else type_group + 1
                date_raw = match.group(date_group)
                date_str = datetime.strptime(date_raw, config["date_format"]).strftime('%Y-%m-%d')
            else:
                date_group = config.get("date_group")
                if date_group is not None and match and len(match.groups()) >= date_group:
                    date_raw = match.group(date_group)
                    if processor_original == "xbo" and date_raw:
                        date_raw = date_raw.replace(' ̸ ', '/').replace(' ‒ ', '-').strip()
                        date_obj = datetime.strptime(date_raw, '%m/%d/%Y')
                        date_str = date_obj.strftime('%Y-%m-%d')
                    elif config["date_format"]:
                        date_str = datetime.strptime(date_raw, config["date_format"]).strftime('%Y-%m-%d')
                    else:
                        date_str = date_raw
                elif config["date_column"] and config["header_row"] is not None:
                    date_str = extract_date_from_file(
                        file_path, config["date_column"], config["header_row"], processor_original, config
                    )
                    if not date_str:
                        logging.warning(f"No date found for {filename} with {processor_original}, skipping")
                        continue
            if forced_date:
                date_str = forced_date

            # If no date_str and not a move-only (like crm without rename), skip unless forced_date set it
            if not date_str and not config.get("is_renamed"):
                logging.warning(f"No date available for {filename} with {processor_original}, skipping")
                continue

            if config.get("is_renamed"):
                new_name = file_path.name
            else:
                if processor_original == "paymentasia" and config["type_group"] and match.group(config['type_group']):
                    new_name = f"{processor_original}_{'deposits' if match.group(config['type_group']) == 'transactions' else 'withdrawals'}_{date_str}{file_path.suffix.lower()}" if date_str else file_path.name
                else:
                    new_name = f"{processor_original}_{date_str}{file_path.suffix.lower()}" if date_str else file_path.name

            # Determine regulations
            if processor == "crm" or processor.endswith("_renamed") and "crm" in processor:
                regulations = ['row', 'uk']  # Duplicate CRM to both
            else:
                reg = get_regulation_from_processor(processor_original)
                regulations = [reg]

            moved = False
            for reg in regulations:
                dirs = setup_dirs_for_reg(reg, create=True)
                if "crm" in processor:
                    dest_dir = dirs['crm_dir']
                else:
                    dest_dir = dirs['processor_dir']
                dest_path = dest_dir / new_name
                if dest_path.exists():
                    logging.warning(f"Destination {dest_path} exists, skipping {filename} for {reg}")
                    continue
                if "crm" in processor:
                    copyfile(str(file_path), str(dest_path))
                    action = "Copied"
                else:
                    move(str(file_path), str(dest_path))
                    action = "Moved"
                logging.info(f"{action} {filename} to {dest_path} for {reg.upper()}")
                moved = True
            if moved and "crm" in processor and file_path.exists():
                file_path.unlink()  # Remove original after successful copies
                logging.info(f"Removed original CRM file {filename} after copying to both regulations")
            if moved:
                return True
        except (ValueError, IndexError) as e:
            logging.error(f"Processing failed for {filename} with {processor_original}: {e}")
            continue
    return False


def run_renamer(incoming_dir: Path = INCOMING_DIR, forced_date: str = None):
    """
    Scan incoming_dir for raw files, rename where needed, and move to regulation-specific dirs.
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
            renamed = False
            try:
                if rename_raw_file(file, forced_date=forced_date):
                    renamed = True
                    renamed_count += 1
            except Exception as e:
                logging.error(f"Unexpected error processing {file}: {e}")
            if not renamed and forced_date:
                filename_lower = file.name.lower()
                processor = detect_processor_from_name(filename_lower)
                if processor == "crm":
                    # Handle CRM fallback
                    match = re.match(r"(?i)crm_(\d{4}-\d{2}-\d{2})(?i:\.csv|\.xlsx|\.xls)", filename_lower)
                    if match and match.group(1) != forced_date:
                        new_name = f"crm_{forced_date}{file.suffix.lower()}"
                    else:
                        new_name = f"crm_{forced_date}{file.suffix.lower()}"
                    for reg in ['row', 'uk']:
                        dirs = setup_dirs_for_reg(reg, create=True)
                        dest_path = dirs['crm_dir'] / new_name
                        if not dest_path.exists():
                            copyfile(str(file), str(dest_path))
                            logging.info(f"Fallback copied CRM {file.name} to {dest_path} for {reg.upper()}")
                            renamed_count += 1
                    if file.exists():
                        file.unlink()
                        logging.info(f"Removed original CRM file {file.name} after fallback copying")
                elif processor != "unknown":
                    # Handle processor fallback
                    type_suffix = ""
                    if processor == "paymentasia":
                        if "payout" in filename_lower or "withdrawal" in filename_lower or "withdraw" in filename_lower:
                            type_suffix = "_withdrawals"
                        else:
                            type_suffix = "_deposits"
                    new_name = f"{processor}{type_suffix}_{forced_date}{file.suffix.lower()}"
                    reg = get_regulation_from_processor(processor)
                    dirs = setup_dirs_for_reg(reg, create=True)
                    dest_path = dirs['processor_dir'] / new_name
                    if not dest_path.exists():
                        move(str(file), str(dest_path))
                        logging.info(f"Fallback renamed {file.name} to {dest_path} for {reg.upper()}")
                        renamed_count += 1
    logging.info(f"Processed {renamed_count} files. Unrecognized files remain in {incoming_dir}.")


if __name__ == "__main__":
    run_renamer()