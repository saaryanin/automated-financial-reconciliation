import pandas as pd
from pathlib import Path
import shutil
import src.config as config
from src.preprocess_test import load_crm_file, load_processor_file, combine_processed_files, process_files_in_parallel
from concurrent.futures import ThreadPoolExecutor
from src.config import BASE_DIR, TEMP_DIR

# Setup ROW structure if not exists (move existing temp contents to ROW if applicable)
ROW_ROOT = TEMP_DIR / "ROW"
if not ROW_ROOT.exists() and any((TEMP_DIR / sub).exists() for sub in ['data', 'output', 'raw_attached_files']):
    ROW_ROOT.mkdir(parents=True, exist_ok=True)
    for sub in ['data', 'output', 'raw_attached_files']:
        src = TEMP_DIR / sub
        dst = ROW_ROOT / sub
        if src.exists():
            shutil.move(str(src), str(dst))

# Clean up any unnecessary top-level data/output/raw_attached_files if they still exist after move
for sub in ['data', 'output', 'raw_attached_files']:
    unnecessary = TEMP_DIR / sub
    if unnecessary.exists():
        shutil.rmtree(unnecessary)

# Setup UK structure
UK_ROOT = TEMP_DIR / "UK"
UK_DATA_DIR = UK_ROOT / "data"
UK_OUTPUT_DIR = UK_ROOT / "output"
UK_RAW_ATTACHED_FILES = UK_ROOT / "raw_attached_files"

# Create directories
for d in [UK_DATA_DIR, UK_OUTPUT_DIR, UK_RAW_ATTACHED_FILES]:
    d.mkdir(parents=True, exist_ok=True)

# Override subdirs for UK
UK_CRM_DIR = UK_DATA_DIR / "crm_reports"
UK_PROCESSOR_DIR = UK_DATA_DIR / "processor_reports"
UK_RAW_TRACKING_DIR = UK_DATA_DIR / "raw_tracking_lists"
UK_PROCESSED_DIR = UK_DATA_DIR / "processed"
UK_PROCESSED_CRM_DIR = UK_PROCESSED_DIR / "crm"
UK_PROCESSED_PROCESSOR_DIR = UK_PROCESSED_DIR / "processors"
UK_RATES_DIR = UK_DATA_DIR / "rates"
UK_LISTS_DIR = UK_DATA_DIR / "lists"
UK_COMBINED_CRM_DIR = UK_PROCESSED_CRM_DIR / "combined"
UK_PROCESSED_UNMATCHED_SHIFTED_DEPOSITS_DIR = UK_PROCESSED_CRM_DIR / "unmatched_shifted_deposits"
UK_TRAINING_DATASET_DIR = UK_DATA_DIR / "training_dataset"
UK_TRUE_TRAINING_DIR = UK_TRAINING_DATASET_DIR / "check_newversion_datasets"
UK_FALSE_TRAINING_DIR = UK_TRAINING_DATASET_DIR / "false_training_datasets"
UK_TEST_MODEL_DIR = UK_TRAINING_DATASET_DIR / "model_testing"

# Create all subdirs
for d in [UK_CRM_DIR, UK_PROCESSOR_DIR, UK_RAW_TRACKING_DIR, UK_PROCESSED_DIR, UK_PROCESSED_CRM_DIR,
          UK_PROCESSED_PROCESSOR_DIR, UK_RATES_DIR, UK_LISTS_DIR, UK_COMBINED_CRM_DIR,
          UK_PROCESSED_UNMATCHED_SHIFTED_DEPOSITS_DIR, UK_TRAINING_DATASET_DIR, UK_TRUE_TRAINING_DIR,
          UK_FALSE_TRAINING_DIR, UK_TEST_MODEL_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# Copy the shared CRM file to UK input dir
shared_crm_filepath = BASE_DIR / "data" / "crm_reports" / "crm_2025-10-20.xlsx"
uk_crm_filepath = UK_CRM_DIR / shared_crm_filepath.name
if shared_crm_filepath.exists():
    shutil.copy(shared_crm_filepath, uk_crm_filepath)
else:
    print(f"CRM file not found at {shared_crm_filepath}")
    exit(1)

# Define processors for UK (including barclays)
uk_processors = [
    'paypal', 'safecharge', 'powercash', 'shift4', 'skrill', 'neteller',
    'trustpayments', 'zotapay', 'bitpay', 'ezeebill', 'paymentasia', 'barclays'
]

# Date from the file
date_str = '2025-10-20'

# Function to preprocess for a given transaction type and regulation (now handles both CRM and processors)
def preprocess_for_uk(transaction_type='deposit'):
    print(f"Using UK_PROCESSED_CRM_DIR: {UK_PROCESSED_CRM_DIR}")
    print(f"Using UK_PROCESSED_PROCESSOR_DIR: {UK_PROCESSED_PROCESSOR_DIR}")

    # No override of module; pass params

    # Process CRM
    crm_file_paths = [uk_crm_filepath] * len(uk_processors)
    processed_crm_dfs = process_files_in_parallel(crm_file_paths, processor_names=uk_processors, is_crm=True, save_clean=True, transaction_type=transaction_type, regulation='uk',
                                                  lists_dir=UK_LISTS_DIR, processed_unmatched_shifted_deposits_dir=UK_PROCESSED_UNMATCHED_SHIFTED_DEPOSITS_DIR, processed_crm_dir=UK_PROCESSED_CRM_DIR)

    # Add print for debug: check if processed_dfs have data
    for i, df in enumerate(processed_crm_dfs):
        print(f"Processed DF for {uk_processors[i]} {transaction_type}: {len(df)} rows")

    # Process processors
    processor_file_paths = []  # Collect actual processor file paths
    for proc in uk_processors:
        # Try both .xlsx and .csv extensions
        for ext in ['xlsx', 'csv']:
            proc_file = UK_PROCESSOR_DIR / f"{proc}_{date_str}.{ext}"
            if proc_file.exists():
                processor_file_paths.append(proc_file)
                break
        else:
            processor_file_paths.append(None)  # No file found

    processed_proc_dfs = process_files_in_parallel(processor_file_paths, processor_names=uk_processors, is_crm=False, save_clean=True, transaction_type=transaction_type, regulation='uk',
                                                   processed_processor_dir=UK_PROCESSED_PROCESSOR_DIR)

    # Combine
    combine_processed_files(
        date_str,
        uk_processors,
        processed_crm_dir=UK_PROCESSED_CRM_DIR,
        processed_proc_dir=UK_PROCESSED_PROCESSOR_DIR,
        out_crm_dir=UK_COMBINED_CRM_DIR,
        out_proc_dir=UK_PROCESSED_PROCESSOR_DIR / "combined",
        transaction_type=transaction_type,
        regulation='uk'
    )
    print(f"Preprocessed and combined {transaction_type}s for UK regulation saved successfully.")


# Run for deposits and withdrawals to check
if __name__ == "__main__":
    preprocess_for_uk('deposit')
    preprocess_for_uk('withdrawal')