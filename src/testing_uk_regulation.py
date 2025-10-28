# testing_regulation.py (Updated copy logic for selective processor files)

import pandas as pd
from pathlib import Path
import shutil
import src.config as config
from src.preprocess_test import load_crm_file, load_processor_file, combine_processed_files, process_files_in_parallel
from concurrent.futures import ThreadPoolExecutor
from src.config import BASE_DIR, TEMP_DIR

def setup_regulation_structure(regulation, processors):
    reg_upper = regulation.upper()
    reg_root = TEMP_DIR / reg_upper
    reg_data_dir = reg_root / "data"
    reg_output_dir = reg_root / "output"
    reg_raw_attached_files = reg_root / "raw_attached_files"

    # Create directories
    for d in [reg_data_dir, reg_output_dir, reg_raw_attached_files]:
        d.mkdir(parents=True, exist_ok=True)

    # Subdirs
    reg_crm_dir = reg_data_dir / "crm_reports"
    reg_processor_dir = reg_data_dir / "processor_reports"
    reg_raw_tracking_dir = reg_data_dir / "raw_tracking_lists"
    reg_processed_dir = reg_data_dir / "processed"
    reg_processed_crm_dir = reg_processed_dir / "crm"
    reg_processed_processor_dir = reg_processed_dir / "processors"
    reg_rates_dir = reg_data_dir / "rates"
    reg_lists_dir = reg_data_dir / "lists"
    reg_combined_crm_dir = reg_processed_crm_dir / "combined"
    reg_processed_unmatched_shifted_deposits_dir = reg_processed_crm_dir / "unmatched_shifted_deposits"
    reg_training_dataset_dir = reg_data_dir / "training_dataset"
    reg_true_training_dir = reg_training_dataset_dir / "check_newversion_datasets"
    reg_false_training_dir = reg_training_dataset_dir / "false_training_datasets"
    reg_test_model_dir = reg_training_dataset_dir / "model_testing"

    # Create all subdirs
    for d in [reg_crm_dir, reg_processor_dir, reg_raw_tracking_dir, reg_processed_dir, reg_processed_crm_dir,
              reg_processed_processor_dir, reg_rates_dir, reg_lists_dir, reg_combined_crm_dir,
              reg_processed_unmatched_shifted_deposits_dir, reg_training_dataset_dir, reg_true_training_dir,
              reg_false_training_dir, reg_test_model_dir]:
        d.mkdir(parents=True, exist_ok=True)

    # Copy shared CRM to reg CRM dir
    shared_crm_filepath = BASE_DIR / "data" / "crm_reports" / "crm_2025-10-20.xlsx"
    reg_crm_filepath = reg_crm_dir / shared_crm_filepath.name
    if shared_crm_filepath.exists():
        shutil.copy(shared_crm_filepath, reg_crm_filepath)
    else:
        print(f"CRM file not found at {shared_crm_filepath}")
        exit(1)

    # Copy relevant processor files from shared to reg_processor_dir
    shared_processor_dir = BASE_DIR / "data" / "processor_reports"
    if shared_processor_dir.exists():
        for proc_file in shared_processor_dir.glob("*"):
            # Extract processor name from filename (before first '_')
            proc_name = proc_file.stem.split('_')[0].lower()
            if proc_name in processors:
                shutil.copy(proc_file, reg_processor_dir / proc_file.name)

    return {
        'root': reg_root,
        'data_dir': reg_data_dir,
        'crm_dir': reg_crm_dir,
        'processor_dir': reg_processor_dir,
        'processed_crm_dir': reg_processed_crm_dir,
        'processed_processor_dir': reg_processed_processor_dir,
        'lists_dir': reg_lists_dir,
        'combined_crm_dir': reg_combined_crm_dir,
        'processed_unmatched_shifted_deposits_dir': reg_processed_unmatched_shifted_deposits_dir,
        'crm_filepath': reg_crm_filepath
    }

# Date from the file
date_str = '2025-10-20'

# Processors (ROW without barclays/safechargeuk/barclaycard, UK with extras)
row_processors = [
    'paypal', 'safecharge', 'powercash', 'shift4', 'skrill', 'neteller',
    'trustpayments', 'zotapay', 'bitpay', 'ezeebill', 'paymentasia'
]
uk_processors = [
    'paypal', 'safechargeuk', 'powercash', 'shift4', 'skrill', 'neteller',
    'trustpayments', 'zotapay', 'bitpay', 'ezeebill', 'paymentasia', 'barclays', 'barclaycard'
]

def preprocess_for_regulation(regulation, transaction_type='deposit'):
    processors = row_processors if regulation == 'row' else uk_processors
    dirs = setup_regulation_structure(regulation, processors)  # Pass processors to setup for selective copy

    print(f"Using {regulation.upper()}_PROCESSED_CRM_DIR: {dirs['processed_crm_dir']}")
    print(f"Using {regulation.upper()}_PROCESSED_PROCESSOR_DIR: {dirs['processed_processor_dir']}")

    # Process CRM
    crm_file_paths = [dirs['crm_filepath']] * len(processors)
    processed_crm_dfs = process_files_in_parallel(crm_file_paths, processor_names=processors, is_crm=True, save_clean=True, transaction_type=transaction_type, regulation=regulation,
                                                  lists_dir=dirs['lists_dir'], processed_unmatched_shifted_deposits_dir=dirs['processed_unmatched_shifted_deposits_dir'], processed_crm_dir=dirs['processed_crm_dir'])

    # Debug print for processed CRM
    for i, df in enumerate(processed_crm_dfs):
        print(f"Processed DF for {processors[i]} {transaction_type} ({regulation}): {len(df)} rows")

    # Process processors (assuming processor raw files are in reg_processor_dir; place them accordingly for ROW/UK)
    processor_file_paths = []
    for proc in processors:
        for ext in ['xlsx', 'csv', 'xls']:
            proc_file = dirs['processor_dir'] / f"{proc}_{date_str}.{ext}"
            if proc_file.exists():
                processor_file_paths.append(proc_file)
                break
        else:
            processor_file_paths.append(None)

    processed_proc_dfs = process_files_in_parallel(processor_file_paths, processor_names=processors, is_crm=False, save_clean=True, transaction_type=transaction_type, regulation=regulation,
                                                   processed_processor_dir=dirs['processed_processor_dir'])

    # Combine
    combine_processed_files(
        date_str,
        processors,
        processed_crm_dir=dirs['processed_crm_dir'],
        processed_proc_dir=dirs['processed_processor_dir'],
        out_crm_dir=dirs['combined_crm_dir'],
        out_proc_dir=dirs['processed_processor_dir'] / "combined",
        transaction_type=transaction_type,
        regulation=regulation
    )
    print(f"Preprocessed and combined {transaction_type}s for {regulation.upper()} regulation saved successfully.")

# Run for both ROW and UK, deposits and withdrawals
if __name__ == "__main__":
    for reg in ['row', 'uk']:
        preprocess_for_regulation(reg, 'deposit')
        preprocess_for_regulation(reg, 'withdrawal')