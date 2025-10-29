# testing_uk_regulation.py (my main basically):

# testing_regulation.py (Updated copy logic for selective processor files)

import pandas as pd
from pathlib import Path
import shutil
import src.config as config
from src.preprocess_test import load_crm_file, load_processor_file, combine_processed_files, process_files_in_parallel, PSP_NAME_MAP  # Added PSP_NAME_MAP import
from concurrent.futures import ThreadPoolExecutor
from src.config import BASE_DIR, TEMP_DIR
import time  # Added for timing
from src.utils import categorize_regulation  # Added import for categorize_regulation
from src.deposits_matcher_test import match_deposits_for_date  # Import the matching function

def setup_regulation_structure(regulation, processors):
    start_time = time.time()  # Timing start
    dirs = config.setup_dirs_for_reg(regulation, create=True)  # Use config to get and create dirs

    # Copy shared CRM to reg CRM dir only if not exists
    shared_crm_filepath = BASE_DIR / "data" / "crm_reports" / "crm_2025-10-20.xlsx"
    reg_crm_filepath = dirs['crm_dir'] / shared_crm_filepath.name
    if shared_crm_filepath.exists() and not reg_crm_filepath.exists():
        shutil.copy(shared_crm_filepath, reg_crm_filepath)
    elif not shared_crm_filepath.exists():
        print(f"CRM file not found at {shared_crm_filepath}")
        exit(1)

    # Copy relevant processor files from shared to reg_processor_dir only if not exists
    shared_processor_dir = BASE_DIR / "data" / "processor_reports"
    if shared_processor_dir.exists():
        for proc_file in shared_processor_dir.glob("*"):
            # Extract processor name from filename (before first '_')
            proc_name = proc_file.stem.split('_')[0].lower()
            if proc_name in processors:
                target_file = dirs['processor_dir'] / proc_file.name
                if not target_file.exists():
                    shutil.copy(proc_file, target_file)

    end_time = time.time()  # Timing end
    print(f"Setup for {regulation.upper()} took {end_time - start_time:.2f} seconds")

    return {
        **dirs,
        'crm_filepath': reg_crm_filepath
    }

# Date from the file
date_str = '2025-10-20'

# Processors (ROW without barclays/safechargeuk/barclaycard, UK with extras)
row_processors = [
    'paypal', 'safecharge', 'powercash', 'shift4', 'skrill', 'neteller',
    'trustpayments', 'zotapay', 'bitpay', 'ezeebill', 'paymentasia', 'bridgerpay'
]
uk_processors = [
    'safechargeuk', 'barclays', 'barclaycard'
]

def preprocess_for_regulation(regulation, transaction_type='deposit', dirs=None):
    start_time = time.time()  # Timing start for preprocess
    processors = row_processors if regulation == 'row' else uk_processors
    if dirs is None:
        dirs = setup_regulation_structure(regulation, processors)  # Fallback if not passed

    # Load CRM once to get unique relevant processors and cache it
    crm_df = pd.read_excel(dirs['crm_filepath'], engine="openpyxl")
    crm_df.columns = crm_df.columns.str.strip()
    crm_df['regulation'] = crm_df['Site (Account) (Account)'].apply(categorize_regulation)
    if regulation == 'row':
        row_regs = ['mauritius', 'cyprus', 'australia']
        crm_df = crm_df[crm_df['regulation'].isin(row_regs)]
        # Filter out paypal and inpendium for australia regulation (only for row)
        mask_aus = crm_df['regulation'] == 'australia'
        mask_psp = crm_df["PSP name"].str.lower().isin(['paypal', 'inpendium'])
        crm_df = crm_df[~(mask_aus & mask_psp)]
    elif regulation == 'uk':
        crm_df = crm_df[crm_df['regulation'] == 'uk']
    crm_df["PSP name"] = crm_df["PSP name"].astype(str).str.strip().str.lower().replace(PSP_NAME_MAP)
    if regulation == 'uk':
        crm_df["PSP name"] = crm_df["PSP name"].replace({'safecharge': 'safechargeuk'})
    name_mask = crm_df["Name"].str.lower() == transaction_type
    unique_psps = set(crm_df[name_mask]["PSP name"].dropna().unique())
    filtered_processors = [p for p in processors if p in unique_psps]

    crm_start = time.time()  # Timing for CRM
    # Process CRM only for filtered_processors, passing cached crm_df if possible (but since load_crm_file needs filepath for unmatched, keep as is)
    crm_file_paths = [dirs['crm_filepath']] * len(filtered_processors)
    processed_crm_dfs = process_files_in_parallel(crm_file_paths, processor_names=filtered_processors, is_crm=True, save_clean=True, transaction_type=transaction_type, regulation=regulation,
                                                  lists_dir=dirs['lists_dir'], processed_unmatched_shifted_deposits_dir=dirs['processed_unmatched_shifted_deposits_dir'], processed_crm_dir=dirs['processed_crm_dir'])
    crm_end = time.time()
    print(f"CRM processing for {regulation.upper()} {transaction_type} took {crm_end - crm_start:.2f} seconds")

    proc_start = time.time()  # Timing for processors
    # Process processors (only if file exists, but use filtered_processors to align with CRM)
    processor_file_paths = []
    for proc in filtered_processors:
        for ext in ['xlsx', 'csv', 'xls']:
            proc_file = dirs['processor_dir'] / f"{proc}_{date_str}.{ext}"
            if proc_file.exists():
                processor_file_paths.append(proc_file)
                break
        else:
            processor_file_paths.append(None)

    processed_proc_dfs = process_files_in_parallel(processor_file_paths, processor_names=filtered_processors, is_crm=False, save_clean=True, transaction_type=transaction_type, regulation=regulation,
                                                   processed_processor_dir=dirs['processed_processor_dir'])
    proc_end = time.time()
    print(f"Processor processing for {regulation.upper()} {transaction_type} took {proc_end - proc_start:.2f} seconds")

    # Special combine for zotapay and paymentasia for withdrawals
    if transaction_type == 'withdrawal' and 'zotapay' in filtered_processors and 'paymentasia' in filtered_processors:
        zotapay_file = dirs['processed_processor_dir'] / "zotapay" / date_str / "zotapay_withdrawals.xlsx"
        paymentasia_file = dirs['processed_processor_dir'] / "paymentasia" / date_str / "paymentasia_withdrawals.xlsx"
        combined_out_dir = dirs['processed_processor_dir'] / "zotapay_paymentasia" / date_str
        combined_out_file = combined_out_dir / "zotapay_paymentasia_withdrawals.xlsx"
        zota_df = pd.read_excel(zotapay_file) if zotapay_file.exists() else pd.DataFrame()
        pa_df = pd.read_excel(paymentasia_file) if paymentasia_file.exists() else pd.DataFrame()
        combined_df = pd.concat([zota_df, pa_df], ignore_index=True)
        if not combined_df.empty:
            combined_out_dir.mkdir(parents=True, exist_ok=True)
            combined_df.to_excel(combined_out_file, index=False)
            print(f"Combined Zotapay + PaymentAsia withdrawals saved to {combined_out_file}")

    combine_start = time.time()  # Timing for combine
    # Combine using filtered_processors
    extra_processors = ['zotapay_paymentasia'] if transaction_type == 'withdrawal' and 'zotapay' in filtered_processors and 'paymentasia' in filtered_processors else []
    combine_processed_files(
        date_str,
        filtered_processors,
        processed_crm_dir=dirs['processed_crm_dir'],
        processed_proc_dir=dirs['processed_processor_dir'],
        out_crm_dir=dirs['combined_crm_dir'],
        out_proc_dir=dirs['processed_processor_dir'] / "combined",
        transaction_type=transaction_type,
        regulation=regulation,
        crm_dir=dirs['crm_dir'],  # Added this to pass the per-reg CRM dir
        extra_processors=extra_processors
    )
    combine_end = time.time()
    print(f"Combining for {regulation.upper()} {transaction_type} took {combine_end - combine_start:.2f} seconds")

    end_time = time.time()  # Total timing end
    print(f"Preprocessed and combined {transaction_type}s for {regulation.upper()} regulation saved successfully. Total time: {end_time - start_time:.2f} seconds.")

# Run for both ROW and UK, deposits and withdrawals
if __name__ == "__main__":
    overall_start = time.time()  # Overall timing
    for reg in ['row', 'uk']:
        processors = row_processors if reg == 'row' else uk_processors
        dirs = setup_regulation_structure(reg, processors)  # Setup once per regulation
        preprocess_for_regulation(reg, 'deposit', dirs=dirs)
        preprocess_for_regulation(reg, 'withdrawal', dirs=dirs)
    overall_end = time.time()
    print(f"Overall processing time: {overall_end - overall_start:.2f} seconds")

    # Run deposits matching after preprocessing
    match_deposits_for_date(date_str)