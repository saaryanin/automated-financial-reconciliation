# testing_uk_regulation.py (updated to append previous unmatched shifted deposits to raw CRM for deposits)
# testing_uk_regulation.py (updated)
import pandas as pd
from pathlib import Path
import shutil
import src.config as config
from src.preprocess_test import load_crm_file, load_processor_file, combine_processed_files, process_files_in_parallel, PSP_NAME_MAP, process_crm_subset, extract_crm_transaction_id
from concurrent.futures import ThreadPoolExecutor
from src.config import BASE_DIR, TEMP_DIR
import time
from src.utils import categorize_regulation, get_previous_business_day
from src.deposits_matcher_test import match_deposits_for_date
from src.shifts_handler_test import main as handle_shifts
from src.withdrawals_matcher_test import match_withdrawals_for_date,run_cross_processor_matching
from src.cross_regulation_matcher import run_cross_regulation_matching
def setup_regulation_structure(regulation, processors):
    start_time = time.time()
    dirs = config.setup_dirs_for_reg(regulation, create=True)
    shared_crm_filepath = BASE_DIR / "data" / "crm_reports" / f"crm_{date_str}.xlsx"
    reg_crm_filepath = dirs['crm_dir'] / shared_crm_filepath.name
    if shared_crm_filepath.exists() and not reg_crm_filepath.exists():
        shutil.copy(shared_crm_filepath, reg_crm_filepath)
    elif not shared_crm_filepath.exists():
        print(f"CRM file not found at {shared_crm_filepath}")
        exit(1)
    shared_processor_dir = BASE_DIR / "data" / "processor_reports"
    if shared_processor_dir.exists():
        for proc_file in shared_processor_dir.glob("*"):
            proc_name = proc_file.stem.split('_')[0].lower()
            if proc_name in processors:
                target_file = dirs['processor_dir'] / proc_file.name
                if not target_file.exists():
                    shutil.copy(proc_file, target_file)
    if regulation == 'uk':
        shared_processors = ['paypal', 'powercash', 'shift4', 'skrill', 'neteller', 'trustpayments']
        for proc in shared_processors:
            for ext in ['xlsx', 'csv', 'xls']:
                proc_file = shared_processor_dir / f"{proc}_{date_str}.{ext}"
                if proc_file.exists():
                    target_file = dirs['processor_dir'] / proc_file.name
                    if not target_file.exists():
                        shutil.copy(proc_file, target_file)
                    break
    end_time = time.time()
    print(f"Setup for {regulation.upper()} took {end_time - start_time:.2f} seconds")
    return {
        **dirs,
        'crm_filepath': reg_crm_filepath
    }
date_str = '2025-10-21'
row_processors = [
    'paypal', 'safecharge', 'powercash', 'shift4', 'skrill', 'neteller',
    'trustpayments', 'zotapay', 'bitpay', 'ezeebill', 'paymentasia', 'bridgerpay'
]
uk_processors = [
    'safechargeuk', 'barclays', 'barclaycard'
]
def preprocess_for_regulation(regulation, transaction_type='deposit', dirs=None):
    start_time = time.time()
    processors = row_processors if regulation == 'row' else uk_processors
    if dirs is None:
        dirs = setup_regulation_structure(regulation, processors)
    crm_df = pd.read_excel(dirs['crm_filepath'], engine="openpyxl")
    crm_df.columns = crm_df.columns.str.strip()
    if transaction_type == 'deposit':
        previous_date_str = get_previous_business_day(date_str)
        prev_unmatched_path = dirs['lists_dir'] / previous_date_str / f"{regulation}_unmatched_shifted_deposits.xlsx"
        if prev_unmatched_path.exists():
            prev_df = pd.read_excel(prev_unmatched_path, engine="openpyxl")
            prev_df.columns = prev_df.columns.str.strip()
            crm_df = pd.concat([crm_df, prev_df], ignore_index=True)
            print(f"Appended {len(prev_df)} rows from previous unmatched shifted deposits for {regulation}")
    crm_df['regulation'] = crm_df['Site (Account) (Account)'].apply(categorize_regulation)
    if regulation == 'row':
        row_regs = ['mauritius', 'cyprus', 'australia']
        crm_df = crm_df[crm_df['regulation'].isin(row_regs)]
        mask_aus = crm_df['regulation'] == 'australia'
        mask_psp = crm_df["PSP name"].str.lower().isin(['paypal', 'inpendium'])
        crm_df = crm_df[~(mask_aus & mask_psp)]
    elif regulation == 'uk':
        crm_df = crm_df[crm_df['regulation'] == 'uk']
    crm_df["PSP name"] = crm_df["PSP name"].astype(str).str.strip().str.lower().replace(PSP_NAME_MAP)
    if regulation == 'uk':
        crm_df["PSP name"] = crm_df["PSP name"].replace({'safecharge': 'safechargeuk'})
    # Override to Neteller if Method Of Payment is "Neteller", regardless of PSP name
    neteller_mask = crm_df["Method of Payment"].astype(str).str.strip().str.lower() == "neteller"
    crm_df.loc[neteller_mask, "PSP name"] = "neteller"
    name_mask = crm_df["Name"].str.lower() == transaction_type
    unique_psps = set(crm_df[name_mask]["PSP name"].dropna().unique())
    filtered_processors = [p for p in processors if p in unique_psps]
    if regulation == 'uk':
        additional_psps = [p for p in row_processors if p not in ['safecharge'] and p in unique_psps]
        filtered_processors += additional_psps
    filtered_processors = list(set(filtered_processors)) # Dedup
    potential_processors = processors
    if regulation == 'uk':
        potential_processors += [p for p in row_processors if p not in ['safecharge']]
    for proc in potential_processors:
        for ext in ['xlsx', 'csv', 'xls']:
            proc_file = dirs['processor_dir'] / f"{proc}_{date_str}.{ext}"
            if proc_file.exists() and proc not in filtered_processors:
                filtered_processors.append(proc)
                break
    crm_start = time.time()
    processed_crm_dfs = []
    for proc in filtered_processors:
        mask = crm_df["Name"].str.lower() == transaction_type
        psp_mask = crm_df["PSP name"] == proc
        subset = crm_df[mask & psp_mask].copy()
        if regulation == 'uk' and proc == 'safechargeuk':
            subset["PSP name"] = 'safecharge'
        processed_subset = process_crm_subset(subset, proc, regulation, transaction_type, True, dirs['processed_crm_dir'], date_str)
        if processed_subset is not None:
            processed_crm_dfs.append(processed_subset)
    crm_end = time.time()
    print(f"CRM processing for {regulation.upper()} {transaction_type} took {crm_end - crm_start:.2f} seconds")
    proc_start = time.time()
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
    combine_start = time.time()
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
        crm_dir=dirs['crm_dir'],
        extra_processors=extra_processors
    )
    combine_end = time.time()
    print(f"Combining for {regulation.upper()} {transaction_type} took {combine_end - combine_start:.2f} seconds")
    end_time = time.time()
    print(f"Preprocessed and combined {transaction_type}s for {regulation.upper()} regulation saved successfully. Total time: {end_time - start_time:.2f} seconds.")
if __name__ == "__main__":
    overall_start = time.time()
    for reg in ['row', 'uk']:
        processors = row_processors if reg == 'row' else uk_processors
        dirs = setup_regulation_structure(reg, processors)
        preprocess_for_regulation(reg, 'deposit', dirs=dirs)
        preprocess_for_regulation(reg, 'withdrawal', dirs=dirs)
    overall_end = time.time()
    print(f"Overall processing time: {overall_end - overall_start:.2f} seconds")
    match_deposits_for_date(date_str)
    matched_sums = handle_shifts(date_str)
    if matched_sums:
        print("Matched Shifted Deposits by Currency:")
        for reg, sums in matched_sums.items():
            print(f"{reg.upper()}:")
            for currency, amount in sums.items():
                print(f" {currency}: {amount}")
    rates_path = BASE_DIR / "data" / "rates" / f"rates_{date_str}.csv"
    if rates_path.exists():
        rates_df = pd.read_csv(rates_path)
        rates_df['from_currency'] = rates_df['from_currency'].str.strip()
        rates_df['to_currency'] = rates_df['to_currency'].str.strip()
        exchange_rate_map = {
            (row['from_currency'], row['to_currency']): row['rate']
            for _, row in rates_df.iterrows()
        }
    else:
        exchange_rate_map = {}
        print("No rates file found; using empty exchange rate map.")
    match_withdrawals_for_date(date_str, exchange_rate_map)
    run_cross_regulation_matching(date_str, exchange_rate_map)
    run_cross_processor_matching(date_str, exchange_rate_map)