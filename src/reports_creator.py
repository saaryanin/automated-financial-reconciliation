import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

import time
import warnings
from src.preprocess_test import process_files_in_parallel, combine_processed_files,append_unmatched_to_combined
from src.config import CRM_DIR, PROCESSOR_DIR, DATA_DIR, PROCESSED_CRM_DIR, PROCESSED_PROCESSOR_DIR, LISTS_DIR
import pandas as pd
import numpy as np
from src.withdrawals_matcher_test import ReconciliationEngine
from src.utils import (
    logging as utils_logging, setup_logger, load_excel_if_exists, safe_concat, drop_cols
)
from src.shifts_handler import main as handle_shifts
import sys
from src.processor_renamer import run_renamer  # Import the renamer
import re
from collections import defaultdict

# Suppress openpyxl and pandas warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
warnings.filterwarnings("ignore", category=FutureWarning, module="pandas")

start_time = time.time()

# --- Configuration ---
DATE = sys.argv[1] if len(sys.argv) > 1 else "2025-09-02"  # Use command-line arg or default
PROCESSORS = ["paypal", "safecharge", "powercash", "shift4", "skrill", "trustpayments", "neteller", "zotapay", "bitpay", "ezeebill", "paymentasia"]

# --- Activate Renamer with Forced Date ---
run_renamer(forced_date=DATE)

# --- Step 1: Gather files (use DATE for all) ---
crm_files = list(CRM_DIR.glob(f"crm_{DATE}.xlsx"))
paypal_files = list(PROCESSOR_DIR.glob(f"paypal_*{DATE}*.csv"))
safecharge_files = list(PROCESSOR_DIR.glob(f"safecharge_*{DATE}*.xlsx"))
powercash_files = list(PROCESSOR_DIR.glob(f"powercash_*{DATE}*.csv"))
shift4_files = list(PROCESSOR_DIR.glob(f"shift4_*{DATE}*.csv"))
skrill_files = list(PROCESSOR_DIR.glob(f"skrill_*{DATE}*.csv"))
trustpayments_files = list(PROCESSOR_DIR.glob(f"trustpayments_*{DATE}*.csv"))
neteller_files = list(PROCESSOR_DIR.glob(f"neteller_{DATE}.csv"))
zotapay_files = list(PROCESSOR_DIR.glob(f"zotapay_{DATE}.csv*"))
bitpay_files = list(PROCESSOR_DIR.glob(f"bitpay_{DATE}.csv"))
ezeebill_files = list(PROCESSOR_DIR.glob(f"ezeebill_{DATE}.csv"))
paymentasia_deposits_files = list(PROCESSOR_DIR.glob(f"paymentasia_deposits_{DATE}.csv"))
paymentasia_withdrawals_files = list(PROCESSOR_DIR.glob(f"paymentasia_withdrawals_{DATE}.csv"))

if not any([paypal_files, safecharge_files, powercash_files, shift4_files, skrill_files,
            trustpayments_files, neteller_files, zotapay_files, bitpay_files, ezeebill_files,
            paymentasia_deposits_files, paymentasia_withdrawals_files]):
    raise FileNotFoundError(f"No processor raw files found for the given date {DATE}.")

# --- Deposits Processing ---

# --- Step 2: Preprocess processor files for deposits ---
process_files_in_parallel(paypal_files, processor_name="paypal", is_crm=False, transaction_type="deposit")
process_files_in_parallel(safecharge_files, processor_name="safecharge", is_crm=False, transaction_type="deposit")
process_files_in_parallel(powercash_files, processor_name="powercash", is_crm=False, transaction_type="deposit")
process_files_in_parallel(shift4_files, processor_name="shift4", is_crm=False, transaction_type="deposit")
process_files_in_parallel(skrill_files, processor_name="skrill", is_crm=False, transaction_type="deposit")
process_files_in_parallel(trustpayments_files, processor_name="trustpayments", is_crm=False, transaction_type="deposit")
process_files_in_parallel(neteller_files, processor_name="neteller", is_crm=False, transaction_type="deposit")
process_files_in_parallel(zotapay_files, processor_name="zotapay", is_crm=False, transaction_type="deposit")
process_files_in_parallel(bitpay_files, processor_name="bitpay", is_crm=False, transaction_type="deposit")
process_files_in_parallel(ezeebill_files, processor_name="ezeebill", is_crm=False, transaction_type="deposit")
process_files_in_parallel(paymentasia_deposits_files, processor_name="paymentasia", is_crm=False, transaction_type="deposit")

# --- Step 3: Preprocess CRM files for deposits ---
for processor in PROCESSORS:
    process_files_in_parallel(crm_files, processor_name=processor, is_crm=True, transaction_type="deposit")

# --- Step 3.5: Combine processed files for deposits ---
combine_processed_files(
    date=DATE,
    processors=PROCESSORS,
    transaction_type="deposit",
    exchange_rate_map={},  # Load rates if needed, else empty dict
)
unmatched_shifted_path = str(PROCESSED_CRM_DIR / "unmatched_shifted_deposits" / DATE / "unmatched_shifted_deposits.xlsx")
append_unmatched_to_combined(DATE, unmatched_shifted_path)


# --- Step 4: Generate deposits matching report ---
combined_crm_path_deposits = PROCESSED_CRM_DIR / "combined" / DATE / "combined_crm_deposits.xlsx"
combined_proc_path_deposits = PROCESSED_PROCESSOR_DIR / "combined" / DATE / "combined_processor_deposits.xlsx"

crm_df_deposits = pd.read_excel(combined_crm_path_deposits, dtype={'crm_transaction_id': str})
proc_df_deposits = pd.read_excel(combined_proc_path_deposits, dtype={'proc_transaction_id': str})

# Match by transaction_id (exact match)
matched_deposits = pd.merge(crm_df_deposits, proc_df_deposits, left_on='crm_transaction_id', right_on='proc_transaction_id', how='inner', suffixes=('_crm', '_proc'))
matched_deposits['match_status'] = 1

# Unmatched CRM: add proc columns as NaN
unmatched_crm_deposits = crm_df_deposits[~crm_df_deposits['crm_transaction_id'].isin(matched_deposits['crm_transaction_id'])].copy()
if not unmatched_crm_deposits.empty:
    for col in proc_df_deposits.columns:
        if col not in unmatched_crm_deposits.columns:
            unmatched_crm_deposits.loc[:, col] = np.nan
    unmatched_crm_deposits.loc[:, 'match_status'] = 0

# Unmatched Processor: add crm columns as NaN
unmatched_proc_deposits = proc_df_deposits[~proc_df_deposits['proc_transaction_id'].isin(matched_deposits['proc_transaction_id'])].copy()
if not unmatched_proc_deposits.empty:
    for col in crm_df_deposits.columns:
        if col not in unmatched_proc_deposits.columns:
            unmatched_proc_deposits.loc[:, col] = np.nan
    unmatched_proc_deposits.loc[:, 'match_status'] = 0

# Combine all: matched + unmatched_crm + unmatched_proc
dfs = [df for df in [matched_deposits, unmatched_crm_deposits, unmatched_proc_deposits] if not df.empty]
if dfs:
    all_rows_deposits = pd.concat(dfs, ignore_index=True)
    if 'crm_type' in all_rows_deposits.columns:
        all_rows_deposits = all_rows_deposits.drop(columns=['crm_type'])
else:
    all_rows_deposits = pd.DataFrame()
all_rows_deposits = all_rows_deposits.sort_values(by='match_status', ascending=False)


# Save to Excel
report_dir = LISTS_DIR / DATE
report_dir.mkdir(parents=True, exist_ok=True)
report_path_deposits = report_dir / "deposits_matching.xlsx"

with pd.ExcelWriter(report_path_deposits, engine='openpyxl') as writer:
    all_rows_deposits.to_excel(writer, sheet_name='Deposits_Matching', index=False)

print(f" Deposits matching report saved to {report_path_deposits}")

# --- Apply Shifts Handler ---
matched_sums = handle_shifts(DATE)
if matched_sums:
    print("Matched Shifted Deposits by Currency:")
    for currency, amount in matched_sums.items():
        print(f"{currency}: {amount}")

# --- Withdrawals Processing ---

logger = setup_logger('TrainingGenerator', logging.INFO)

processor_filetypes = {
    "safecharge": ".xlsx",
    "paypal": ".csv",
    "powercash": ".csv",
    "shift4": ".csv",
    "skrill": ".csv",
    "neteller": ".csv",
    "bitpay": ".csv",
    "zotapay": ".csv",
    "paymentasia": ".csv",
    "trustpayments": ".csv"
}

# --- Preprocess files for withdrawals ---
for proc in PROCESSORS:
    crm_file = CRM_DIR / f"crm_{DATE}.xlsx"
    proc_ext = processor_filetypes.get(proc, ".csv")
    processor_file = PROCESSOR_DIR / f"{proc}_{DATE}{proc_ext}"

    print(f"Preprocessing CRM file for {proc}...")
    process_files_in_parallel([crm_file], processor_name=proc, is_crm=True, save_clean=True, transaction_type="withdrawal")

    if processor_file.exists():
        print(f"Preprocessing processor file for {proc}...")
        process_files_in_parallel([processor_file], processor_name=proc, is_crm=False, save_clean=True, transaction_type="withdrawal")
    else:
        print(f"Processor file for {proc} not found.")

# --- Combine Zotapay and PaymentAsia files for withdrawals ---
zotapay_file = PROCESSED_PROCESSOR_DIR / "zotapay" / DATE / "zotapay_withdrawals.xlsx"
paymentasia_file = PROCESSED_PROCESSOR_DIR / "paymentasia" / DATE / "paymentasia_withdrawals.xlsx"
combined_out_dir = PROCESSED_PROCESSOR_DIR / "zotapay_paymentasia" / DATE
combined_out_file = combined_out_dir / "zotapay_paymentasia_withdrawals.xlsx"

zota_df = load_excel_if_exists(zotapay_file)
pa_df = load_excel_if_exists(paymentasia_file)
zota_pa_dfs = [zota_df, pa_df]
if safe_concat(zota_pa_dfs).shape[0]:
    combined_df = safe_concat(zota_pa_dfs, ignore_index=True)
    combined_out_dir.mkdir(parents=True, exist_ok=True)
    combined_df.to_excel(combined_out_file, index=False)
    print(f"Combined Zotapay + PaymentAsia withdrawals saved to {combined_out_file}")
else:
    print("No Zotapay or PaymentAsia files found to combine.")

# --- Load exchange rates ---
rates_path = DATA_DIR / "rates" / f"rates_{DATE}.csv"
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

# --- Combine all processed files for withdrawals ---
combine_processed_files(
    date=DATE,
    processors=PROCESSORS + ['zotapay_paymentasia'],
    processed_crm_dir=PROCESSED_CRM_DIR,
    processed_proc_dir=PROCESSED_PROCESSOR_DIR,
    transaction_type="withdrawal",
    exchange_rate_map=exchange_rate_map
)

# --- Load combined data for withdrawals matching ---
combined_crm_path_withdrawals = PROCESSED_CRM_DIR / "combined" / DATE / "combined_crm_withdrawals.xlsx"
combined_proc_path_withdrawals = PROCESSED_PROCESSOR_DIR / "combined" / DATE / "combined_processor_withdrawals.xlsx"

crm_df_withdrawals = load_excel_if_exists(combined_crm_path_withdrawals)
processor_df_withdrawals = load_excel_if_exists(combined_proc_path_withdrawals)

if crm_df_withdrawals is None or processor_df_withdrawals is None:
    print("No valid combined CRM or processor files found for withdrawals. Skipping.")
else:
    # --- Reconciliation for withdrawals ---
    engine = ReconciliationEngine(
        exchange_rate_map, config={
            'enable_cross_processor': True,
            'enable_warning_flag': True
        }
    )
    crm_df_non_cancelled = crm_df_withdrawals[crm_df_withdrawals['crm_type'].str.lower() != 'withdrawal cancelled']

    matches = engine.match_withdrawals(crm_df_non_cancelled, processor_df_withdrawals)
    report = engine.generate_report()
    print("\n" + "="*80)
    print(report)
    print("="*80 + "\n")

    # --- Prepare output dataframe for withdrawals ---
    matches_df = pd.DataFrame(matches)

    desired_columns = [
        'crm_date', 'crm_email', 'crm_firstname', 'crm_lastname', 'crm_tp', 'crm_last4', 'crm_currency', 'crm_amount',
        'crm_processor_name',
        'regulation',
        'proc_date', 'proc_email', 'proc_tp', 'proc_firstname', 'proc_last_name', 'proc_last4', 'proc_currency',
        'proc_amount', 'proc_amount_crm_currency', 'proc_processor_name',
        'email_similarity_avg', 'last4_match', 'name_fallback_used', 'exact_match_used', 'match_status',
        'payment_status', 'warning', 'comment'
    ]

    matches_df = matches_df[[c for c in desired_columns if c in matches_df.columns]]

    # Parse the report to add comments for Rule 4 warnings
    report = str(report)  # Ensure report is string to avoid AttributeError
    unmatched_by_last4 = defaultdict(list)
    matched_by_last4 = defaultdict(list)
    # Parse unmatched
    for match in re.finditer(r'Row (\d+) breaks Rule 4: Unmatched-processor last4 ([\d.]+) found in CRM last4s', report):
        row_num = int(match.group(1))
        last4 = match.group(2).rstrip('.0')
        unmatched_by_last4[last4].append(row_num)
    # Parse propagation
    for match in re.finditer(r'Row (\d+) breaks Rule 4 propagation: Matching last4 ([\d.]+)', report):
        row_num = int(match.group(1))
        last4 = match.group(2).rstrip('.0')
        matched_by_last4[last4].append(row_num)

    for last4, unmatched_rows in unmatched_by_last4.items():
        matched_rows = matched_by_last4.get(last4, [])
        if matched_rows:
            matched_str = ', '.join([f"row {r}" for r in matched_rows]) if len(matched_rows) > 1 else f"row {matched_rows[0]}"
            comment = f"Matched the same last4 :{last4} in {matched_str}"
            for u_row in unmatched_rows:
                idx = u_row - 1
                current = matches_df.at[idx, 'comment']
                new_com = current + ' . ' + comment if pd.notna(current) and current else comment
                matches_df.at[idx, 'comment'] = new_com

            unmatched_str = ', '.join([f"row {r}" for r in unmatched_rows]) if len(unmatched_rows) > 1 else f"row {unmatched_rows[0]}"
            comment_m = f"Matched the same last4 :{last4} in {unmatched_str}"
            for m_row in matched_rows:
                idx = m_row - 1
                current = matches_df.at[idx, 'comment']
                new_com = current + ' . ' + comment_m if pd.notna(current) and current else comment_m
                matches_df.at[idx, 'comment'] = new_com

    # --- Append cancellations ---
    cancelled = engine.make_cancelled_rows(crm_df_withdrawals)
    if cancelled:
        matches_df = safe_concat([matches_df, pd.DataFrame(cancelled)], ignore_index=True)

    matches_df = drop_cols(matches_df, ['matched_proc_indices'])

    # --- Save withdrawals matching report ---
    report_path_withdrawals = report_dir / "withdrawals_matching.xlsx"
    with pd.ExcelWriter(report_path_withdrawals, engine='openpyxl') as writer:
        matches_df.to_excel(writer, sheet_name='Withdrawals_Matching', index=False)

    print(f"Withdrawals matching report saved to {report_path_withdrawals}")

    # --- Diagnostics JSON ---
    if engine.diagnostics:
        diag_path = report_path_withdrawals.with_name(f"diagnostics_{DATE}.json")
        pd.DataFrame(engine.diagnostics).to_json(diag_path, orient='records', indent=2)
        print(f"Saved diagnostics to {diag_path}")

    print(f"Saved {len(matches_df)} rows for withdrawals")

end_time = time.time()
print(f"\nTotal time: {end_time - start_time:.2f} seconds")