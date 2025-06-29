from config import CRM_DIR, PROCESSOR_DIR, DATA_DIR
from pathlib import Path
import pandas as pd
from src.preprocess_test import (
    process_files_in_parallel,
    PROCESSED_CRM_DIR, PROCESSED_PROCESSOR_DIR,
    combine_processed_files
)
from src.withdrawals_matcher_test import ReconciliationEngine
from src.utils import (
    logging, setup_logger, load_excel_if_exists, safe_concat, create_cancelled_row, drop_cols, normalize_currency
)

logger = setup_logger('TrainingGenerator', logging.INFO)

date = "2025-03-20"
processors = [
    "safecharge", "paypal", "powercash", "shift4",
    "skrill", "neteller", "bitpay", "zotapay", "paymentasia",
    "trustpayments"
]
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

# --- Preprocess files ---
for proc in processors:
    crm_file = CRM_DIR / f"crm_{date}.xlsx"
    proc_ext = processor_filetypes.get(proc, ".csv")
    processor_file = PROCESSOR_DIR / f"{proc}_{date}{proc_ext}"

    logger.info(f"Preprocessing CRM file for {proc}...")
    process_files_in_parallel([crm_file], processor_name=proc, is_crm=True, save_clean=True, transaction_type="withdrawal")

    if processor_file.exists():
        logger.info(f"Preprocessing processor file for {proc}...")
        process_files_in_parallel([processor_file], processor_name=proc, is_crm=False, save_clean=True, transaction_type="withdrawal")
    else:
        logger.warning(f"Processor file for {proc} not found.")

# --- Combine Zotapay and PaymentAsia files into the special subfolder ---
zotapay_file = PROCESSED_PROCESSOR_DIR / "zotapay" / date / "zotapay_withdrawals.xlsx"
paymentasia_file = PROCESSED_PROCESSOR_DIR / "paymentasia" / date / "paymentasia_withdrawals.xlsx"
combined_out_dir = PROCESSED_PROCESSOR_DIR / "zotapay_paymentasia" / date
combined_out_file = combined_out_dir / "zotapay_paymentasia_withdrawals.xlsx"

zota_df = load_excel_if_exists(zotapay_file)
pa_df = load_excel_if_exists(paymentasia_file)
zota_pa_dfs = [zota_df, pa_df]
if safe_concat(zota_pa_dfs).shape[0]:
    combined_df = safe_concat(zota_pa_dfs, ignore_index=True)
    combined_out_dir.mkdir(parents=True, exist_ok=True)
    combined_df.to_excel(combined_out_file, index=False)
    print(f"✅ Combined Zotapay + PaymentAsia withdrawals saved to {combined_out_file}")
else:
    print("⚠️ No Zotapay or PaymentAsia files found to combine.")

# --- Load exchange rates BEFORE combining processed files ---
rates_path = DATA_DIR / "rates" / f"rates_{date}.csv"
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

# --- NOW combine everything from all subfolders (including zotapay_paymentasia) ---
combine_processed_files(
    date=date,
    processors=processors,
    processed_crm_dir=PROCESSED_CRM_DIR,
    processed_proc_dir=PROCESSED_PROCESSOR_DIR,
    transaction_type="withdrawal",
    exchange_rate_map=exchange_rate_map
)

# --- Load your fully combined files for matching ---
combined_crm_path = PROCESSED_CRM_DIR / "combined" / date / "combined_crm_withdrawals.xlsx"
combined_proc_path = PROCESSED_PROCESSOR_DIR / "combined" / date / "combined_processor_withdrawals.xlsx"

crm_df = load_excel_if_exists(combined_crm_path)
processor_df = load_excel_if_exists(combined_proc_path)

# --- PATCH Combined CRM Columns ---
if 'Currency' in crm_df.columns:
    crm_df['crm_currency'] = crm_df['Currency'].map(normalize_currency)
elif 'crm_currency' not in crm_df.columns:
    crm_df['crm_currency'] = ''

if 'Amount' in crm_df.columns:
    crm_df['crm_amount'] = pd.to_numeric(crm_df['Amount'], errors='coerce').abs()
elif 'crm_amount' not in crm_df.columns:
    crm_df['crm_amount'] = 0.0

if 'CC Last 4 Digits' in crm_df.columns:
    crm_df['crm_last4'] = crm_df['CC Last 4 Digits'].fillna(0).astype(int).astype(str).str.zfill(4)
elif 'crm_last4' not in crm_df.columns:
    crm_df['crm_last4'] = ''

if 'Email (Account) (Account)' in crm_df.columns:
    crm_df['crm_email'] = crm_df['Email (Account) (Account)'].fillna('').astype(str)
elif 'crm_email' not in crm_df.columns:
    crm_df['crm_email'] = ''

if 'First Name (Account) (Account)' in crm_df.columns:
    crm_df['crm_firstname'] = crm_df['First Name (Account) (Account)'].fillna('')
elif 'crm_firstname' not in crm_df.columns:
    crm_df['crm_firstname'] = ''

if 'Last Name (Account) (Account)' in crm_df.columns:
    crm_df['crm_lastname'] = crm_df['Last Name (Account) (Account)'].fillna('')
elif 'crm_lastname' not in crm_df.columns:
    crm_df['crm_lastname'] = ''

if 'tp' in crm_df.columns:
    crm_df['crm_tp'] = crm_df['tp'].fillna('')
elif 'crm_tp' not in crm_df.columns:
    crm_df['crm_tp'] = ''

if 'PSP name' in crm_df.columns:
    crm_df['crm_processor_name'] = crm_df['PSP name'].str.strip().str.lower()
elif 'crm_processor_name' not in crm_df.columns:
    crm_df['crm_processor_name'] = ''

# --- PATCH Processor Combined Columns ---
if 'date' in processor_df.columns:
    processor_df['proc_date'] = pd.to_datetime(processor_df['date'], errors='coerce').dt.date
elif 'proc_date' not in processor_df.columns:
    processor_df['proc_date'] = pd.NaT

if 'last_4cc' in processor_df.columns:
    processor_df['proc_last4_digits'] = (
        processor_df['last_4cc'].fillna(0).astype(int).astype(str).str.zfill(4)
    )
elif 'proc_last4_digits' not in processor_df.columns:
    processor_df['proc_last4_digits'] = ''

if 'email' in processor_df.columns:
    processor_df['proc_emails'] = processor_df['email'].fillna('').astype(str)
elif 'proc_emails' not in processor_df.columns:
    processor_df['proc_emails'] = ''

if 'currency' in processor_df.columns:
    processor_df['proc_currency'] = processor_df['currency']
elif 'proc_currency' not in processor_df.columns:
    processor_df['proc_currency'] = ''

if 'amount' in processor_df.columns:
    processor_df['proc_total_amount'] = pd.to_numeric(processor_df['amount'], errors='coerce').abs()
elif 'proc_total_amount' not in processor_df.columns:
    processor_df['proc_total_amount'] = 0.0

if 'processor_name' in processor_df.columns:
    processor_df['proc_processor_name'] = processor_df['processor_name'].str.strip().str.lower()
elif 'proc_processor_name' not in processor_df.columns:
    processor_df['proc_processor_name'] = ''



if crm_df is None or processor_df is None:
    logger.error("No valid combined CRM or processor files found. Exiting.")
    exit(1)

# --- The rest is unchanged! ---

logger.info("Configuring reconciliation engine...")
engine = ReconciliationEngine(exchange_rate_map, config={
    'max_combo': 20,
    'tolerance': 0.02,
    'email_threshold': 0.5,
    'enable_diagnostics': True,
    'log_level': logging.DEBUG
})

non_cancelled_mask = crm_df['Name'].str.lower() != 'withdrawal cancelled'
crm_df_non_cancelled = crm_df[non_cancelled_mask]

logger.info(f"Starting reconciliation for {len(crm_df_non_cancelled)} CRM rows and {len(processor_df)} processor rows...")
matches = engine.match_withdrawals(crm_df_non_cancelled, processor_df)
report = engine.generate_report()
print("\n" + "="*80)
print(report)
print("="*80 + "\n")

output_path = DATA_DIR / "training_dataset" / f"training_dataset_{date}.csv"
output_path.parent.mkdir(parents=True, exist_ok=True)

matches_df = pd.DataFrame(matches)

# Add 'Withdrawal Cancelled' CRM rows
cancelled_mask = crm_df['Name'].str.lower() == 'withdrawal cancelled'
cancelled_rows = crm_df[cancelled_mask]
cancelled_outputs = [create_cancelled_row(row) for _, row in cancelled_rows.iterrows()]

if cancelled_outputs:
    matches_df = safe_concat([matches_df, pd.DataFrame(cancelled_outputs)], ignore_index=True)

matches_df = drop_cols(matches_df, ['matched_proc_indices'])
matches_df.to_csv(output_path, index=False)

if engine.diagnostics:
    diag_path = output_path.with_name(f"diagnostics_{date}.json")
    pd.DataFrame(engine.diagnostics).to_json(diag_path, orient='records', indent=2)
    logger.info(f"Saved diagnostics to {diag_path}")

logger.info(f"✅ Saved {len(matches_df)} rows to {output_path}")
logger.info(f"Metrics: {engine.metrics}")
