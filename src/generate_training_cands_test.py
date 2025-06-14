from config import CRM_DIR, PROCESSOR_DIR, DATA_DIR
from pathlib import Path
import pandas as pd
from src.preprocess_test import process_files_in_parallel, PROCESSED_CRM_DIR, PROCESSED_PROCESSOR_DIR
from src.withdrawals_matcher_test import ReconciliationEngine
import logging

# Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('TrainingGenerator')

# --- Configuration ---
date = "2025-02-10"
processors = [
    "safecharge", "paypal", "powercash", "shift4",
    "skrill", "neteller", "bitpay", "zotapay", "paymentasia"
]
# Define processor input formats
processor_filetypes = {
    "safecharge": ".xlsx",
    "paypal": ".csv",
    "powercash": ".csv",
    "shift4": ".csv",
    "skrill": ".csv",
    "neteller": ".csv",
    "bitpay": ".csv",
    "zotapay": ".csv",
    "paymentasia": ".csv"
}

# --- Preprocess ---
for proc in processors:
    # CRM is always .xlsx
    crm_file = CRM_DIR / f"crm_{date}.xlsx"
    # Get correct processor file extension
    proc_ext = processor_filetypes.get(proc, ".csv")
    processor_file = PROCESSOR_DIR / f"{proc}_{date}{proc_ext}"

    logger.info(f"Preprocessing CRM file for {proc}...")
    process_files_in_parallel([crm_file], processor_name=proc, is_crm=True, save_clean=True, transaction_type="withdrawal")

    logger.info(f"Preprocessing processor file for {proc}...")
    process_files_in_parallel([processor_file], processor_name=proc, is_crm=False, save_clean=True, transaction_type="withdrawal")


# --- Preprocess ---
for proc in processors:
    logger.info(f"Preprocessing CRM file for {proc}...")
    crm_file = CRM_DIR / f"crm_{date}.xlsx"
    process_files_in_parallel([crm_file], processor_name=proc, is_crm=True, save_clean=True, transaction_type="withdrawal")

    logger.info(f"Preprocessing processor file for {proc}...")
    proc_ext = processor_filetypes.get(proc, ".csv")
    processor_file = PROCESSOR_DIR / f"{proc}_{date}{proc_ext}"
    process_files_in_parallel([processor_file], processor_name=proc, is_crm=False, save_clean=True, transaction_type="withdrawal")


# --- Load processed files ---
crm_dfs, proc_dfs = [], []

for proc in processors:
    crm_file = PROCESSED_CRM_DIR / proc / date / f"{proc}_withdrawals.xlsx"
    proc_file = PROCESSED_PROCESSOR_DIR / proc / date / f"{proc}_withdrawals.xlsx"

    if not crm_file.exists():
        logger.warning(f"Skipping {proc} - CRM file not found")
        continue

    # --- Load CRM ---
    crm_df = pd.read_excel(crm_file)
    crm_df['crm_date'] = pd.to_datetime(crm_df['Created On']).dt.date
    crm_df['crm_email'] = crm_df['Email (Account) (Account)'].fillna('').astype(str)
    crm_df['crm_firstname'] = crm_df['First Name (Account) (Account)'].fillna('')
    crm_df['crm_lastname'] = crm_df['Last Name (Account) (Account)'].fillna('')
    crm_df['crm_tp'] = crm_df['tp'].fillna('')
    crm_df['crm_currency'] = crm_df['Currency'].replace({'US Dollar': 'USD'})
    crm_df['crm_amount'] = pd.to_numeric(crm_df['Amount'], errors='coerce').abs()

    # Handle missing 'CC Last 4 Digits'
    if 'CC Last 4 Digits' in crm_df.columns:
        crm_df['crm_last4'] = (
            crm_df['CC Last 4 Digits'].fillna(0).astype(int).astype(str).str.zfill(4)
        )
    else:
        logger.warning(f"'CC Last 4 Digits' column not found in CRM file for {proc}. Setting empty values.")
        crm_df['crm_last4'] = ''

    # Normalize PSP names
    psp_map = {
        'netteler': 'neteller',
        'skrilll': 'skrill',
        'skrill ': 'skrill',
        'skrll': 'skrill',
        'paypal ': 'paypal',
        'safecharge ': 'safecharge',
        'powercash ': 'powercash',
        'shift4 ': 'shift4',
    }
    crm_df['crm_processor_name'] = crm_df['PSP name'].str.strip().str.lower().replace(psp_map)

    print(f"[{proc.upper()}] CRM rows: {len(crm_df)}, PSPs: {crm_df['crm_processor_name'].unique()}, TPs: {crm_df['crm_tp'].unique()}")
    crm_dfs.append(crm_df)

    # --- Load Processor ---
    if not proc_file.exists():
        logger.warning(f"Processor file for {proc} not found. Continuing with CRM only.")
        proc_df = pd.DataFrame(columns=[
            'proc_date', 'proc_emails', 'proc_tp', 'proc_last4_digits',
            'proc_currency', 'proc_total_amount', 'proc_processor_name',
            'proc_firstname', 'proc_lastname'
        ])
        proc_df['proc_processor_name'] = proc
    else:
        if proc_file.suffix == ".csv":
            proc_df = pd.read_csv(proc_file)
        else:
            proc_df = pd.read_excel(proc_file)

        proc_df['proc_date'] = pd.to_datetime(proc_df['date']).dt.date
        proc_df['proc_emails'] = proc_df['email'].fillna('').astype(str)
        proc_df['proc_tp'] = proc_df['tp'].astype(str).fillna('') if 'tp' in proc_df.columns else ''
        proc_df['proc_last4_digits'] = proc_df['last_4cc'].astype(str).str.zfill(4).str[-4:]
        proc_df['proc_currency'] = proc_df['currency']
        proc_df['proc_total_amount'] = pd.to_numeric(proc_df['amount'], errors='coerce').abs()
        proc_df['proc_processor_name'] = proc_df.get('processor_name', proc)

        proc_df['proc_firstname'] = proc_df['first_name'].fillna('').astype(str) if 'first_name' in proc_df else ''
        proc_df['proc_lastname'] = proc_df['last_name'].fillna('').astype(str) if 'last_name' in proc_df else ''

    proc_dfs.append(proc_df)


# Handle case where no files were found
if not crm_dfs or not proc_dfs:
    logger.error("No valid CRM or processor files found. Exiting.")
    exit(1)


# --- Combine all CRM and Processor data ---
crm_df = pd.concat(crm_dfs, ignore_index=True)
processor_df = pd.concat(proc_dfs, ignore_index=True)

# --- Load exchange rates ---
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
    logger.warning(f"Exchange rates file not found: {rates_path}")
    exchange_rate_map = {}

# --- Configure and run reconciliation ---
logger.info("Configuring reconciliation engine...")
engine = ReconciliationEngine(exchange_rate_map, config={
    'max_combo': 20,
    'tolerance': 0.02,
    'email_threshold': 0.5,
    'enable_diagnostics': True,
    'log_level': logging.DEBUG
})

logger.info(f"Starting reconciliation for {len(crm_df)} CRM rows and {len(processor_df)} processor rows...")
matches = engine.match_withdrawals(crm_df, processor_df)

# --- Generate report and diagnostics ---
report = engine.generate_report()
print("\n" + "="*80)
print(report)
print("="*80 + "\n")

# --- Save results ---
output_path = DATA_DIR / "training_dataset" / f"training_dataset_{date}.csv"
output_path.parent.mkdir(parents=True, exist_ok=True)

matches_df = pd.DataFrame(matches)
matches_df.drop(columns=['matched_proc_indices'], errors='ignore').to_csv(output_path, index=False)

if engine.diagnostics:
    diag_path = output_path.with_name(f"diagnostics_{date}.json")
    pd.DataFrame(engine.diagnostics).to_json(diag_path, orient='records', indent=2)
    logger.info(f"Saved diagnostics to {diag_path}")

logger.info(f"✅ Saved {len(matches)} rows to {output_path}")
logger.info(f"Metrics: {engine.metrics}")
