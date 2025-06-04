from config import CRM_DIR, PROCESSOR_DIR, DATA_DIR
from pathlib import Path
import pandas as pd
from src.preprocess import process_files_in_parallel, PROCESSED_CRM_DIR, PROCESSED_PROCESSOR_DIR
from src.withdrawals_matcher import ReconciliationEngine  # Updated import
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('TrainingGenerator')

# --- Configuration ---
processor_name = "safecharge"
date = "2025-05-05"
crm_path = CRM_DIR / f"crm_{date}.xlsx"
processor_path = PROCESSOR_DIR / f"{processor_name}_{date}.xlsx"
output_path = DATA_DIR / "training_dataset" / f"training_dataset_{date}.csv"

# --- Preprocess ---
logger.info("Preprocessing CRM file...")
process_files_in_parallel([crm_path], processor_name=processor_name, is_crm=True, save_clean=True, transaction_type="withdrawal")
logger.info("Preprocessing processor file...")
process_files_in_parallel([processor_path], processor_name=processor_name, is_crm=False, save_clean=True, transaction_type="withdrawal")

# --- Load processed files ---
logger.info("Loading processed files...")
crm_df = pd.read_excel(PROCESSED_CRM_DIR / processor_name / date / f"{processor_name}_withdrawals.xlsx")
processor_df = pd.read_excel(PROCESSED_PROCESSOR_DIR / processor_name / date / f"{processor_name}_withdrawals.xlsx")

# --- Format CRM data ---
logger.info("Formatting CRM data...")
crm_df['crm_date'] = pd.to_datetime(crm_df['Created On']).dt.date
crm_df['crm_email'] = crm_df['Email (Account) (Account)'].fillna('').astype(str)
crm_df['crm_firstname'] = crm_df['First Name (Account) (Account)'].fillna('')
crm_df['crm_lastname'] = crm_df['Last Name (Account) (Account)'].fillna('')
crm_df['crm_last4'] = crm_df['CC Last 4 Digits'].fillna(0).astype(int).astype(str).str.zfill(4)
crm_df['crm_currency'] = crm_df['Currency'].replace({'US Dollar': 'USD'})
crm_df['crm_amount'] = pd.to_numeric(crm_df['Amount'], errors='coerce').abs()
crm_df['crm_processor_name'] = crm_df['PSP name']

# --- Format processor data ---
logger.info("Formatting processor data...")
processor_df['proc_date'] = pd.to_datetime(processor_df['date']).dt.date
processor_df['proc_emails'] = processor_df['email'].fillna('').astype(str)
processor_df['proc_last4_digits'] = processor_df['last_4cc'].astype(str).str.zfill(4).str[-4:]
processor_df['proc_currency'] = processor_df['currency']
processor_df['proc_total_amount'] = pd.to_numeric(processor_df['amount'], errors='coerce').abs()

# --- Load exchange rates ---
logger.info("Loading exchange rates...")
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
config = {
    'max_combo': 6,
    'tolerance': 0.01,
    'email_threshold': 0.7,
    'enable_diagnostics': True,
    'log_level': logging.DEBUG
}

engine = ReconciliationEngine(exchange_rate_map, config=config)
logger.info(f"Starting reconciliation for {len(crm_df)} CRM rows and {len(processor_df)} processor rows...")
matches = engine.match_withdrawals(crm_df, processor_df)

# --- Generate report and diagnostics ---
logger.info("Generating reconciliation report...")
report = engine.generate_report()
print("\n" + "="*80)
print(report)
print("="*80 + "\n")

# --- Save results ---
logger.info("Saving results...")
output_path.parent.mkdir(parents=True, exist_ok=True)
matches_df = pd.DataFrame(matches)

# Save full results
matches_df.to_csv(output_path, index=False)

# Save diagnostics separately
if engine.diagnostics:
    diag_path = output_path.with_name(f"diagnostics_{date}.json")
    pd.DataFrame(engine.diagnostics).to_json(diag_path, orient='records', indent=2)
    logger.info(f"Saved diagnostics to {diag_path}")

logger.info(f"✅ Saved {len(matches)} rows (matched + unmatched) to {output_path}")
logger.info(f"Metrics: {engine.metrics}")
