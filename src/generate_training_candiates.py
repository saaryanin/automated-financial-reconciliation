from config import CRM_DIR, PROCESSOR_DIR, DATA_DIR
from pathlib import Path
import pandas as pd
from src.preprocess import process_files_in_parallel, PROCESSED_CRM_DIR, PROCESSED_PROCESSOR_DIR
from src.withdrawals_matcher import match_withdrawals

# --- Configuration ---
processor_name = "safecharge"
date = "2025-05-05"
crm_path = CRM_DIR / f"crm_{date}.xlsx"
processor_path = PROCESSOR_DIR / f"{processor_name}_{date}.xlsx"
output_path = DATA_DIR / "training_dataset" / f"training_dataset_{date}.csv"

# --- Preprocess ---
process_files_in_parallel([crm_path], processor_name=processor_name, is_crm=True, save_clean=True, transaction_type="withdrawal")
process_files_in_parallel([processor_path], processor_name=processor_name, is_crm=False, save_clean=True, transaction_type="withdrawal")

# --- Load processed files ---
crm_df = pd.read_excel(PROCESSED_CRM_DIR / processor_name / date / f"{processor_name}_withdrawals.xlsx")
processor_df = pd.read_excel(PROCESSED_PROCESSOR_DIR / processor_name / date / f"{processor_name}_withdrawals.xlsx")

# --- Format ---
crm_df['crm_date'] = pd.to_datetime(crm_df['Created On']).dt.date
crm_df['crm_email'] = crm_df['Email (Account) (Account)'].fillna('').astype(str)
crm_df['crm_firstname'] = crm_df['First Name (Account) (Account)'].fillna('')
crm_df['crm_lastname'] = crm_df['Last Name (Account) (Account)'].fillna('')
crm_df['crm_last4'] = crm_df['CC Last 4 Digits'].fillna(0).astype(int).astype(str).str.zfill(4)
crm_df['crm_currency'] = crm_df['Currency'].replace({'US Dollar': 'USD'})
crm_df['crm_amount'] = pd.to_numeric(crm_df['Amount'], errors='coerce').abs()
crm_df['crm_processor_name'] = crm_df['PSP name']

processor_df['proc_date'] = pd.to_datetime(processor_df['date']).dt.date
processor_df['actual_processor'] = processor_name.capitalize()
processor_df['proc_emails'] = processor_df['email'].fillna('').astype(str)
processor_df['firstname'] = ''
processor_df['lastname'] = ''
processor_df['proc_last4_digits'] = processor_df['last_4cc'].astype(str).str.zfill(4).str[-4:]
processor_df['proc_currency'] = processor_df['currency']
processor_df['proc_total_amount'] = pd.to_numeric(processor_df['amount'], errors='coerce').abs()

# --- Match ---
matches = match_withdrawals(crm_df, processor_df)

# --- Save ---
output_path.parent.mkdir(parents=True, exist_ok=True)
pd.DataFrame(matches).to_csv(output_path, index=False)
print(f"✅ Saved {len(matches)} rows (matched + unmatched) to {output_path}")
