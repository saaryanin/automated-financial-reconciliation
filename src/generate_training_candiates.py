from config import CRM_DIR, PROCESSOR_DIR, DATA_DIR
from pathlib import Path
import pandas as pd
import re
import os
from src.preprocess import process_files_in_parallel, PROCESSED_CRM_DIR, PROCESSED_PROCESSOR_DIR
from pathlib import Path

# --- Configuration ---
processor_name = "safecharge"
date = "2025-05-05"
crm_path = CRM_DIR / f"crm_{date}.xlsx"
processor_path = PROCESSOR_DIR / f"safecharge_{date}.xlsx"
output_path = DATA_DIR / "training_dataset" / f"training_dataset_{date}.csv"

# --- CRM LOADING & FILTERING ---
crm_df = pd.read_excel(crm_path)
crm_df = crm_df[crm_df['PSP name'] == 'SafeCharge'].copy()
crm_df['Currency'] = crm_df['Currency'].replace({'US Dollar': 'USD'})

crm_df['crm_date'] = pd.to_datetime(crm_df['Created On']).dt.date
crm_df['crm_email'] = crm_df['Email (Account) (Account)']
crm_df['crm_firstname'] = crm_df['First Name (Account) (Account)']
crm_df['crm_lastname'] = crm_df['Last Name (Account) (Account)']
crm_df['crm_last4'] = crm_df['CC Last 4 Digits'].astype(str).str[-4:]
crm_df['crm_currency'] = crm_df['Currency']
crm_df['crm_amount'] = pd.to_numeric(crm_df['Amount'], errors='coerce')
crm_df['crm_processor_name'] = crm_df['PSP name']

# --- PROCESSOR LOADING & FILTERING ---
processor_df = pd.read_excel(processor_path, skiprows=11)
processor_df = processor_df[
    (processor_df['Transaction Type'].isin(['Credit', 'Voidcheque'])) &
    (processor_df['Transaction Result'] == 'Approved')
].copy()

cancel_indexes = processor_df[processor_df['Transaction Type'] == 'Voidcheque'].index
remove_indexes = set(cancel_indexes) | set(cancel_indexes - 1)
processor_df = processor_df.drop(remove_indexes, errors='ignore')

processor_df['proc_date'] = pd.to_datetime(processor_df['Date']).dt.date
processor_df['actual_processor'] = 'SafeCharge'
processor_df['proc_emails'] = processor_df['Email Address'].fillna('').astype(str)
processor_df['firstname'] = ''  # Placeholder if not available
processor_df['lastname'] = ''   # Placeholder if not available
processor_df['proc_last4_digits'] = processor_df['PAN'].str.extract(r'(\d{4})$')
processor_df['proc_currency'] = processor_df['Currency']
processor_df['proc_total_amount'] = pd.to_numeric(processor_df['Amount'], errors='coerce')

# --- Output placeholder columns ---
training_columns = [
    'crm_date', 'crm_email', 'crm_firstname', 'crm_lastname', 'crm_last4', 'crm_currency', 'crm_amount',
    'crm_processor_name', 'proc_date', 'actual_processor', 'proc_emails', 'firstname', 'lastname',
    'proc_last4_digits', 'proc_currency', 'proc_total_amount',
    'date_match', 'email_similarity', 'name_similarity', 'last4_match', 'currency_match',
    'amount_diff', 'amount_ratio', 'converted', 'combo_len', 'label'
]

empty_output = pd.DataFrame(columns=training_columns)
output_path.parent.mkdir(parents=True, exist_ok=True)
empty_output.to_csv(output_path, index=False)

process_files_in_parallel([crm_path], processor_name=processor_name, is_crm=True, save_clean=True, transaction_type="withdrawal")
process_files_in_parallel([processor_path], processor_name=processor_name, is_crm=False, save_clean=True, transaction_type="withdrawal")

print(f"✅ Created correctly structured training dataset at: {output_path}")
