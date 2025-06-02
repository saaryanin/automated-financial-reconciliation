from config import CRM_DIR, PROCESSOR_DIR, DATA_DIR
from pathlib import Path
import pandas as pd
import numpy as np
from src.preprocess import process_files_in_parallel, PROCESSED_CRM_DIR, PROCESSED_PROCESSOR_DIR
from difflib import SequenceMatcher
from itertools import combinations
from concurrent.futures import ThreadPoolExecutor

# --- Configuration ---
processor_name = "safecharge"
date = "2025-05-05"
crm_path = CRM_DIR / f"crm_{date}.xlsx"
processor_path = PROCESSOR_DIR / f"{processor_name}_{date}.xlsx"
output_path = DATA_DIR / "training_dataset" / f"training_dataset_{date}.csv"

# --- Preprocess both files ---
process_files_in_parallel([crm_path], processor_name=processor_name, is_crm=True, save_clean=True, transaction_type="withdrawal")
process_files_in_parallel([processor_path], processor_name=processor_name, is_crm=False, save_clean=True, transaction_type="withdrawal")

# --- Load preprocessed files ---
processed_crm_path = PROCESSED_CRM_DIR / processor_name / date / f"{processor_name}_withdrawals.xlsx"
processed_processor_path = PROCESSED_PROCESSOR_DIR / processor_name / date / f"{processor_name}_withdrawals.xlsx"

crm_df = pd.read_excel(processed_crm_path)
processor_df = pd.read_excel(processed_processor_path)

# --- Format CRM ---
crm_df['crm_date'] = pd.to_datetime(crm_df['Created On']).dt.date
crm_df['crm_email'] = crm_df['Email (Account) (Account)'].fillna('').astype(str)
crm_df['crm_firstname'] = crm_df['First Name (Account) (Account)'].fillna('')
crm_df['crm_lastname'] = crm_df['Last Name (Account) (Account)'].fillna('')
crm_df['crm_last4'] = crm_df['CC Last 4 Digits'].fillna(0).astype(int).astype(str).str.zfill(4)
crm_df['crm_currency'] = crm_df['Currency'].replace({'US Dollar': 'USD'})
crm_df['crm_amount'] = pd.to_numeric(crm_df['Amount'], errors='coerce').abs()
crm_df['crm_processor_name'] = crm_df['PSP name']

# --- Format Processor ---
processor_df['proc_date'] = pd.to_datetime(processor_df['date']).dt.date
processor_df['actual_processor'] = processor_name.capitalize()
processor_df['proc_emails'] = processor_df['email'].fillna('').astype(str)
processor_df['firstname'] = ''
processor_df['lastname'] = ''
processor_df['proc_last4_digits'] = processor_df['last_4cc'].astype(str).str.zfill(4).str[-4:]
processor_df['proc_currency'] = processor_df['currency']
processor_df['proc_total_amount'] = pd.to_numeric(processor_df['amount'], errors='coerce').abs()

# --- Helper Functions ---
def email_similarity(e1, e2):
    e1 = str(e1).split('@')[0]
    e2 = str(e2).split('@')[0]
    return SequenceMatcher(None, e1, e2).ratio()

def match_crm_row(crm_row):
    crm_email = crm_row['crm_email']
    crm_amount = crm_row['crm_amount']
    crm_last4 = crm_row['crm_last4']
    best_score = 0
    best_combo = None

    for combo_len in [1, 2]:
        for proc_indices in combinations(processor_df.index, combo_len):
            if any(i in used_proc_indices for i in proc_indices):
                continue

            combo_rows = processor_df.loc[list(proc_indices)]
            total = combo_rows['proc_total_amount'].sum()
            if abs(total - crm_amount) > 0.1 * crm_amount:
                continue

            email_scores = [email_similarity(crm_email, r['proc_emails']) for _, r in combo_rows.iterrows()]
            avg_email_score = np.mean(email_scores)

            if avg_email_score >= 0.85 and avg_email_score > best_score:
                best_score = avg_email_score
                best_combo = (combo_rows.copy(), proc_indices, combo_len)

    results = []
    if best_combo:
        combo_rows, indices, combo_len = best_combo
        for _, proc_row in combo_rows.iterrows():
            results.append({
                'crm_date': crm_row['crm_date'],
                'crm_email': crm_row['crm_email'],
                'crm_firstname': crm_row['crm_firstname'],
                'crm_lastname': crm_row['crm_lastname'],
                'crm_last4': crm_row['crm_last4'],
                'crm_currency': crm_row['crm_currency'],
                'crm_amount': crm_row['crm_amount'],
                'crm_processor_name': crm_row['crm_processor_name'],
                'proc_date': proc_row['proc_date'],
                'actual_processor': proc_row['actual_processor'],
                'proc_emails': proc_row['proc_emails'],
                'firstname': '',
                'lastname': '',
                'proc_last4_digits': proc_row['proc_last4_digits'],
                'proc_currency': proc_row['proc_currency'],
                'proc_total_amount': proc_row['proc_total_amount'],
                'date_match': crm_row['crm_date'] == proc_row['proc_date'],
                'email_similarity': round(best_score, 3),
                'name_similarity': 0.0,
                'last4_match': crm_row['crm_last4'] == proc_row['proc_last4_digits'],
                'currency_match': crm_row['crm_currency'] == proc_row['proc_currency'],
                'amount_diff': abs(crm_row['crm_amount'] - combo_rows['proc_total_amount'].sum()),
                'amount_ratio': combo_rows['proc_total_amount'].sum() / crm_row['crm_amount'] if crm_row['crm_amount'] else 0,
                'converted': False,
                'combo_len': combo_len,
                'label': ''
            })
        used_proc_indices.update(indices)
        used_crm_indices.add(crm_row.name)
    return results

# --- Matching Execution (Parallel) ---
matches = []
used_proc_indices = set()
used_crm_indices = set()

with ThreadPoolExecutor() as executor:
    results = list(executor.map(match_crm_row, [row for _, row in crm_df.iterrows()]))
    for res in results:
        matches.extend(res)

# --- Unmatched Processor Rows ---
unmatched_proc_df = processor_df.drop(index=used_proc_indices)
for _, proc_row in unmatched_proc_df.iterrows():
    matches.append({
        'crm_date': '', 'crm_email': '', 'crm_firstname': '', 'crm_lastname': '', 'crm_last4': '',
        'crm_currency': '', 'crm_amount': '', 'crm_processor_name': '',
        'proc_date': proc_row['proc_date'],
        'actual_processor': proc_row['actual_processor'],
        'proc_emails': proc_row['proc_emails'],
        'firstname': '', 'lastname': '',
        'proc_last4_digits': proc_row['proc_last4_digits'],
        'proc_currency': proc_row['proc_currency'],
        'proc_total_amount': proc_row['proc_total_amount'],
        'date_match': '', 'email_similarity': '', 'name_similarity': '', 'last4_match': '', 'currency_match': '',
        'amount_diff': '', 'amount_ratio': '', 'converted': False,
        'combo_len': 1, 'label': 0
    })

# --- Save ---
output_path.parent.mkdir(parents=True, exist_ok=True)
pd.DataFrame(matches).to_csv(output_path, index=False)
print(f"\u2705 Saved {len(matches)} rows (matched + unmatched) to {output_path}")