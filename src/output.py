import sys
from pathlib import Path
import shutil
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))
from src.config import OUTPUT_DIR, LISTS_DIR
from src.shifts_handler import main as handle_shifts, get_cutoff_time
from collections import OrderedDict
import ast
from datetime import datetime
import numpy as np
import re
import pandas as pd

# Determine BASE_DIR for dev vs frozen (EXE) mode
if getattr(sys, 'frozen', False):
    # In PyInstaller bundle (onefile mode), use the EXE's dir as base (though not strictly needed for config, ensures sys.path if relative)
    BASE_DIR = Path(sys.executable).parent
else:
    # Dev mode: standard script path
    BASE_DIR = Path(__file__).resolve().parent.parent

# Append only if not already in path (avoids dupes in imported mode)
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))


def save_excel(df, path, text_columns=None):
    if text_columns is None:
        text_columns = []
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
        worksheet = writer.sheets['Sheet1']
        # Set text format for specified columns
        for col in text_columns:
            if col in df.columns:
                col_idx = df.columns.get_loc(col) + 1
                for row_idx in range(2, len(df) + 2):
                    cell = worksheet.cell(row=row_idx, column=col_idx)
                    cell.number_format = '@'
        # Auto-adjust column widths
        for column in worksheet.columns:
            max_length = 0
            column_letter = column[0].column_letter
            for cell in column:
                if cell.value is not None:
                    max_length = max(max_length, len(str(cell.value)))
            adjusted_width = max_length + 2
            worksheet.column_dimensions[column_letter].width = adjusted_width

def pad_last4(df, last4_col):
    """Pad last4 column to 4 digits with leading zeros, ensuring string type and stripping .0"""
    if last4_col in df.columns:
        df[last4_col] = df[last4_col].apply(clean_value)
        df[last4_col] = df[last4_col].astype(str)
        df[last4_col] = df[last4_col].replace('nan', '')
        mask = df[last4_col] != ''
        df.loc[mask, last4_col] = df.loc[mask, last4_col].str.rstrip('.0').str.zfill(4)

def generate_unmatched_crm_deposits(date_str):
    deposits_matching_path = LISTS_DIR / date_str / "deposits_matching.xlsx"
    if not deposits_matching_path.exists():
        print(f"Deposits matching file not found: {deposits_matching_path}")
        return
    df = pd.read_excel(deposits_matching_path, dtype={'crm_last4': str, 'proc_last4': str, 'crm_transaction_id': str, 'proc_transaction_id': str})
    df['crm_amount'] = df['crm_amount'].apply(clean_value)
    df['crm_amount'] = pd.to_numeric(df['crm_amount'], errors='coerce')
    # Filter unmatched CRM deposits: match_status == 0 and proc_date is NaN (indicating CRM unmatched)
    unmatched_crm = df[(df['match_status'] == 0) & (df['proc_date'].isna())]
    unmatched_crm = unmatched_crm.copy() # Fix SettingWithCopyWarning
    if unmatched_crm.empty:
        print(f"No unmatched CRM deposits found for {date_str}, skipping file creation.")
        return
    # Pad last4
    pad_last4(unmatched_crm, 'crm_last4')
    # Convert crm_date to datetime for filtering and sorting
    unmatched_crm['crm_date'] = pd.to_datetime(unmatched_crm['crm_date'], errors='coerce')
    # Get cutoff time for the date
    cutoff = get_cutoff_time(date_str)
    # Remove rows after the cutoff
    unmatched_crm = unmatched_crm[unmatched_crm['crm_date'] <= cutoff]
    if unmatched_crm.empty:
        print(f"No unmatched CRM deposits after cutoff filter for {date_str}, skipping file creation.")
        return
    # Sort by crm_date from newest to oldest
    unmatched_crm = unmatched_crm.sort_values(by='crm_date', ascending=False)
    # Select specified columns
    columns = [
        'crm_type', 'crm_date', 'crm_firstname', 'crm_lastname', 'crm_email', 'crm_amount', 'crm_currency',
        'crm_approved', 'crm_tp', 'payment_method', 'regulation', 'crm_processor_name', 'crm_last4','crm_transaction_id'
    ]
    unmatched_crm = unmatched_crm[columns]
    # Rename columns: strip crm_ prefix, capitalize first letter, and apply specific overrides
    rename_dict = {
        'crm_type': 'Type',
        'crm_date': 'Date',
        'crm_firstname': 'First Name',
        'crm_lastname': 'Last Name',
        'crm_email': 'Email',
        'crm_amount': 'Amount',
        'crm_currency': 'Currency',
        'crm_approved': 'Approved',
        'crm_tp': 'TP',
        'payment_method': 'Payment Method',
        'regulation': 'Regulation',
        'crm_processor_name': 'Processor Name',
        'crm_last4': 'Last 4 Digits',
        'crm_transaction_id': 'Transaction ID'
    }
    unmatched_crm.rename(columns=rename_dict, inplace=True)
    # Save to output/dated/unmatched_crm_deposits.xlsx
    output_dir = OUTPUT_DIR / date_str
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "Unmatched CRM Deposits.xlsx"
    save_excel(unmatched_crm, output_path, text_columns=['Last 4 Digits', 'Transaction ID'])
    print(f"Unmatched CRM deposits saved to {output_path}")

def generate_unapproved_crm_deposits(date_str):
    deposits_matching_path = LISTS_DIR / date_str / "deposits_matching.xlsx"
    if not deposits_matching_path.exists():
        print(f"Deposits matching file not found: {deposits_matching_path}")
        return
    df = pd.read_excel(deposits_matching_path, dtype={'crm_last4': str, 'proc_last4': str, 'crm_transaction_id': str, 'proc_transaction_id': str})
    df['crm_amount'] = df['crm_amount'].apply(clean_value)
    df['crm_amount'] = pd.to_numeric(df['crm_amount'], errors='coerce')
    # Filter unapproved CRM deposits: match_status == 1 and crm_approved == 'No'
    unapproved_crm = df[(df['match_status'] == 1) & (df['crm_approved'] == 'No')]
    unapproved_crm = unapproved_crm.copy() # Fix SettingWithCopyWarning
    if unapproved_crm.empty:
        print(f"No unapproved CRM deposits found for {date_str}, skipping file creation.")
        return
    # Pad last4
    pad_last4(unapproved_crm, 'crm_last4')
    # Convert crm_date to datetime for filtering and sorting
    unapproved_crm['crm_date'] = pd.to_datetime(unapproved_crm['crm_date'], errors='coerce')
    # Get cutoff time for the date
    cutoff = get_cutoff_time(date_str)
    # Remove rows after the cutoff
    unapproved_crm = unapproved_crm[unapproved_crm['crm_date'] <= cutoff]
    if unapproved_crm.empty:
        print(f"No unapproved CRM deposits after cutoff filter for {date_str}, skipping file creation.")
        return
    # Sort by crm_date from newest to oldest
    unapproved_crm = unapproved_crm.sort_values(by='crm_date', ascending=False)
    # Select specified columns
    columns = [
        'crm_type', 'crm_date', 'crm_firstname', 'crm_lastname', 'crm_email', 'crm_amount', 'crm_currency',
        'crm_approved', 'crm_tp', 'payment_method', 'regulation', 'crm_processor_name', 'crm_last4','crm_transaction_id'
    ]
    unapproved_crm = unapproved_crm[columns]
    # Rename columns: strip crm_ prefix, capitalize first letter, and apply specific overrides
    rename_dict = {
        'crm_type': 'Type',
        'crm_date': 'Date',
        'crm_firstname': 'First Name',
        'crm_lastname': 'Last Name',
        'crm_email': 'Email',
        'crm_amount': 'Amount',
        'crm_currency': 'Currency',
        'crm_approved': 'Approved',
        'crm_tp': 'TP',
        'payment_method': 'Payment Method',
        'regulation': 'Regulation',
        'crm_processor_name': 'Processor Name',
        'crm_last4': 'Last 4 Digits',
        'crm_transaction_id': 'Transaction ID'
    }
    unapproved_crm.rename(columns=rename_dict, inplace=True)
    # Save to output/dated/unapproved_crm_deposits.xlsx
    output_dir = OUTPUT_DIR / date_str
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "Unapproved Deposits.xlsx"
    save_excel(unapproved_crm, output_path, text_columns=['Last 4 Digits', 'Transaction ID'])
    print(f"Unapproved CRM deposits saved to {output_path}")

def generate_unmatched_proc_deposits(date_str):
    deposits_matching_path = LISTS_DIR / date_str / "deposits_matching.xlsx"
    if not deposits_matching_path.exists():
        print(f"Deposits matching file not found: {deposits_matching_path}")
        return
    df = pd.read_excel(deposits_matching_path, dtype={'proc_transaction_id': str, 'proc_last4': str, 'crm_last4': str})
    df['proc_amount'] = df['proc_amount'].apply(clean_value)
    df['proc_amount'] = pd.to_numeric(df['proc_amount'], errors='coerce')
    # Filter unmatched processor deposits: match_status == 0 and crm_date is NaN (indicating processor unmatched)
    unmatched_proc = df[(df['match_status'] == 0) & (df['crm_date'].isna())]
    unmatched_proc = unmatched_proc.copy() # Fix SettingWithCopyWarning
    if unmatched_proc.empty:
        print(f"No unmatched processor deposits found for {date_str}, skipping file creation.")
        return
    # Clean processor columns
    columns_to_clean = [
        'proc_date', 'proc_firstname', 'proc_lastname', 'proc_email', 'proc_tp', 'proc_amount', 'proc_currency',
        'proc_processor_name', 'proc_last4', 'proc_transaction_id'
    ]
    for col in columns_to_clean:
        if col in unmatched_proc.columns:
            unmatched_proc.loc[:, col] = unmatched_proc[col].apply(clean_value)
    # Correct ambiguous date parses for powercash/shift4
    unmatched_proc['proc_date'] = unmatched_proc.apply(
        lambda row: correct_proc_date(row['proc_date'], row['proc_processor_name']), axis=1)

    # Format proc_date
    unmatched_proc.loc[:, 'proc_date'] = unmatched_proc['proc_date'].apply(lambda x: format_date(x, is_proc=True))
    # Ensure proc_transaction_id and proc_last4 are strings and pad
    unmatched_proc['proc_transaction_id'] = unmatched_proc['proc_transaction_id'].astype(str)
    pad_last4(unmatched_proc, 'proc_last4')
    # Manually add crm_type as 'Deposit' since it doesn't exist for processor rows
    unmatched_proc['crm_type'] = 'Deposit'
    # Select specified columns in order
    columns = [
        'crm_type', 'proc_date', 'proc_firstname', 'proc_lastname', 'proc_email', 'proc_amount', 'proc_currency',
        'proc_tp', 'proc_processor_name', 'proc_last4', 'proc_transaction_id'
    ]
    unmatched_proc = unmatched_proc[columns]
    # Rename columns
    rename_dict = {
        'crm_type': 'Type',
        'proc_date': 'Date',
        'proc_firstname': 'First Name',
        'proc_lastname': 'Last Name',
        'proc_email': 'Email',
        'proc_amount': 'Amount',
        'proc_currency': 'Currency',
        'proc_tp': 'TP',
        'proc_processor_name': 'Processor Name',
        'proc_last4': 'Last 4 Digits',
        'proc_transaction_id': 'Transaction ID'
    }
    unmatched_proc.rename(columns=rename_dict, inplace=True)
    # Sort by Date from newest to oldest
    unmatched_proc['Date'] = pd.to_datetime(unmatched_proc['Date'], errors='coerce')
    unmatched_proc = unmatched_proc.sort_values(by='Date', ascending=False)
    # Save to output/dated/unmatched_proc_deposits.xlsx with text format for specific columns
    output_dir = OUTPUT_DIR / date_str
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "Unmatched Processors Deposits.xlsx"
    save_excel(unmatched_proc, output_path, text_columns=['Last 4 Digits', 'Transaction ID'])
    print(f"Unmatched processor deposits saved to {output_path}")

def clean_value(val):
    if isinstance(val, str) and val.strip() == '[nan]':
        return np.nan
    while True:
        if isinstance(val, str):
            try:
                val = ast.literal_eval(val)
            except:
                break
        elif isinstance(val, list):
            if val:
                val = val[0]
            else:
                return np.nan
        else:
            break
    if isinstance(val, float):
        if val.is_integer():
            val = int(val)
    if isinstance(val, str):
        val = val.strip("'\"")
    # Treat 0 or '0' as no value (blank)
    if isinstance(val, (int, float)) and val == 0:
        return np.nan
    if isinstance(val, str) and val.strip() == '0':
        return np.nan
    if pd.isna(val):
        return np.nan
    return val


def format_date(val, is_proc=False):
    if pd.isna(val):
        return val
    if isinstance(val, datetime):
        dt_str = val.strftime('%m/%d/%Y %I:%M:%S %p')
    elif isinstance(val, str):
        val = val.strip()
        try:
            dt = pd.to_datetime(val, dayfirst=is_proc)
            dt_str = dt.strftime('%m/%d/%Y %I:%M:%S %p')
        except:
            return val
    else:
        return str(val) if val is not None else np.nan

    # Strip leading zeros from month and day (e.g., 08/05 → 8/5)
    if ' ' in dt_str:
        date_part, time_part = dt_str.split(' ', 1)
    else:
        date_part = dt_str
        time_part = ''
    month, day, year = date_part.split('/')
    month = month.lstrip('0') or '0'  # Avoid empty if 00 (edge case)
    day = day.lstrip('0') or '0'
    new_date_str = f"{month}/{day}/{year}"
    if time_part:
        new_date_str += f" {time_part}"
    return new_date_str

def correct_proc_date(date_val, processor_name):
    if pd.isna(date_val) or processor_name not in ['powercash', 'shift4']:
        return date_val
    date_str = str(date_val).strip()
    if not re.match(r'\d{4}-\d{2}-\d{2}', date_str.split()[0]):
        return date_str  # Not in expected YYYY-MM-DD format, skip
    try:
        parts = date_str.split()
        date_part = parts[0]
        time_part = ' '.join(parts[1:]) if len(parts) > 1 else ''
        y, m, d = map(int, date_part.split('-'))
        if 1 <= m <= 12 and 1 <= d <= 12 and m > d:
            # Swap for likely misparsed DD/MM as MM/DD where first num > second
            m, d = d, m
        new_date_part = f"{y:04d}-{m:02d}-{d:02d}"
        return f"{new_date_part} {time_part}"
    except:
        return date_str

def process_comment(comment):
    if pd.isna(comment):
        return ''
    parts = [p.strip() for p in str(comment).split(' . ')]
    new_parts = []
    full_emails = OrderedDict()
    masked_emails = OrderedDict()
    last4s = OrderedDict()
    for p in parts:
        if p.startswith('Matched the same last4 :'):
            idx = p.find(' in ')
            if idx != -1:
                temp = p[:idx]
            else:
                temp = p
            last4 = temp[len('Matched the same last4 :'):].strip()
            if last4 not in last4s:
                last4s[last4] = last4
        elif p.startswith('Matched similar email :'):
            idx = p.find(' in ')
            if idx != -1:
                temp = p[:idx]
            else:
                temp = p
            idx_sim = temp.rfind(' (sim ')
            if idx_sim != -1:
                temp = temp[:idx_sim]
            email = temp[len('Matched similar email :'):].strip()
            lower_email = email.lower()
            if '*' in email: # masked
                if lower_email not in masked_emails and lower_email not in full_emails:
                    masked_emails[lower_email] = email
            else: # full
                if lower_email not in full_emails:
                    full_emails[lower_email] = email
        elif p.startswith('Cross-processor fallback match'):
            new_parts.append(p)
        elif p.startswith('Processor names differ'):
            new_parts.append(p)
        # ignore other parts
    # Add similar email
    if full_emails:
        similar_str = "Matched similar email :" + " , ".join(full_emails.values())
        new_parts.append(similar_str)
    elif masked_emails:
        similar_str = "Matched similar email :" + " , ".join(masked_emails.values())
        new_parts.append(similar_str)
    # Add last4
    if last4s:
        last4_str = "Matched the same last4 :" + " , ".join(last4s.values())
        new_parts.append(last4_str)
    return ' . '.join(new_parts)

def process_unmatched_comment(comment):
    if pd.isna(comment):
        return ''
    comment_str = str(comment)
    # First, strip any prefixes or suffixes to get the core comment
    cleaned = comment_str
    if "[unmatched_warning]" in cleaned:
        cleaned = cleaned.replace(" [unmatched_warning]", "")
    if "Unmatched due to warning: " in cleaned:
        cleaned = cleaned.replace("Unmatched due to warning: ", "")
    elif "No matching CRM row found (due to warning: " in cleaned:
        start = cleaned.find("due to warning: ") + len("due to warning: ")
        end = cleaned.rfind(")")
        if end != -1:
            cleaned = cleaned[start:end]
        else:
            cleaned = cleaned[start:]
    elif cleaned.startswith("Unmatched due to warning: "):
        cleaned = cleaned[len("Unmatched due to warning: "):]
    elif cleaned == "No matching CRM row found":
        return ''
    # Transform based on content
    cleaned_lower = cleaned.lower()
    if "matched the same last4" in cleaned_lower or "matched the same last 4 digits" in cleaned_lower:
        return "Matched the same last 4 digits but the user rejected the match"
    elif "matched similar email" in cleaned_lower:
        return "Matched a similar email but the user rejected the match"
    elif "cross-processor fallback match" in cleaned_lower:
        return "row was matched but was executed on different processors so the user rejected the match"
    else:
        return cleaned

def generate_warning_withdrawals(date_str):
    withdrawals_matching_path = LISTS_DIR / date_str / "withdrawals_matching.xlsx"
    if not withdrawals_matching_path.exists():
        print(f"Withdrawals matching file not found: {withdrawals_matching_path}")
        return
    df = pd.read_excel(withdrawals_matching_path, dtype={'crm_last4': str, 'proc_last4': str})
    df['crm_amount'] = df['crm_amount'].apply(clean_value)
    df['proc_amount'] = df['proc_amount'].apply(clean_value)
    df['proc_amount_crm_currency'] = df['proc_amount_crm_currency'].apply(clean_value)
    df['crm_amount'] = pd.to_numeric(df['crm_amount'], errors='coerce')
    df['proc_amount'] = pd.to_numeric(df['proc_amount'], errors='coerce')
    df['proc_amount_crm_currency'] = pd.to_numeric(df['proc_amount_crm_currency'], errors='coerce')
    # Filter rows where warning == True
    warnings_df = df[df['warning'] == True].copy()
    warnings_df['orig_index'] = warnings_df.index
    if warnings_df.empty:
        print(f"No warnings found in withdrawals matching for {date_str}, skipping file creation.")
        return
    # Add orig_index using the original index
    warnings_df['orig_index'] = warnings_df.index
    # Clean processor columns (added crm_last4)
    columns_to_clean = [
        'proc_date', 'proc_email', 'proc_tp', 'proc_firstname', 'proc_lastname',
        'proc_last4', 'proc_currency', 'proc_amount', 'proc_amount_crm_currency', 'crm_last4'
    ]
    for col in columns_to_clean:
        if col in warnings_df.columns:
            warnings_df.loc[:, col] = warnings_df[col].apply(clean_value)
    # Pad last4 columns
    pad_last4(warnings_df, 'crm_last4')
    pad_last4(warnings_df, 'proc_last4')
    # Format proc_date
    warnings_df.loc[:, 'proc_date'] = warnings_df['proc_date'].apply(lambda x: format_date(x, is_proc=True))
    # Make amounts negative
    warnings_df.loc[:, 'crm_amount'] = warnings_df['crm_amount'].apply(lambda x: -abs(x) if pd.notna(x) else x)
    warnings_df.loc[:, 'proc_amount'] = warnings_df['proc_amount'].apply(lambda x: -abs(x) if pd.notna(x) else x)
    # Process the comment column
    warnings_df.loc[:, 'comment'] = warnings_df['comment'].apply(process_comment)
    # Select specified columns
    columns = [
        'orig_index', 'crm_date', 'crm_email', 'crm_firstname', 'crm_lastname', 'crm_tp', 'crm_last4', 'crm_currency',
        'crm_amount',
        'payment_method',
        'crm_processor_name', 'regulation', 'proc_date', 'proc_email', 'proc_tp', 'proc_firstname', 'proc_lastname',
        'proc_last4',
        'proc_currency', 'proc_amount', 'proc_amount_crm_currency', 'proc_processor_name', 'comment'
    ]
    warnings_df = warnings_df[[c for c in columns if c in warnings_df.columns]]
    # Save to output/dated/warnings_withdrawals.xlsx
    output_dir = OUTPUT_DIR / date_str
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "warnings_withdrawals.xlsx"
    save_excel(warnings_df, output_path, text_columns=['crm_last4', 'proc_last4', 'orig_index', 'payment_method'])
    print(f"Warnings withdrawals saved to {output_path}")

def load_matching_df(date_str):
    """Helper to load the most appropriate withdrawals_matching DataFrame, preferring updated version."""
    updated_path = OUTPUT_DIR / date_str / "withdrawals_matching_updated.xlsx"
    original_path = LISTS_DIR / date_str / "withdrawals_matching.xlsx"
    dtype_dict = {'proc_last4': str, 'crm_last4': str}
    if updated_path.exists():
        print(f"Loading updated withdrawals matching from: {updated_path}")
        df = pd.read_excel(updated_path, dtype=dtype_dict)
    elif original_path.exists():
        print(f"Loading original withdrawals matching from: {original_path}")
        df = pd.read_excel(original_path, dtype=dtype_dict)
    else:
        print(f"No withdrawals matching file found for {date_str}")
        return None
    df['crm_amount'] = df['crm_amount'].apply(clean_value)
    df['proc_amount'] = df['proc_amount'].apply(clean_value)
    df['proc_amount_crm_currency'] = df['proc_amount_crm_currency'].apply(clean_value)
    df['crm_amount'] = pd.to_numeric(df['crm_amount'], errors='coerce')
    df['proc_amount'] = pd.to_numeric(df['proc_amount'], errors='coerce')
    df['proc_amount_crm_currency'] = pd.to_numeric(df['proc_amount_crm_currency'], errors='coerce')
    df['comment'] = df['comment'].fillna('').astype(str)
    return df

def generate_unmatched_proc_withdrawals(date_str, matching_df=None):
    if matching_df is None:
        matching_df = load_matching_df(date_str)
        if matching_df is None:
            return
    print(f"Total rows in withdrawals_matching: {len(matching_df)}")
    # Filter rows where warning == False
    df = matching_df[matching_df['warning'] == False]
    print(f"Rows after warning == False: {len(df)}")
    # Filter unmatched processor withdrawals: match_status == 0 and comment contains "No matching CRM row found"
    unmatched_proc = df[(df['match_status'] == 0) & (
            df['comment'].str.contains("No matching CRM row found|Unmatched due to warning|\[unmatched_warning\]", na=False)
    )]
    unmatched_proc = unmatched_proc.copy() # Fix SettingWithCopyWarning if needed in future mods
    print(f"Rows after match_status==0 and comment contains 'No matching CRM row found': {len(unmatched_proc)}")
    print(f"Number of rows with proc_email NaN: {unmatched_proc['proc_email'].isna().sum()}")
    if not unmatched_proc.empty:
        nan_proc_rows = unmatched_proc[unmatched_proc['proc_email'].isna()][['proc_email', 'comment', 'match_status', 'proc_amount']]
        if not nan_proc_rows.empty:
            print("Sample NaN proc_email rows:")
            print(nan_proc_rows.head())
    # Filter out rows with NaN proc_email
    unmatched_proc = unmatched_proc[unmatched_proc['proc_email'].notna()].copy()
    print(f"Rows after filtering NaN proc_email: {len(unmatched_proc)}")
    if unmatched_proc.empty:
        print(f"No unmatched processor withdrawals found for {date_str}, skipping file creation.")
        return
    # Process comments for unmatched (strips prefixes for warnings, sets non-warning to empty)
    unmatched_proc.loc[:, 'comment'] = unmatched_proc['comment'].apply(process_unmatched_comment)
    # Clean processor columns
    columns_to_clean = [
        'proc_date', 'proc_email', 'proc_tp', 'proc_firstname', 'proc_lastname',
        'proc_last4', 'proc_currency', 'proc_amount', 'proc_processor_name'
    ]
    for col in columns_to_clean:
        if col in unmatched_proc.columns:
            unmatched_proc.loc[:, col] = unmatched_proc[col].apply(clean_value)
    unmatched_proc['proc_amount'] = pd.to_numeric(unmatched_proc['proc_amount'], errors='coerce')
    # Correct ambiguous date parses for powercash/shift4
    unmatched_proc['proc_date'] = unmatched_proc.apply(
        lambda row: correct_proc_date(row['proc_date'], row['proc_processor_name']), axis=1)
    # Format proc_date
    unmatched_proc.loc[:, 'proc_date'] = unmatched_proc['proc_date'].apply(lambda x: format_date(x, is_proc=True))
    # Make amounts negative
    unmatched_proc.loc[:, 'proc_amount'] = unmatched_proc['proc_amount'].apply(lambda x: -abs(x) if pd.notna(x) else x)
    # Pad proc_last4
    pad_last4(unmatched_proc, 'proc_last4')
    # Manually add Type as 'Withdrawal'
    unmatched_proc['Type'] = 'Withdrawal'
    # Select specified columns in order (including comment)
    columns = [
        'Type', 'proc_date', 'proc_firstname', 'proc_lastname', 'proc_email',
        'proc_amount', 'proc_currency', 'proc_tp', 'proc_processor_name', 'proc_last4', 'comment'
    ]
    unmatched_proc = unmatched_proc[columns]
    # Rename columns
    rename_dict = {
        'Type': 'Type',
        'proc_date': 'Date',
        'proc_firstname': 'First Name',
        'proc_lastname': 'Last Name',
        'proc_email': 'Email',
        'proc_amount': 'Amount',
        'proc_currency': 'Currency',
        'proc_tp': 'TP',
        'proc_processor_name': 'Processor Name',
        'proc_last4': 'Last 4 Digits',
        'comment': 'Comment'
    }
    unmatched_proc.rename(columns=rename_dict, inplace=True)
    # Sort by Date from newest to oldest
    unmatched_proc['Date'] = pd.to_datetime(unmatched_proc['Date'], errors='coerce')
    unmatched_proc = unmatched_proc.sort_values(by='Date', ascending=False)
    # Save to output/dated/unmatched_proc_withdrawals.xlsx
    output_dir = OUTPUT_DIR / date_str
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "Unmatched Processors Withdrawals.xlsx"
    save_excel(unmatched_proc, output_path, text_columns=['Last 4 Digits'])
    print(f"Unmatched processor withdrawals saved to {output_path}")

def remove_compensated_entries(date_str):
    deposits_path = OUTPUT_DIR / date_str / "Unmatched Processors Deposits.xlsx"
    withdrawals_path = OUTPUT_DIR / date_str / "Unmatched Processors Withdrawals.xlsx"
    if not deposits_path.exists() or not withdrawals_path.exists():
        print(f"Missing files for compensated entries removal in {date_str}, skipping.")
        return None, None
    deposits_df = pd.read_excel(deposits_path, dtype={'Last 4 Digits': str, 'Transaction ID': str})
    deposits_df['Amount'] = deposits_df['Amount'].apply(clean_value)
    deposits_df['Amount'] = pd.to_numeric(deposits_df['Amount'], errors='coerce')
    withdrawals_df = pd.read_excel(withdrawals_path, dtype={'Last 4 Digits': str})
    withdrawals_df['Amount'] = withdrawals_df['Amount'].apply(clean_value)
    withdrawals_df['Amount'] = pd.to_numeric(withdrawals_df['Amount'], errors='coerce')
    # Temporarily rename deposits_df columns to match proc_ prefix for merging
    original_columns_deposits = {
        'Amount': 'proc_amount',
        'Currency': 'proc_currency',
        'Last 4 Digits': 'proc_last4',
        'Processor Name': 'proc_processor_name',
        'Email': 'proc_email'
    }
    deposits_df = deposits_df.rename(columns=original_columns_deposits)
    # Temporarily rename withdrawals_df columns to match proc_ prefix for merging
    original_columns_withdrawals = {
        'Amount': 'proc_amount',
        'Currency': 'proc_currency',
        'Last 4 Digits': 'proc_last4',
        'Processor Name': 'proc_processor_name',
        'Email': 'proc_email'
    }
    withdrawals_df = withdrawals_df.rename(columns=original_columns_withdrawals)
    # Normalize last4 for deposits: pad with leading zeros to 4 digits
    deposits_df['norm_last4'] = deposits_df['proc_last4'].str.zfill(4)
    # For withdrawals, last4 is already 4 digits with leading zeros
    withdrawals_df['norm_last4'] = withdrawals_df['proc_last4']
    # Normalize amounts to absolute values for comparison
    deposits_df['norm_amount'] = deposits_df['proc_amount'].abs().astype(float)
    withdrawals_df['norm_amount'] = withdrawals_df['proc_amount'].abs().astype(float)
    # Ensure other merge columns are strings
    for df in [deposits_df, withdrawals_df]:
        for col in ['proc_currency', 'proc_processor_name', 'proc_email']:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
    # Merge on the matching columns
    merge_columns = ['norm_amount', 'proc_currency', 'norm_last4', 'proc_processor_name', 'proc_email']
    matched = pd.merge(deposits_df.reset_index(), withdrawals_df.reset_index(), on=merge_columns, how='inner', suffixes=('_dep', '_wd'))
    if matched.empty:
        print(f"No compensated entries found for {date_str}.")
        return None, None
    # Get indices to drop
    dep_indices_to_drop = matched['index_dep'].unique()
    wd_indices_to_drop = matched['index_wd'].unique()
    # Capture compensated rows (with proc_ naming)
    compensated_deposits = deposits_df.loc[dep_indices_to_drop].copy()
    compensated_withdrawals = withdrawals_df.loc[wd_indices_to_drop].copy()
    # Drop from deposits
    deposits_df = deposits_df.drop(dep_indices_to_drop).drop(columns=['norm_last4', 'norm_amount'])
    # Rename deposits_df columns back to original
    reverse_columns_deposits = {v: k for k, v in original_columns_deposits.items()}
    deposits_df = deposits_df.rename(columns=reverse_columns_deposits)
    # Drop from withdrawals
    withdrawals_df = withdrawals_df.drop(wd_indices_to_drop).drop(columns=['norm_last4', 'norm_amount'])
    # Rename withdrawals_df columns back to original
    reverse_columns_withdrawals = {v: k for k, v in original_columns_withdrawals.items()}
    withdrawals_df = withdrawals_df.rename(columns=reverse_columns_withdrawals)
    # Save updated files
    save_excel(deposits_df, deposits_path, text_columns=['Last 4 Digits', 'Transaction ID'])
    print(f"Updated Unmatched Processors Deposits.xlsx after removing {len(dep_indices_to_drop)} compensated entries.")
    save_excel(withdrawals_df, withdrawals_path, text_columns=['Last 4 Digits'])
    print(f"Updated Unmatched Processors Withdrawals.xlsx after removing {len(wd_indices_to_drop)} compensated entries.")
    return compensated_deposits, compensated_withdrawals

def generate_unmatched_crm_withdrawals(date_str, matching_df=None):
    if matching_df is None:
        matching_df = load_matching_df(date_str)
        if matching_df is None:
            return
    # Apply warning == False to all groups
    df = matching_df[matching_df['warning'] == False]
    # Group 1: match_status == 0 and payment_status == 0 and comment == "No matching processor row found"
    group1 = df[(df['match_status'] == 0) & (df['payment_status'] == 0) & (df['comment'] == "No matching processor row found")].copy()
    # Group 2: match_status == 1 and payment_status == 0 and (comment contains "Overpaid" or "Underpaid")
    group2 = df[(df['match_status'] == 1) & (df['payment_status'] == 0) & (df['comment'].str.contains("Overpaid|Underpaid", na=False))].copy()
    # Group 3: comment == "Withdrawal cancelled with no matching withdrawal found"
    group3 = df[df['comment'] == "Withdrawal cancelled with no matching withdrawal found"].copy()
    # Group 4: comment contains "Unmatched due to warning" AND crm_email notna (to exclude proc-only splits)
    group4 = df[(df['comment'].str.contains("Unmatched due to warning|\[unmatched_warning\]", na=False)) & (df['crm_email'].notna())].copy()
    # Process Group 1
    if not group1.empty:
        group1['crm_amount'] = group1['crm_amount'].apply(clean_value)
        group1['crm_amount'] = pd.to_numeric(group1['crm_amount'], errors='coerce')
        group1['comment'] = '' # Blank comment
        # Ensure crm_amount is negative
        group1['crm_amount'] = group1['crm_amount'].apply(lambda x: -abs(x) if pd.notna(x) else x)
    # Process Group 3 like Group 1 but crm_amount positive and comment "Withdrawal cancellation"
    if not group3.empty:
        group3['crm_amount'] = group3['crm_amount'].apply(clean_value)
        group3['crm_amount'] = pd.to_numeric(group3['crm_amount'], errors='coerce')
        group3['comment'] = "Withdrawal cancellation"
        # Make crm_amount positive
        group3['crm_amount'] = group3['crm_amount'].apply(lambda x: abs(x) if pd.notna(x) else x)
    # Process Group 2: Parse comment for underpaid/overpaid amount and update crm_amount and comment
    if not group2.empty:
        group2['crm_amount'] = group2['crm_amount'].apply(clean_value)
        group2['proc_amount'] = group2['proc_amount'].apply(clean_value)
        group2['crm_amount'] = pd.to_numeric(group2['crm_amount'], errors='coerce')
        group2['proc_amount'] = pd.to_numeric(group2['proc_amount'], errors='coerce')
        def format_amount(amt):
            if pd.isna(amt):
                return ''
            if float(amt).is_integer():
                return int(amt)
            return amt
        def parse_adjustment(row):
            comment = row['comment']
            if "Underpaid by" in comment:
                # Extract amount after "Underpaid by "
                amount_str = comment.split("Underpaid by ")[1].split(" ")[0]
                amount = float(amount_str)
                sign = -1 # Negative for underpaid
            elif "Overpaid by" in comment:
                # Extract amount after "Overpaid by "
                amount_str = comment.split("Overpaid by ")[1].split(" ")[0]
                amount = float(amount_str)
                sign = 1 # Positive for overpaid
            else:
                return row['crm_amount'], row['comment'] # No change if parse fails
            # Update crm_amount
            new_amount = sign * amount
            # Update comment to "Client requested {original crm_amount} {crm_currency} and received {original proc_amount} {proc_currency}."
            orig_crm_amount = format_amount(row['crm_amount'])
            orig_proc_amount = format_amount(row['proc_amount'])
            crm_curr = row['crm_currency']
            proc_curr = row['proc_currency']
            new_comment = f"Client requested {orig_crm_amount} {crm_curr} and received {orig_proc_amount} {proc_curr}."
            return new_amount, new_comment
        # Apply parsing
        group2[['crm_amount', 'comment']] = group2.apply(parse_adjustment, axis=1, result_type='expand')
    # Process Group 4: Keep as is, crm_amount negative, but process comment to strip prefix
    if not group4.empty:
        group4['crm_amount'] = group4['crm_amount'].apply(clean_value)
        group4['crm_amount'] = pd.to_numeric(group4['crm_amount'], errors='coerce')
        group4['crm_amount'] = group4['crm_amount'].apply(lambda x: -abs(x) if pd.notna(x) else x)
        group4['comment'] = group4['comment'].apply(process_unmatched_comment)
    # Combine all groups
    unmatched_crm = pd.concat([group1, group2, group3, group4], ignore_index=True)
    if unmatched_crm.empty:
        print(f"No unmatched CRM withdrawals found for {date_str}, skipping file creation.")
        return

    # Format crm_date consistently (applies to all groups, including regular unmatched)
    unmatched_crm['crm_date'] = unmatched_crm['crm_date'].apply(lambda x: format_date(x, is_proc=False))
    # Pad last4
    pad_last4(unmatched_crm, 'crm_last4')
    # Select specified columns
    columns = [
        'crm_type', 'crm_date', 'crm_firstname', 'crm_lastname', 'crm_email', 'crm_amount', 'crm_currency',
        'crm_tp','payment_method', 'regulation', 'crm_processor_name', 'crm_last4', 'comment'
    ]
    unmatched_crm = unmatched_crm[columns]
    # Rename columns
    rename_dict = {
        'crm_type': 'Type',
        'crm_date': 'Date',
        'crm_firstname': 'First Name',
        'crm_lastname': 'Last Name',
        'crm_email': 'Email',
        'crm_amount': 'Amount',
        'crm_currency': 'Currency',
        'crm_tp': 'TP',
        'payment_method': 'Payment Method',
        'regulation': 'Regulation',
        'crm_processor_name': 'Processor Name',
        'crm_last4': 'Last 4 Digits',
        'comment': 'Comment'
    }
    unmatched_crm.rename(columns=rename_dict, inplace=True)
    # Sort by Date from newest to oldest
    unmatched_crm['Date'] = pd.to_datetime(unmatched_crm['Date'], errors='coerce')
    unmatched_crm = unmatched_crm.sort_values(by='Date', ascending=False)
    # Save to output/dated/unmatched_crm_withdrawals.xlsx
    output_dir = OUTPUT_DIR / date_str
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "Unmatched CRM Withdrawals.xlsx"
    save_excel(unmatched_crm, output_path, text_columns=['Last 4 Digits'])
    print(f"Unmatched CRM withdrawals saved to {output_path}")

def generate_matched_deposits(date_str, compensated_deps=None):
    deposits_matching_path = LISTS_DIR / date_str / "deposits_matching.xlsx"
    if not deposits_matching_path.exists():
        print(f"Deposits matching file not found: {deposits_matching_path}")
        return
    df = pd.read_excel(deposits_matching_path, dtype={'crm_last4': str, 'proc_last4': str, 'crm_transaction_id': str, 'proc_transaction_id': str})
    df['crm_amount'] = df['crm_amount'].apply(clean_value)
    df['proc_amount'] = df['proc_amount'].apply(clean_value)
    df['crm_amount'] = pd.to_numeric(df['crm_amount'], errors='coerce')
    df['proc_amount'] = pd.to_numeric(df['proc_amount'], errors='coerce')
    matched_df = df[df['match_status'] == 1].copy()
    if matched_df.empty:
        print(f"No matched deposits found for {date_str}.")
    # Select and rename columns as specified
    columns_map = {
        'crm_type': 'Type',
        'crm_date': 'Date',
        'crm_firstname': 'First Name',
        'crm_lastname': 'Last Name',
        'crm_email': 'CRM Email',
        'proc_email': 'PSP Email',
        'crm_amount': 'CRM Amount',
        'crm_currency': 'CRM Currency',
        'proc_amount': 'PSP Amount',
        'proc_currency': 'PSP Currency',
        'crm_approved': 'Approved',
        'crm_tp': 'TP',
        'payment_method': 'Payment Method',
        'regulation': 'Regulation',
        'crm_processor_name': 'CRM Processor Name',
        'proc_processor_name': 'PSP Processor Name',
        'crm_last4': 'CRM Last 4 Digits',
        'proc_last4': 'PSP Last 4 Digits',
        'crm_transaction_id': 'CRM Transaction ID',
        'proc_transaction_id': 'PSP Transaction ID'
    }
    available_cols = [col for col in columns_map if col in matched_df.columns]
    matched_df = matched_df[available_cols]
    matched_df.rename(columns={k: v for k, v in columns_map.items() if k in available_cols}, inplace=True)
    matched_df['Match'] = 'Yes'
    matched_df['Comment'] = np.nan
    # Pad last4 and format date
    pad_last4(matched_df, 'CRM Last 4 Digits')
    pad_last4(matched_df, 'PSP Last 4 Digits')
    matched_df['Date'] = matched_df['Date'].apply(format_date)
    # Sort matched_df by Date descending
    matched_df['Date'] = pd.to_datetime(matched_df['Date'], errors='coerce')
    matched_df = matched_df.sort_values(by='Date', ascending=False)
    # Handle compensated deposits (cancellations) if provided
    compensated_formatted = pd.DataFrame(columns=matched_df.columns)
    if compensated_deps is not None and not compensated_deps.empty:
        compensated_deps['proc_amount'] = compensated_deps['proc_amount'].apply(clean_value)
        compensated_deps['proc_amount'] = pd.to_numeric(compensated_deps['proc_amount'], errors='coerce')
        compensated_formatted['Date'] = compensated_deps['Date']
        compensated_formatted['First Name'] = compensated_deps['First Name']
        compensated_formatted['Last Name'] = compensated_deps['Last Name']
        compensated_formatted['PSP Email'] = compensated_deps['proc_email']
        compensated_formatted['PSP Amount'] = compensated_deps['proc_amount']
        compensated_formatted['PSP Currency'] = compensated_deps['proc_currency']
        compensated_formatted['TP'] = compensated_deps['TP']
        compensated_formatted['PSP Processor Name'] = compensated_deps['proc_processor_name']
        compensated_formatted['PSP Last 4 Digits'] = compensated_deps['proc_last4']
        compensated_formatted['PSP Transaction ID'] = compensated_deps.get('Transaction ID', pd.NA)
        compensated_formatted['Type'] = 'Deposit Cancelled'
        compensated_formatted['Match'] = 'No'
        compensated_formatted['Comment'] = "Deposit cancelled within the same day"
    # Concat and sort: compensated first, then by Date descending
    all_deposits = pd.concat([matched_df, compensated_formatted], ignore_index=True)
    all_deposits['Date'] = pd.to_datetime(all_deposits['Date'], errors='coerce')
    all_deposits['sort_group'] = np.where(all_deposits['Match'] == 'No', 0, 1)
    all_deposits = all_deposits.sort_values(by=['sort_group', 'Date'], ascending=[True, False])
    all_deposits = all_deposits.drop(columns=['sort_group'])
    if all_deposits.empty:
        print(f"No data for matched deposits (including cancellations) for {date_str}, skipping file creation.")
        return
    # Save
    output_dir = OUTPUT_DIR / date_str
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "Matched Deposits.xlsx"
    save_excel(all_deposits, output_path, text_columns=['CRM Last 4 Digits', 'PSP Last 4 Digits', 'CRM Transaction ID', 'PSP Transaction ID'])
    print(f"Matched deposits (including cancellations) saved to {output_path}")

def generate_matched_withdrawals(date_str, compensated_wds=None):
    matching_df = load_matching_df(date_str)
    if matching_df is None:
        return
    matched_df = matching_df[matching_df['match_status'] == 1].copy()
    if matched_df.empty:
        print(f"No matched withdrawals found for {date_str}.")
    # Make amounts negative
    matched_df['crm_amount'] = matched_df['crm_amount'].apply(lambda x: -abs(x) if pd.notna(x) else x)
    matched_df['proc_amount'] = matched_df['proc_amount'].apply(lambda x: -abs(x) if pd.notna(x) else x)
    # Select and rename columns (analogous to deposits, omitting Approved/Transaction IDs, adding Comment)
    columns_map = {
        'crm_type': 'Type',
        'crm_date': 'Date',
        'crm_firstname': 'CRM First Name',
        'crm_lastname': 'CRM Last Name',
        'proc_firstname': 'PSP First Name',
        'proc_lastname': 'PSP Last Name',
        'crm_email': 'CRM Email',
        'proc_email': 'PSP Email',
        'crm_amount': 'CRM Amount',
        'crm_currency': 'CRM Currency',
        'proc_amount': 'PSP Amount',
        'proc_currency': 'PSP Currency',
        'crm_tp': 'TP',
        'payment_method': 'Payment Method',
        'regulation': 'Regulation',
        'crm_processor_name': 'CRM Processor Name',
        'proc_processor_name': 'PSP Processor Name',
        'crm_last4': 'CRM Last 4 Digits',
        'proc_last4': 'PSP Last 4 Digits',
        'comment': 'Comment'
    }
    available_cols = [col for col in columns_map if col in matched_df.columns]
    matched_df = matched_df[available_cols]
    matched_df.rename(columns={k: v for k, v in columns_map.items() if k in available_cols}, inplace=True)
    matched_df['Match'] = 'Yes'
    # Re-order columns to place 'Match' before 'Comment'
    columns = list(matched_df.columns)
    if 'Comment' in columns and 'Match' in columns:
        columns.remove('Match')
        comment_idx = columns.index('Comment')
        columns.insert(comment_idx, 'Match')
        matched_df = matched_df[columns]
    # Pad last4 and format date
    pad_last4(matched_df, 'CRM Last 4 Digits')
    pad_last4(matched_df, 'PSP Last 4 Digits')
    matched_df['Date'] = matched_df['Date'].apply(format_date)
    # Sort matched_df by Date descending
    matched_df['Date'] = pd.to_datetime(matched_df['Date'], errors='coerce')
    matched_df = matched_df.sort_values(by='Date', ascending=False)
    # Handle compensated withdrawals (cancellations) if provided
    compensated_formatted = pd.DataFrame(columns=matched_df.columns)
    if compensated_wds is not None and not compensated_wds.empty:
        compensated_wds['proc_amount'] = compensated_wds['proc_amount'].apply(clean_value)
        compensated_wds['proc_amount'] = pd.to_numeric(compensated_wds['proc_amount'], errors='coerce')
        compensated_formatted['Date'] = compensated_wds['Date']
        compensated_formatted['PSP First Name'] = compensated_wds['First Name']
        compensated_formatted['PSP Last Name'] = compensated_wds['Last Name']
        compensated_formatted['PSP Email'] = compensated_wds['proc_email']
        compensated_formatted['PSP Amount'] = compensated_wds['proc_amount']
        compensated_formatted['PSP Currency'] = compensated_wds['proc_currency']
        compensated_formatted['TP'] = compensated_wds['TP']
        compensated_formatted['Payment Method'] = pd.NA  # No equivalent
        compensated_formatted['Regulation'] = pd.NA  # No equivalent
        compensated_formatted['PSP Processor Name'] = compensated_wds['proc_processor_name']
        compensated_formatted['PSP Last 4 Digits'] = compensated_wds['proc_last4']
        compensated_formatted['Type'] = 'Deposit Cancellation'
        compensated_formatted['Comment'] = "Deposit cancellation within the same day"
        compensated_formatted['Match'] = 'No'
    # Concat and sort: compensated first, then by Date descending
    all_withdrawals = pd.concat([matched_df, compensated_formatted], ignore_index=True)
    all_withdrawals['Date'] = pd.to_datetime(all_withdrawals['Date'], errors='coerce')
    all_withdrawals['sort_group'] = np.where(all_withdrawals['Match'] == 'No', 0, 1)
    all_withdrawals = all_withdrawals.sort_values(by=['sort_group', 'Date'], ascending=[True, False])
    all_withdrawals = all_withdrawals.drop(columns=['sort_group'])
    if all_withdrawals.empty:
        print(f"No data for matched withdrawals (including cancellations) for {date_str}, skipping file creation.")
        return
    # Save
    output_dir = OUTPUT_DIR / date_str
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "Matched Withdrawals.xlsx"
    save_excel(all_withdrawals, output_path, text_columns=['CRM Last 4 Digits', 'PSP Last 4 Digits'])
    print(f"Matched withdrawals (including cancellations) saved to {output_path}")

def main(date_str):
    # Clear OUTPUT_DIR contents fully (rmtree all subdirs/files to prevent any stale remnants)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)  # Ensure output exists
    for item in list(OUTPUT_DIR.iterdir()):
        try:
            if item.is_file():
                item.unlink()
                print(f"Removed file {item} in OUTPUT_DIR")
            else:  # dir
                shutil.rmtree(item)
                print(f"Removed dir {item} in OUTPUT_DIR")
        except Exception as e:
            print(f"Failed to remove {item}: {e}")

    output_dir = OUTPUT_DIR / date_str
    output_dir.mkdir(parents=True, exist_ok=True)

    # For standalone testing; in frontend, phases are called separately
    # Phase 1 (handled in third_window): clear, handle_shifts, warnings (user-edited)
    matched_sums = handle_shifts(date_str)
    if matched_sums:
        output_path = output_dir / "total_shifts_by_currency.csv"
        df = pd.DataFrame([matched_sums])
        if df.empty:
            print(f"No shifts data for {date_str}, skipping file creation.")
        else:
            df.to_csv(output_path, index=False)
            print(f"Total shifts by currency saved to {output_path}")
    generate_warning_withdrawals(date_str)  # Standalone; overridden in frontend
    # Phase 2
    generate_unmatched_crm_deposits(date_str)
    generate_unapproved_crm_deposits(date_str)
    generate_unmatched_proc_deposits(date_str)
    generate_unmatched_proc_withdrawals(date_str)
    compensated_deps, compensated_wds = remove_compensated_entries(date_str)
    generate_unmatched_crm_withdrawals(date_str)
    generate_matched_deposits(date_str, compensated_deps)
    generate_matched_withdrawals(date_str, compensated_wds)

if __name__ == "__main__":
    DATE = sys.argv[1] if len(sys.argv) > 1 else "2025-09-02" # Default date for testing; use command-line arg in production
    main(DATE)