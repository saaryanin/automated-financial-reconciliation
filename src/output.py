import sys
from pathlib import Path
import shutil
import warnings
import re
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))
from src.shifts_handler import main as handle_shifts, get_cutoff_time
from collections import OrderedDict
import ast
from datetime import datetime
import numpy as np
import pandas as pd
import src.config as config
# Ignore known non-critical pd.to_datetime warnings (ambiguous formats are handled via errors='coerce' elsewhere)
warnings.filterwarnings("ignore", category=UserWarning, message=".*Parsing dates.*")
warnings.filterwarnings("ignore", category=UserWarning, message=".*Could not infer format.*")
# Determine BASE_DIR for dev vs frozen (EXE) mode
if getattr(sys, 'frozen', False):
    BASE_DIR = Path(sys.executable).parent
else:
    BASE_DIR = Path(__file__).resolve().parent.parent
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
def generate_unmatched_crm_deposits(date_str, lists_dir, regulation):
    deposits_matching_path = lists_dir / date_str / f"{regulation}_deposits_matching.xlsx"
    if not deposits_matching_path.exists():
        print(f"Deposits matching file not found: {deposits_matching_path}")
        return None
    df = pd.read_excel(deposits_matching_path, dtype={'crm_last4': str, 'proc_last4': str, 'crm_transaction_id': str, 'proc_transaction_id': str})
    df['crm_amount'] = df['crm_amount'].apply(clean_value)
    df['crm_amount'] = pd.to_numeric(df['crm_amount'], errors='coerce')
    # Filter unmatched CRM deposits: match_status == 0 and proc_date is NaN (indicating CRM unmatched)
    unmatched_crm = df[(df['match_status'] == 0) & (df['proc_date'].isna())]
    unmatched_crm = unmatched_crm.copy() # Fix SettingWithCopyWarning
    if unmatched_crm.empty:
        print(f"No unmatched CRM deposits found for {date_str}, skipping file creation.")
        return None
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
        return None
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
    unmatched_crm['Currency'] = unmatched_crm['Currency'].replace({'USD': 'US Dollar', 'EUR': 'Euro'})
    if 'Regulation' in df.columns: # Replace 'df' with the actual DF name, e.g., matched_df, all_withdrawals, unmatched_crm, etc.
        df['Regulation'] = df['Regulation'].apply(lambda x: 'UK' if str(x).lower() == 'uk' else x)
    print(f"Unmatched CRM deposits DataFrame prepared for {date_str}")
    return unmatched_crm
def generate_unapproved_crm_deposits(date_str, lists_dir, output_dir, regulation):
    deposits_matching_path = lists_dir / date_str / f"{regulation}_deposits_matching.xlsx"
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
    unapproved_crm = unapproved_crm.sort_values(by='crm_date', ascending=True)
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
    unapproved_crm['Currency'] = unapproved_crm['Currency'].replace({'USD': 'US Dollar', 'EUR': 'Euro'})
    if 'Regulation' in unapproved_crm.columns:
        unapproved_crm['Regulation'] = unapproved_crm['Regulation'].apply(
            lambda x: 'UK' if str(x).lower() == 'uk' else x)
    # Save to output/dated/unapproved_crm_deposits.xlsx
    output_path = output_dir / f"{regulation.upper()} Unapproved Deposits.xlsx"
    save_excel(unapproved_crm, output_path, text_columns=['Last 4 Digits', 'Transaction ID'])
    print(f"Unapproved CRM deposits saved to {output_path}")
def generate_unmatched_proc_deposits(date_str, lists_dir, regulation):
    deposits_matching_path = lists_dir / date_str / f"{regulation}_deposits_matching.xlsx"
    if not deposits_matching_path.exists():
        print(f"Deposits matching file not found: {deposits_matching_path}")
        return None
    df = pd.read_excel(deposits_matching_path, dtype={'proc_transaction_id': str, 'proc_last4': str, 'crm_last4': str})
    df['proc_amount'] = df['proc_amount'].apply(clean_value)
    df['proc_amount'] = pd.to_numeric(df['proc_amount'], errors='coerce')
    # Filter unmatched processor deposits: match_status == 0 and crm_date is NaN (indicating processor unmatched)
    unmatched_proc = df[(df['match_status'] == 0) & (df['crm_date'].isna())]
    unmatched_proc = unmatched_proc.copy() # Fix SettingWithCopyWarning
    if unmatched_proc.empty:
        print(f"No unmatched processor deposits found for {date_str}, skipping file creation.")
        return None
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
    print(f"Unmatched processor deposits DataFrame prepared for {date_str}")
    return unmatched_proc
def clean_value(val, join_list=False, is_email=False):
    if isinstance(val, str) and val.strip() == '[nan]':
        result = "" if is_email else np.nan
        return result
    if isinstance(val, str) and val.strip() == "['nan']": # New check for string representation
        result = "" if is_email else np.nan
        return result
    if isinstance(val, str):
        try:
            val = ast.literal_eval(val)
        except:
            pass
    if isinstance(val, list):
        cleaned_list = [clean_value(v, join_list=join_list, is_email=is_email) for v in val]
        if join_list:
            result = ','.join(str(v) for v in cleaned_list if not pd.isna(v))
        else:
            if cleaned_list == ['nan']: # Handle actual list ['nan']
                result = "" if is_email else np.nan
            elif cleaned_list:
                result = cleaned_list[0]
            else:
                result = "" if is_email else np.nan
        return result
    if pd.isna(val):
        result = "" if is_email else np.nan
        return result
    if isinstance(val, float):
        if val.is_integer():
            val = int(val)
    if isinstance(val, str):
        val = val.strip("'\"")
    if isinstance(val, (int, float)) and val == 0:
        result = "" if is_email else np.nan
        return result
    if isinstance(val, str) and val.strip() == '0':
        result = "" if is_email else np.nan
        return result
    result = val
    return result
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
    month = month.lstrip('0') or '0' # Avoid empty if 00 (edge case)
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
        return date_str # Not in expected YYYY-MM-DD format, skip
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
        else:
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
def generate_warning_withdrawals(date_str, lists_dir, output_dir, regulation):
    withdrawals_matching_path = lists_dir / date_str / f"{regulation}_withdrawals_matching.xlsx"
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
    if 'regulation' in warnings_df.columns:
        warnings_df['regulation'] = warnings_df['regulation'].apply(lambda x: 'UK' if str(x).lower() == 'uk' else x)
    # Save to output/dated/warnings_withdrawals.xlsx
    output_path = output_dir / f"{regulation.upper()} warnings_withdrawals.xlsx"
    save_excel(warnings_df, output_path, text_columns=['crm_last4', 'proc_last4', 'orig_index', 'payment_method'])
    print(f"Warnings withdrawals saved to {output_path}")
def load_matching_df(date_str, lists_dir, output_dir, regulation):
    """Helper to load the most appropriate withdrawals_matching DataFrame, preferring updated version."""
    updated_path = output_dir / "withdrawals_matching_updated.xlsx"
    original_path = lists_dir / date_str / f"{regulation}_withdrawals_matching.xlsx"
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
    str_columns = ['crm_firstname', 'crm_lastname', 'proc_firstname', 'proc_lastname',
                   'crm_email', 'proc_email',
                   'crm_currency', 'proc_currency',
                   'crm_processor_name', 'proc_processor_name',
                   'payment_method', 'regulation',
                   'crm_last4', 'proc_last4',
                   'crm_tp', 'proc_tp',
                   'crm_type']
    for col in str_columns:
        if col in df.columns:
            join = 'email' in col
            df[col] = df[col].apply(lambda x: clean_value(x, join_list=join))
    return df
def format_amount(amt):
    if pd.isna(amt):
        return ''
    if float(amt).is_integer():
        return int(amt)
    return amt


def parse_adjustment(row):
    def format_num(num):
        if pd.isna(num):
            return '0'
        rounded = round(abs(num), 2)
        formatted = f"{rounded:.2f}"
        return formatted.rstrip('0').rstrip('.') if formatted.endswith('00') else formatted

    comment = str(row['comment'])

    # Check if this is an accepted warning row with underpaid/overpaid
    if "Warning accepted and was considered a match after review" in comment:
        # Extract the underpaid/overpaid information
        match = re.search(r'(Underpaid|Overpaid) by ([\d.]+) (\w+)', comment)
        if match:
            paid_type = match.group(1)
            amt_str = match.group(2)
            curr = match.group(3)
            amt = float(amt_str)

            requested_amount = round(abs(row['crm_amount']), 2)
            received_amount = round(abs(row['proc_amount_crm_currency']), 2)

            # Recalculate type based on amounts for consistency
            type_ = 'Underpaid' if received_amount < requested_amount else 'Overpaid'
            requested_str = format_num(requested_amount)
            received_str = format_num(received_amount)
            diff_str = format_num(amt)

            new_comment = f"Client Requested {requested_str} {curr} and received {received_str} {curr}, {type_} by {diff_str} {curr}."
            # FIX: For underpaid, amount should be negative; for overpaid, positive
            new_amount = -amt if type_ == 'Underpaid' else amt
            return new_amount, new_comment

    # Original logic for regular underpaid/overpaid
    match = re.search(r'(Underpaid|Overpaid) by ([\d.]+) (\w+)', comment)
    if not match:
        return row['crm_amount'], comment

    paid_type = match.group(1)
    amt_str = match.group(2)
    curr = match.group(3)
    amt = float(amt_str)

    requested_amount = round(abs(row['crm_amount']), 2)
    received_amount = round(abs(row['proc_amount_crm_currency']), 2)

    # Recalculate type based on amounts for consistency
    type_ = 'Underpaid' if received_amount < requested_amount else 'Overpaid'
    requested_str = format_num(requested_amount)
    received_str = format_num(received_amount)
    diff_str = format_num(amt)

    new_comment = f"Client Requested {requested_str} {curr} and received {received_str} {curr}, {type_} by {diff_str} {curr}."
    # FIX: For underpaid, amount should be negative; for overpaid, positive
    new_amount = -amt if type_ == 'Underpaid' else amt
    return new_amount, new_comment

def generate_unmatched_proc_withdrawals(date_str, lists_dir, output_dir, regulation, matching_df=None):
    if matching_df is None:
        matching_df = load_matching_df(date_str, lists_dir, output_dir, regulation)
        if matching_df is None:
            return None

    print(f"DEBUG OUTPUT: Total rows in withdrawals_matching: {len(matching_df)}")
    print(f"DEBUG OUTPUT: Columns in matching_df: {matching_df.columns.tolist()}")

    # Filter rows where warning == False
    df = matching_df[matching_df['warning'] == False]
    print(f"DEBUG OUTPUT: Rows after warning == False: {len(df)}")

    # Filter unmatched processor withdrawals
    unmatched_proc = df[(df['match_status'] == 0) & (
        df['comment'].str.contains("No matching CRM row found|Unmatched due to warning|\\[unmatched_warning\\]",
                                   na=False)
    )]

    print(f"DEBUG OUTPUT: Rows after match_status==0 and comment filter: {len(unmatched_proc)}")
    print(f"DEBUG OUTPUT: Number of rows with proc_email not NaN: {unmatched_proc['proc_email'].notna().sum()}")

    if not unmatched_proc.empty:
        sample_proc_rows = unmatched_proc[unmatched_proc['proc_email'].notna()][
            ['proc_email', 'proc_amount', 'proc_currency', 'proc_processor_name', 'comment']].head()
        print("DEBUG OUTPUT: Sample PSP rows:")
        print(sample_proc_rows)

        # Check for rows with Unmatched due to warning
        warning_rows = unmatched_proc[unmatched_proc['comment'].str.contains("Unmatched due to warning", na=False)]
        print(f"DEBUG OUTPUT: Rows with 'Unmatched due to warning': {len(warning_rows)}")
        if not warning_rows.empty:
            print("DEBUG OUTPUT: Sample warning rows:")
            print(warning_rows[['proc_email', 'proc_amount', 'proc_currency', 'comment']].head())
    print(f"Rows after handling NaN proc_email (no filter): {len(unmatched_proc)}")
    if unmatched_proc.empty:
        print(f"No unmatched processor withdrawals found for {date_str}, skipping file creation.")
        return None
    # Process comments for unmatched (strips prefixes for warnings, sets non-warning to empty)
    unmatched_proc.loc[:, 'comment'] = unmatched_proc['comment'].apply(process_unmatched_comment)
    # Clean processor columns
    columns_to_clean = [
        'proc_date', 'proc_email', 'proc_tp', 'proc_firstname', 'proc_lastname',
        'proc_last4', 'proc_currency', 'proc_amount', 'proc_processor_name'
    ]
    for col in columns_to_clean:
        if col in unmatched_proc.columns:
            is_email = 'email' in col.lower()  # Set is_email=True for email columns to convert NaN to ''
            unmatched_proc.loc[:, col] = unmatched_proc[col].apply(lambda x: clean_value(x, is_email=is_email))
    unmatched_proc['proc_amount'] = pd.to_numeric(unmatched_proc['proc_amount'], errors='coerce')
    # Correct ambiguous date parses for powercash/shift4
    unmatched_proc['proc_date'] = unmatched_proc.apply(
        lambda row: correct_proc_date(row['proc_date'], row['proc_processor_name']), axis=1)
    # Format proc_date
    unmatched_proc.loc[:, 'proc_date'] = unmatched_proc['proc_date'].apply(lambda x: format_date(x, is_proc=True))
    # Make amounts negative
    unmatched_proc.loc[:, 'proc_amount'] = unmatched_proc['proc_amount'].apply(lambda x: -abs(x) if pd.notna(x) else x)
    # Remove rows where proc_amount is NaN (excludes invalid/blank rows like CRM splits)
    unmatched_proc = unmatched_proc[unmatched_proc['proc_amount'].notna()]
    if unmatched_proc.empty:
        print(f"No valid unmatched processor withdrawals (all had NaN amounts) for {date_str}, skipping file creation.")
        return None
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
    print(f"Unmatched processor withdrawals DataFrame prepared for {date_str}")
    return unmatched_proc
def remove_compensated_entries(proc_deps_df, proc_wds_df):
    if proc_deps_df is None or proc_wds_df is None:
        print("Missing processor DFs for compensated entries removal, skipping.")
        return proc_deps_df, proc_wds_df, None, None
    # Use the DFs directly (no file loading)
    deposits_df = proc_deps_df.copy()
    withdrawals_df = proc_wds_df.copy()
    deposits_df['Amount'] = deposits_df['Amount'].apply(clean_value)
    deposits_df['Amount'] = pd.to_numeric(deposits_df['Amount'], errors='coerce')
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
        print("No compensated entries found.")
        return deposits_df.drop(columns=['norm_last4', 'norm_amount']).rename(columns={v: k for k, v in original_columns_deposits.items()}), \
               withdrawals_df.drop(columns=['norm_last4', 'norm_amount']).rename(columns={v: k for k, v in original_columns_withdrawals.items()}), \
               None, None
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
    print(f"Removed {len(dep_indices_to_drop)} compensated entries from processor deposits DF.")
    print(f"Removed {len(wd_indices_to_drop)} compensated entries from processor withdrawals DF.")
    return deposits_df, withdrawals_df, compensated_deposits, compensated_withdrawals


def generate_unmatched_crm_withdrawals(date_str, lists_dir, output_dir, regulation, matching_df=None):
    if matching_df is None:
        matching_df = load_matching_df(date_str, lists_dir, output_dir, regulation)
        if matching_df is None:
            return None

    # Include both warning and non-warning rows for proper grouping
    df = matching_df.copy()

    # Group 1: match_status == 0 and payment_status == 0 and comment == "No matching processor row found"
    group1 = df[(df['match_status'] == 0) & (df['payment_status'] == 0) & (
                df['comment'] == "No matching processor row found")].copy()

    # Group 2: match_status == 1 and payment_status == 0 and (comment contains "Overpaid" or "Underpaid")
    # This includes both regular underpaid/overpaid AND accepted warning rows with amount differences
    group2_condition = (
            (df['match_status'] == 1) &
            (df['payment_status'] == 0) &
            (
                    df['comment'].str.contains("Overpaid|Underpaid", na=False) |
                    df['comment'].str.contains("Warning accepted and was considered a match after review", na=False)
            )
    )
    group2 = df[group2_condition].copy()

    # Group 3: comment == "Withdrawal cancelled with no matching withdrawal found"
    group3 = df[df['comment'] == "Withdrawal cancelled with no matching withdrawal found"].copy()

    # Group 4: comment contains "Unmatched due to warning" AND crm_email notna (to exclude proc-only splits)
    group4 = df[(df['comment'].str.contains("Unmatched due to warning|\\[unmatched_warning\\]", na=False)) & (
        df['crm_email'].notna())].copy()

    # Process Group 1
    if not group1.empty:
        group1['crm_amount'] = group1['crm_amount'].apply(clean_value)
        group1['crm_amount'] = pd.to_numeric(group1['crm_amount'], errors='coerce')
        group1['comment'] = ''  # Blank comment
        # Ensure crm_amount is negative
        group1['crm_amount'] = group1['crm_amount'].apply(lambda x: -abs(x) if pd.notna(x) else x)

    # Process Group 3 like Group 1 but crm_amount positive and comment "Withdrawal cancellation"
    if not group3.empty:
        group3['crm_amount'] = group3['crm_amount'].apply(clean_value)
        group3['crm_amount'] = pd.to_numeric(group3['crm_amount'], errors='coerce')
        group3['comment'] = "Withdrawal cancellation"
        # Make crm_amount positive
        group3['crm_amount'] = group3['crm_amount'].apply(lambda x: abs(x) if pd.notna(x) else x)

    # Process Group 2 - this now includes accepted warning rows with underpaid/overpaid
    if not group2.empty:
        group2['crm_amount'] = group2['crm_amount'].apply(clean_value)
        group2['proc_amount'] = group2['proc_amount'].apply(clean_value)
        group2['proc_amount_crm_currency'] = group2['proc_amount_crm_currency'].apply(clean_value)
        group2['crm_amount'] = pd.to_numeric(group2['crm_amount'], errors='coerce')
        group2['proc_amount'] = pd.to_numeric(group2['proc_amount'], errors='coerce')
        group2['proc_amount_crm_currency'] = pd.to_numeric(group2['proc_amount_crm_currency'], errors='coerce')
        group2[['crm_amount', 'comment']] = group2.apply(parse_adjustment, axis=1, result_type='expand')

    # Process Group 4: Keep as is, crm_amount negative, but process comment to strip prefix
    if not group4.empty:
        group4['crm_amount'] = group4['crm_amount'].apply(clean_value)
        group4['crm_amount'] = pd.to_numeric(group4['crm_amount'], errors='coerce')
        group4['crm_amount'] = group4['crm_amount'].apply(lambda x: -abs(x) if pd.notna(x) else x)
        group4['comment'] = group4['comment'].apply(process_unmatched_comment)
    # Load cross-regulation for crm side (where CRM is current reg, PROC is opposite)
    cross_path = lists_dir / date_str / f"{regulation}_cross_regulation.xlsx"
    group_cross_over_under = pd.DataFrame()
    if cross_path.exists():
        cross_df = pd.read_excel(cross_path, dtype={'crm_last4': str, 'proc_last4': str})
        cross_df['crm_amount'] = cross_df['crm_amount'].apply(clean_value)
        cross_df['proc_amount'] = cross_df['proc_amount'].apply(clean_value)
        cross_df['crm_amount'] = pd.to_numeric(cross_df['crm_amount'], errors='coerce')
        cross_df['proc_amount'] = pd.to_numeric(cross_df['proc_amount'], errors='coerce')
        # Removed warning==False filter to include warning==True rows (e.g., for underpaid cross-processor fallbacks)
        # cross_df = cross_df[cross_df['warning'] == False] # Commented out
        group_cross_over_under = cross_df[(cross_df['match_status'] == 1) & (cross_df['payment_status'] == 0) & (cross_df['comment'].str.contains("Overpaid|Underpaid", na=False))].copy()
        if not group_cross_over_under.empty:
            group_cross_over_under['comment'] = group_cross_over_under['comment'].apply(process_comment)
            group_cross_over_under[['crm_amount', 'comment']] = group_cross_over_under.apply(parse_adjustment, axis=1, result_type='expand')
            print(f"Added {len(group_cross_over_under)} cross-regulation over/underpaid rows to unmatched crm for {regulation}")
    # Load cross-processor for crm side
    cross_processor_path = lists_dir / date_str / f"{regulation}_cross_processor.xlsx"
    group_cross_processor_over_under = pd.DataFrame()
    if cross_processor_path.exists():
        cross_proc_df = pd.read_excel(cross_processor_path, dtype={'crm_last4': str, 'proc_last4': str})
        cross_proc_df['crm_amount'] = cross_proc_df['crm_amount'].apply(clean_value)
        cross_proc_df['proc_amount'] = cross_proc_df['proc_amount'].apply(clean_value)
        cross_proc_df['crm_amount'] = pd.to_numeric(cross_proc_df['crm_amount'], errors='coerce')
        cross_proc_df['proc_amount'] = pd.to_numeric(cross_proc_df['proc_amount'], errors='coerce')
        # Removed warning==False filter to include warning==True rows (e.g., for underpaid cross-processor fallbacks)
        # cross_proc_df = cross_proc_df[cross_proc_df['warning'] == False] # Commented out
        group_cross_processor_over_under = cross_proc_df[(cross_proc_df['match_status'] == 1) & (cross_proc_df['payment_status'] == 0) & (cross_proc_df['comment'].str.contains("Overpaid|Underpaid", na=False))].copy()
        if not group_cross_processor_over_under.empty:
            group_cross_processor_over_under['comment'] = group_cross_processor_over_under['comment'].apply(process_comment)
            group_cross_processor_over_under[['crm_amount', 'comment']] = group_cross_processor_over_under.apply(parse_adjustment, axis=1, result_type='expand')
            print(f"Added {len(group_cross_processor_over_under)} cross-processor over/underpaid rows to unmatched crm for {regulation}")
    # Combine all groups
    groups = [group1, group2, group3, group4, group_cross_over_under, group_cross_processor_over_under]
    filtered_groups = [g for g in groups if not g.empty]
    if filtered_groups:
        unmatched_crm = pd.concat(filtered_groups, ignore_index=True)
    else:
        unmatched_crm = pd.DataFrame()
    if unmatched_crm.empty:
        print(f"No unmatched CRM withdrawals found for {date_str}, skipping file creation.")
        return None
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
    unmatched_crm['Currency'] = unmatched_crm['Currency'].replace({'USD': 'US Dollar', 'EUR': 'Euro'})
    if 'Regulation' in df.columns: # Replace 'df' with the actual DF name, e.g., matched_df, all_withdrawals, unmatched_crm, etc.
        df['Regulation'] = df['Regulation'].apply(lambda x: 'UK' if str(x).lower() == 'uk' else x)
    # Sort by Date from newest to oldest
    unmatched_crm['Date'] = pd.to_datetime(unmatched_crm['Date'], errors='coerce')
    unmatched_crm = unmatched_crm.sort_values(by='Date', ascending=False)
    print(f"Unmatched CRM withdrawals DataFrame prepared for {date_str}")
    return unmatched_crm
def generate_matched_deposits(date_str, lists_dir, regulation, compensated_deps=None):
    deposits_matching_path = lists_dir / date_str / f"{regulation}_deposits_matching.xlsx"
    if not deposits_matching_path.exists():
        print(f"Deposits matching file not found: {deposits_matching_path}")
        return None
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
    if 'Regulation' in df.columns: # Replace 'df' with the actual DF name, e.g., matched_df, all_withdrawals, unmatched_crm, etc.
        df['Regulation'] = df['Regulation'].apply(lambda x: 'UK' if str(x).lower() == 'uk' else x)
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
    all_deposits = matched_df.copy()
    if not compensated_formatted.empty:
        all_deposits = pd.concat([all_deposits, compensated_formatted], ignore_index=True)
    all_deposits['Date'] = pd.to_datetime(all_deposits['Date'], errors='coerce')
    all_deposits['sort_group'] = np.where(all_deposits['Match'] == 'No', 0, 1)
    all_deposits = all_deposits.sort_values(by=['sort_group', 'Date'], ascending=[True, False])
    all_deposits = all_deposits.drop(columns=['sort_group'])
    if all_deposits.empty:
        print(f"No data for matched deposits (including cancellations) for {date_str}, skipping.")
        return None
    print(f"Matched deposits (including cancellations) DataFrame prepared for {date_str}")
    return all_deposits


def generate_matched_withdrawals(date_str, regulation, lists_dir, output_dir, compensated_wds=None):
    matching_df = load_matching_df(date_str, lists_dir, output_dir, regulation)
    if matching_df is None:
        return None

    # Filter for matched rows (match_status == 1) including those with Underpaid/Overpaid
    matched_df = matching_df[matching_df['match_status'] == 1].copy()
    if matched_df.empty:
        print(f"No matched withdrawals found for {date_str}.")

    # Make amounts negative
    matched_df['crm_amount'] = matched_df['crm_amount'].apply(lambda x: -abs(x) if pd.notna(x) else x)
    matched_df['proc_amount'] = matched_df['proc_amount'].apply(lambda x: -abs(x) if pd.notna(x) else x)

    # Select and rename columns
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

    if 'Regulation' in matched_df.columns:
        matched_df['Regulation'] = matched_df['Regulation'].apply(lambda x: 'UK' if str(x).lower() == 'uk' else x)
    # *** SYMMETRIC CROSS-REGULATION LOADING (fixed for full ROW <-> UK separation) ***
    cross_lists_dir = config.setup_dirs_for_reg(regulation)['lists_dir'] # Load from *current* reg's lists_dir
    cross_path = cross_lists_dir / date_str / f"{regulation}_cross_regulation.xlsx"
    print(f"Checking for cross-regulation file: {cross_path}") # NEW: always log path
    if cross_path.exists():
        print(f"Found cross-regulation file: {cross_path} (size: {cross_path.stat().st_size} bytes)")
        cross_df = pd.read_excel(cross_path, dtype={'crm_last4': str, 'proc_last4': str})
        if cross_df.empty:
            print("Cross-regulation file is empty, skipping.")
        else:
            cross_df['crm_amount'] = cross_df['crm_amount'].apply(lambda x: -abs(x) if pd.notna(x) else x)
            cross_df['proc_amount'] = cross_df['proc_amount'].apply(lambda x: -abs(x) if pd.notna(x) else x)
            if 'comment' in cross_df.columns:
                cross_df['comment'] = cross_df['comment'].apply(process_comment)
            available_cols = [col for col in columns_map if col in cross_df.columns]
            cross_df = cross_df[available_cols]
            cross_df.rename(columns={k: v for k, v in columns_map.items() if k in available_cols}, inplace=True)
            if 'comment' in cross_df.columns and 'Comment' not in cross_df.columns:
                cross_df.rename(columns={'comment': 'Comment'}, inplace=True) # Fallback direct rename if missed
            cross_df['Comment'] = cross_df['Comment'].fillna('') # Ensure no NaN, show empty string
            cross_df['Match'] = 'Yes'
            if 'Comment' in cross_df.columns and 'Match' in cross_df.columns:
                columns = list(cross_df.columns)
                columns.remove('Match')
                comment_idx = columns.index('Comment')
                columns.insert(comment_idx, 'Match')
                cross_df = cross_df[columns]
            pad_last4(cross_df, 'CRM Last 4 Digits')
            pad_last4(cross_df, 'PSP Last 4 Digits')
            cross_df['Date'] = cross_df['Date'].apply(format_date)
            cross_df['Date'] = pd.to_datetime(cross_df['Date'], errors='coerce')
            if 'Regulation' in cross_df.columns:
                cross_df['Regulation'] = cross_df['Regulation'].apply(lambda x: 'UK' if str(x).lower() == 'uk' else x)
            matched_df = pd.concat([matched_df, cross_df], ignore_index=True)
            print(f"Added {len(cross_df)} cross-regulation rows to matched withdrawals for {regulation}")
    else:
        print(f"Cross-regulation file NOT found: {cross_path}")
    # Load cross-processor
    cross_processor_path = cross_lists_dir / date_str / f"{regulation}_cross_processor.xlsx"
    print(f"Checking for cross-processor file: {cross_processor_path}")
    if cross_processor_path.exists():
        print(f"Found cross-processor file: {cross_processor_path} (size: {cross_processor_path.stat().st_size} bytes)")
        cross_proc_df = pd.read_excel(cross_processor_path, dtype={'crm_last4': str, 'proc_last4': str})
        if cross_proc_df.empty:
            print("Cross-processor file is empty, skipping.")
        else:
            cross_proc_df['crm_amount'] = cross_proc_df['crm_amount'].apply(lambda x: -abs(x) if pd.notna(x) else x)
            cross_proc_df['proc_amount'] = cross_proc_df['proc_amount'].apply(lambda x: -abs(x) if pd.notna(x) else x)
            if 'comment' in cross_proc_df.columns:
                cross_proc_df['comment'] = cross_proc_df['comment'].apply(process_comment)
            available_cols = [col for col in columns_map if col in cross_proc_df.columns]
            cross_proc_df = cross_proc_df[available_cols]
            cross_proc_df.rename(columns={k: v for k, v in columns_map.items() if k in available_cols}, inplace=True)
            if 'comment' in cross_proc_df.columns and 'Comment' not in cross_proc_df.columns:
                cross_proc_df.rename(columns={'comment': 'Comment'}, inplace=True)
            cross_proc_df['Comment'] = cross_proc_df['Comment'].fillna('')
            cross_proc_df['Match'] = 'Yes'
            if 'Comment' in cross_proc_df.columns and 'Match' in cross_proc_df.columns:
                columns = list(cross_proc_df.columns)
                columns.remove('Match')
                comment_idx = columns.index('Comment')
                columns.insert(comment_idx, 'Match')
                cross_proc_df = cross_proc_df[columns]
            pad_last4(cross_proc_df, 'CRM Last 4 Digits')
            pad_last4(cross_proc_df, 'PSP Last 4 Digits')
            cross_proc_df['Date'] = cross_proc_df['Date'].apply(format_date)
            cross_proc_df['Date'] = pd.to_datetime(cross_proc_df['Date'], errors='coerce')
            if 'Regulation' in cross_proc_df.columns:
                cross_proc_df['Regulation'] = cross_proc_df['Regulation'].apply(lambda x: 'UK' if str(x).lower() == 'uk' else x)
            matched_df = pd.concat([matched_df, cross_proc_df], ignore_index=True)
            print(f"Added {len(cross_proc_df)} cross-processor rows to matched withdrawals for {regulation}")
    else:
        print(f"Cross-processor file NOT found: {cross_processor_path}")
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
        compensated_formatted['Payment Method'] = pd.NA # No equivalent
        compensated_formatted['Regulation'] = pd.NA # No equivalent
        compensated_formatted['PSP Processor Name'] = compensated_wds['proc_processor_name']
        compensated_formatted['PSP Last 4 Digits'] = compensated_wds['proc_last4']
        compensated_formatted['Type'] = 'Deposit Cancellation'
        compensated_formatted['Comment'] = "Deposit cancellation within the same day"
        compensated_formatted['Match'] = 'No'
        if 'Regulation' in compensated_formatted.columns:
            compensated_formatted['Regulation'] = compensated_formatted['Regulation'].apply(lambda x: 'UK' if str(x).lower() == 'uk' else x)
    # Concat and sort: compensated first, then by Date descending (avoid FutureWarning by checking empty)
    all_withdrawals = matched_df.copy()
    if not compensated_formatted.empty:
        all_withdrawals = pd.concat([all_withdrawals, compensated_formatted], ignore_index=True)
    all_withdrawals['Date'] = pd.to_datetime(all_withdrawals['Date'], errors='coerce')
    all_withdrawals['sort_group'] = np.where(all_withdrawals['Match'] == 'No', 0, 1)
    all_withdrawals = all_withdrawals.sort_values(by=['sort_group', 'Date'], ascending=[True, False])
    all_withdrawals = all_withdrawals.drop(columns=['sort_group'])
    if all_withdrawals.empty:
        print(f"No data for matched withdrawals (including cancellations) for {date_str}, skipping.")
        return None
    print(f"Matched withdrawals (including cancellations) DataFrame prepared for {date_str}")
    return all_withdrawals
def save_matched_to_excel(date_str, regulation, deps_df, wds_df, output_dir):
    if not ((deps_df is not None and not deps_df.empty) or (wds_df is not None and not wds_df.empty)):
        print(f"No matched data for {date_str} in {regulation}, skipping Matched.xlsx")
        return
    output_path = output_dir / f"{regulation.upper()} Matched.xlsx"
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        if deps_df is not None and not deps_df.empty:
            deps_df.to_excel(writer, index=False, sheet_name='Deps')
            worksheet = writer.sheets['Deps']
            # Set text format for specified columns
            text_columns = ['CRM Last 4 Digits', 'PSP Last 4 Digits', 'CRM Transaction ID', 'PSP Transaction ID']
            for col in text_columns:
                if col in deps_df.columns:
                    col_idx = deps_df.columns.get_loc(col) + 1
                    for row_idx in range(2, len(deps_df) + 2):
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
        if wds_df is not None and not wds_df.empty:
            wds_df.to_excel(writer, index=False, sheet_name='WDs')
            worksheet = writer.sheets['WDs']
            # Set text format for specified columns
            text_columns = ['CRM Last 4 Digits', 'PSP Last 4 Digits']
            for col in text_columns:
                if col in wds_df.columns:
                    col_idx = wds_df.columns.get_loc(col) + 1
                    for row_idx in range(2, len(wds_df) + 2):
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
    print(f"Matched data saved to {output_path} with sheets: Deps, WDs")
def save_unmatched_to_excel(date_str, regulation, crm_deps_df, proc_deps_df, crm_wds_df, proc_wds_df, output_dir):
    output_path = output_dir / f"{regulation.upper()} Unmatched.xlsx"
    dfs = {
        'CRM Deps': crm_deps_df,
        'PSP Deps': proc_deps_df,
        'CRM WDs': crm_wds_df,
        'PSP WDs': proc_wds_df
    }
    non_empty_dfs = {k: v for k, v in dfs.items() if v is not None and not v.empty}
    if not non_empty_dfs:
        print(f"No unmatched data for {date_str} in {regulation}, skipping Unmatched.xlsx")
        return
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        for sheet, df in non_empty_dfs.items():
            if sheet.startswith('CRM') and 'Regulation' in df.columns:
                df['Regulation'] = df['Regulation'].apply(lambda x: 'UK' if str(x).lower() == 'uk' else x)
            df.to_excel(writer, index=False, sheet_name=sheet)
            worksheet = writer.sheets[sheet]
            # Set text format for specified columns based on sheet
            if sheet in ['CRM Deps', 'CRM WDs']:
                text_columns = ['Last 4 Digits']
                if sheet == 'CRM Deps':
                    text_columns += ['Transaction ID']
            else: # PSP Deps or WDs
                text_columns = ['Last 4 Digits']
                if sheet == 'PSP Deps':
                    text_columns += ['Transaction ID']
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
    print(f"Unmatched data saved to {output_path} with sheets: {', '.join(non_empty_dfs.keys())}")
def main(date_str):
    matched_sums = handle_shifts(date_str)
    for regulation in ['row', 'uk']:
        dirs = config.setup_dirs_for_reg(regulation)
        LISTS_DIR = dirs['lists_dir']
        OUTPUT_DIR = dirs['output_dir']
        # Clear OUTPUT_DIR contents fully
        for item in list(OUTPUT_DIR.iterdir()):
            try:
                if item.is_file():
                    item.unlink()
                    print(f"Removed file {item} in OUTPUT_DIR")
                else:
                    shutil.rmtree(item)
                    print(f"Removed dir {item} in OUTPUT_DIR")
            except Exception as e:
                print(f"Failed to remove {item}: {e}")
        output_dir = OUTPUT_DIR / date_str
        output_dir.mkdir(parents=True, exist_ok=True)
        # Replace safechargeuk with safecharge in matching files
        for matching_file in [f"{regulation}_deposits_matching.xlsx", f"{regulation}_withdrawals_matching.xlsx"]:
            matching_path = LISTS_DIR / date_str / matching_file
            if matching_path.exists():
                df = pd.read_excel(matching_path)
                if 'proc_processor_name' in df.columns:
                    df['proc_processor_name'] = df['proc_processor_name'].replace('safechargeuk', 'safecharge')
                    df.to_excel(matching_path, index=False)
                    print(f"Updated {matching_path} by replacing safechargeuk with safecharge in proc_processor_name")
        if matched_sums and regulation in matched_sums:
            df = pd.DataFrame([matched_sums[regulation]])
            if df.empty:
                print(f"No shifts data for {date_str} in {regulation}, skipping file creation.")
            else:
                output_path = output_dir / f"{regulation.upper()} total_shifts_by_currency.xlsx"
                save_excel(df, output_path)
                print(f"Total shifts by currency saved to {output_path}")
        generate_warning_withdrawals(date_str, lists_dir=LISTS_DIR, output_dir=output_dir, regulation=regulation)
        crm_deps_df = generate_unmatched_crm_deposits(date_str, lists_dir=LISTS_DIR, regulation=regulation)
        generate_unapproved_crm_deposits(date_str, lists_dir=LISTS_DIR, output_dir=output_dir, regulation=regulation)
        proc_deps_df = generate_unmatched_proc_deposits(date_str, lists_dir=LISTS_DIR, regulation=regulation)
        proc_wds_df = generate_unmatched_proc_withdrawals(date_str, lists_dir=LISTS_DIR, output_dir=output_dir, regulation=regulation)
        proc_deps_df, proc_wds_df, compensated_deps, compensated_wds = remove_compensated_entries(proc_deps_df, proc_wds_df)
        crm_wds_df = generate_unmatched_crm_withdrawals(date_str, lists_dir=LISTS_DIR, output_dir=output_dir, regulation=regulation)
        deps_df = generate_matched_deposits(date_str, lists_dir=LISTS_DIR, regulation=regulation, compensated_deps=compensated_deps)
        wds_df = generate_matched_withdrawals(date_str, regulation, lists_dir=LISTS_DIR, output_dir=output_dir, compensated_wds=compensated_wds)
        save_matched_to_excel(date_str, regulation, deps_df, wds_df, output_dir=output_dir)
        save_unmatched_to_excel(date_str, regulation, crm_deps_df, proc_deps_df, crm_wds_df, proc_wds_df, output_dir)
if __name__ == "__main__":
    DATE = sys.argv[1] if len(sys.argv) > 1 else "2025-10-22"
    main(DATE)