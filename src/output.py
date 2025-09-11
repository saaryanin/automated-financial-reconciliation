# src/output.py
import sys
import pandas as pd
from pathlib import Path
import shutil
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))
from src.config import OUTPUT_DIR, LISTS_DIR
from src.shifts_handler import main as handle_shifts, is_bst, get_cutoff_time
from collections import OrderedDict
import ast
from datetime import datetime
import numpy as np
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
def generate_unmatched_crm_deposits(date_str):
    deposits_matching_path = LISTS_DIR / date_str / "deposits_matching.xlsx"
    if not deposits_matching_path.exists():
        print(f"Deposits matching file not found: {deposits_matching_path}")
        return
    df = pd.read_excel(deposits_matching_path)
    # Filter unmatched CRM deposits: match_status == 0 and proc_date is NaN (indicating CRM unmatched)
    unmatched_crm = df[(df['match_status'] == 0) & (df['proc_date'].isna())]
    unmatched_crm = unmatched_crm.copy() # Fix SettingWithCopyWarning
    if unmatched_crm.empty:
        print(f"No unmatched CRM deposits found for {date_str}, skipping file creation.")
        return
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
    df = pd.read_excel(deposits_matching_path)
    # Filter unapproved CRM deposits: match_status == 1 and crm_approved == 'No'
    unapproved_crm = df[(df['match_status'] == 1) & (df['crm_approved'] == 'No')]
    unapproved_crm = unapproved_crm.copy() # Fix SettingWithCopyWarning
    if unapproved_crm.empty:
        print(f"No unapproved CRM deposits found for {date_str}, skipping file creation.")
        return
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
    df = pd.read_excel(deposits_matching_path, dtype={'proc_transaction_id': str, 'proc_last4': str})
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
    # Format proc_date
    unmatched_proc.loc[:, 'proc_date'] = unmatched_proc['proc_date'].apply(format_date)
    # Ensure proc_transaction_id and proc_last4 are strings
    unmatched_proc['proc_transaction_id'] = unmatched_proc['proc_transaction_id'].astype(str)
    unmatched_proc['proc_last4'] = unmatched_proc['proc_last4'].astype(str)
    # Pad proc_last4 with leading zeros to make it 4 digits
    unmatched_proc['proc_last4'] = unmatched_proc['proc_last4'].apply(lambda x: x.zfill(4) if x else '')
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
            return int(val)
    if isinstance(val, str):
        val = val.strip("'\"")
    if pd.isna(val):
        return np.nan
    return val
def format_date(val):
    if pd.isna(val):
        return val
    if isinstance(val, datetime):
        return val.strftime('%d/%m/%Y %I:%M:%S %p')
    if isinstance(val, str):
        val = val.strip()
        try:
            dt = datetime.strptime(val, '%Y-%m-%d %H:%M:%S')
            return dt.strftime('%d/%m/%Y %I:%M:%S %p')
        except ValueError:
            try:
                dt = pd.to_datetime(val)
                return dt.strftime('%d/%m/%Y %I:%M:%S %p')
            except:
                return val
    return val
def process_comment(comment):
    if pd.isna(comment):
        return ''
    parts = [p.strip() for p in comment.split(' . ')]
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
def generate_warning_withdrawals(date_str):
    withdrawals_matching_path = LISTS_DIR / date_str / "withdrawals_matching.xlsx"
    if not withdrawals_matching_path.exists():
        print(f"Withdrawals matching file not found: {withdrawals_matching_path}")
        return
    df = pd.read_excel(withdrawals_matching_path)
    # Filter rows where warning == True
    warnings_df = df[df['warning'] == True]
    warnings_df = warnings_df.copy() # Fix SettingWithCopyWarning
    if warnings_df.empty:
        print(f"No warnings found in withdrawals matching for {date_str}, skipping file creation.")
        return
    # Clean processor columns
    columns_to_clean = [
        'proc_date', 'proc_email', 'proc_tp', 'proc_firstname', 'proc_lastname',
        'proc_last4', 'proc_currency', 'proc_amount', 'proc_amount_crm_currency'
    ]
    for col in columns_to_clean:
        if col in warnings_df.columns:
            warnings_df.loc[:, col] = warnings_df[col].apply(clean_value)
    # Format proc_date
    warnings_df.loc[:, 'proc_date'] = warnings_df['proc_date'].apply(format_date)
    # Make amounts negative
    warnings_df.loc[:, 'crm_amount'] = warnings_df['crm_amount'].apply(lambda x: -abs(x) if pd.notna(x) else x)
    warnings_df.loc[:, 'proc_amount'] = warnings_df['proc_amount'].apply(lambda x: -abs(x) if pd.notna(x) else x)
    # Process the comment column
    warnings_df.loc[:, 'comment'] = warnings_df['comment'].apply(process_comment)
    # Select specified columns
    columns = [
        'crm_date', 'crm_email', 'crm_firstname', 'crm_lastname', 'crm_tp', 'crm_last4', 'crm_currency', 'crm_amount',
        'crm_processor_name', 'regulation', 'proc_date', 'proc_email', 'proc_tp', 'proc_firstname', 'proc_lastname', 'proc_last4',
        'proc_currency', 'proc_amount', 'proc_amount_crm_currency', 'proc_processor_name', 'comment'
    ]
    warnings_df = warnings_df[columns]
    # Save to output/dated/warnings_withdrawals.xlsx
    output_dir = OUTPUT_DIR / date_str
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "warnings_withdrawals.xlsx"
    save_excel(warnings_df, output_path, text_columns=['crm_last4', 'proc_last4'])
    print(f"Warnings withdrawals saved to {output_path}")
def generate_unmatched_proc_withdrawals(date_str):
    withdrawals_matching_path = LISTS_DIR / date_str / "withdrawals_matching.xlsx"
    if not withdrawals_matching_path.exists():
        print(f"Withdrawals matching file not found: {withdrawals_matching_path}")
        return
    df = pd.read_excel(withdrawals_matching_path)
    print(f"Total rows in withdrawals_matching: {len(df)}")
    # Filter rows where warning == False
    df = df[df['warning'] == False]
    print(f"Rows after warning == False: {len(df)}")
    # Filter unmatched processor withdrawals: match_status == 0 and comment == "No matching CRM row found"
    unmatched_proc = df[(df['match_status'] == 0) & (df['comment'] == "No matching CRM row found")]
    unmatched_proc = unmatched_proc.copy() # Fix SettingWithCopyWarning if needed in future mods
    print(f"Rows after match_status==0 and comment=='No matching CRM row found': {len(unmatched_proc)}")
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
    # Clean processor columns
    columns_to_clean = [
        'proc_date', 'proc_email', 'proc_tp', 'proc_firstname', 'proc_lastname',
        'proc_last4', 'proc_currency', 'proc_amount', 'proc_processor_name'
    ]
    for col in columns_to_clean:
        if col in unmatched_proc.columns:
            unmatched_proc.loc[:, col] = unmatched_proc[col].apply(clean_value)
    # Format proc_date
    unmatched_proc.loc[:, 'proc_date'] = unmatched_proc['proc_date'].apply(format_date)
    # Make amounts negative
    unmatched_proc.loc[:, 'proc_amount'] = unmatched_proc['proc_amount'].apply(lambda x: -abs(x) if pd.notna(x) else x)
    # Manually add Type as 'Withdrawal'
    unmatched_proc['Type'] = 'Withdrawal'
    # Select specified columns in order (excluding comment and proc_amount_crm_currency)
    columns = [
        'Type', 'proc_date', 'proc_firstname', 'proc_lastname', 'proc_email',
        'proc_amount', 'proc_currency', 'proc_tp', 'proc_processor_name', 'proc_last4'
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
        'proc_last4': 'Last 4 Digits'
    }
    unmatched_proc.rename(columns=rename_dict, inplace=True)
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
        return
    deposits_df = pd.read_excel(deposits_path, dtype={'Last 4 Digits': str, 'Transaction ID': str})
    withdrawals_df = pd.read_excel(withdrawals_path, dtype={'Last 4 Digits': str})
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
    deposits_df['norm_last4'] = deposits_df['proc_last4'].apply(lambda x: str(x).zfill(4) if pd.notna(x) else np.nan)
    # For withdrawals, last4 is already 4 digits with leading zeros
    withdrawals_df['norm_last4'] = withdrawals_df['proc_last4'].apply(lambda x: str(x) if pd.notna(x) else np.nan)
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
        return
    # Get indices to drop
    dep_indices_to_drop = matched['index_dep'].unique()
    wd_indices_to_drop = matched['index_wd'].unique()
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
def generate_unmatched_crm_withdrawals(date_str):
    withdrawals_matching_path = LISTS_DIR / date_str / "withdrawals_matching.xlsx"
    if not withdrawals_matching_path.exists():
        print(f"Withdrawals matching file not found: {withdrawals_matching_path}")
        return
    df = pd.read_excel(withdrawals_matching_path)
    # Apply warning == False to all groups
    df = df[df['warning'] == False]
    # Group 1: match_status == 0 and payment_status == 0 and comment == "No matching processor row found"
    group1 = df[(df['match_status'] == 0) & (df['payment_status'] == 0) & (df['comment'] == "No matching processor row found")].copy()
    # Group 2: match_status == 1 and payment_status == 0 and (comment contains "Overpaid" or "Underpaid")
    group2 = df[(df['match_status'] == 1) & (df['payment_status'] == 0) & (df['comment'].str.contains("Overpaid|Underpaid", na=False))].copy()
    # Group 3: comment == "Withdrawal cancelled with no matching withdrawal found"
    group3 = df[df['comment'] == "Withdrawal cancelled with no matching withdrawal found"].copy()
    # Group 4: comment contains "Unmatched due to warning"
    group4 = df[df['comment'].str.contains("Unmatched due to warning", na=False)].copy()
    # Process Group 1
    if not group1.empty:
        group1['comment'] = '' # Blank comment
        # Ensure crm_amount is negative
        group1['crm_amount'] = group1['crm_amount'].apply(lambda x: -abs(x) if pd.notna(x) else x)
    # Process Group 3 like Group 1 but crm_amount positive and comment "Withdrawal cancellation"
    if not group3.empty:
        group3['comment'] = "Withdrawal cancellation"
        # Make crm_amount positive
        group3['crm_amount'] = group3['crm_amount'].apply(lambda x: abs(x) if pd.notna(x) else x)
    # Process Group 2: Parse comment for underpaid/overpaid amount and update crm_amount and comment
    if not group2.empty:
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
    # Process Group 4: Keep as is, crm_amount negative
    if not group4.empty:
        group4['crm_amount'] = group4['crm_amount'].apply(lambda x: -abs(x) if pd.notna(x) else x)
    # Combine all groups
    unmatched_crm = pd.concat([group1, group2, group3, group4], ignore_index=True)
    if unmatched_crm.empty:
        print(f"No unmatched CRM withdrawals found for {date_str}, skipping file creation.")
        return
    # Select specified columns
    columns = [
        'crm_type', 'crm_date', 'crm_firstname', 'crm_lastname', 'crm_email', 'crm_amount', 'crm_currency',
        'crm_tp', 'regulation', 'crm_processor_name', 'crm_last4', 'comment'
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
        'regulation': 'Regulation',
        'crm_processor_name': 'Processor Name',
        'crm_last4': 'Last 4 Digits',
        'comment': 'Comment'
    }
    unmatched_crm.rename(columns=rename_dict, inplace=True)
    # Save to output/dated/unmatched_crm_withdrawals.xlsx
    output_dir = OUTPUT_DIR / date_str
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "Unmatched CRM Withdrawals.xlsx"
    save_excel(unmatched_crm, output_path, text_columns=['Last 4 Digits'])
    print(f"Unmatched CRM withdrawals saved to {output_path}")
def main(date_str):
    # For standalone testing; in frontend, phases are called separately
    # Phase 1 (handled in third_window): clear, handle_shifts, warnings (user-edited)
    output_dir = OUTPUT_DIR / date_str
    if output_dir.exists():
        shutil.rmtree(output_dir)
        print(f"Cleared old output directory: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)
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
    remove_compensated_entries(date_str)
    generate_unmatched_crm_withdrawals(date_str)
if __name__ == "__main__":
    DATE = sys.argv[1] if len(sys.argv) > 1 else "2025-09-02" # Default date for testing; use command-line arg in production
    main(DATE)