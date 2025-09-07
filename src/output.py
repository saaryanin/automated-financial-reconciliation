# src/output.py

import sys
import pandas as pd
from pathlib import Path
from src.config import OUTPUT_DIR, LISTS_DIR
from src.shifts_handler import main as handle_shifts
from collections import OrderedDict
import ast
from datetime import datetime
import numpy as np


def generate_unmatched_crm_deposits(date_str):
    deposits_matching_path = LISTS_DIR / date_str / "deposits_matching.xlsx"
    if not deposits_matching_path.exists():
        print(f"Deposits matching file not found: {deposits_matching_path}")
        return

    df = pd.read_excel(deposits_matching_path)

    # Filter unmatched CRM deposits: match_status == 0 and proc_date is NaN (indicating CRM unmatched)
    unmatched_crm = df[(df['match_status'] == 0) & (df['proc_date'].isna())]

    if unmatched_crm.empty:
        print(f"No unmatched CRM deposits found for {date_str}, skipping file creation.")
        return

    # Select specified columns
    columns = [
        'crm_date', 'crm_firstname', 'crm_lastname', 'crm_email', 'crm_tp', 'crm_amount', 'crm_currency',
        'payment_method', 'crm_approved', 'crm_processor_name', 'crm_last4', 'regulation', 'crm_transaction_id'
    ]
    unmatched_crm = unmatched_crm[columns]

    # Save to output/dated/unmatched_crm_deposits.xlsx
    output_dir = OUTPUT_DIR / date_str
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "unmatched_crm_deposits.xlsx"
    unmatched_crm.to_excel(output_path, index=False)
    print(f"Unmatched CRM deposits saved to {output_path}")


def generate_unmatched_proc_deposits(date_str):
    deposits_matching_path = LISTS_DIR / date_str / "deposits_matching.xlsx"
    if not deposits_matching_path.exists():
        print(f"Deposits matching file not found: {deposits_matching_path}")
        return

    df = pd.read_excel(deposits_matching_path)

    # Filter unmatched processor deposits: match_status == 0 and crm_date is NaN (indicating processor unmatched)
    unmatched_proc = df[(df['match_status'] == 0) & (df['crm_date'].isna())]

    if unmatched_proc.empty:
        print(f"No unmatched processor deposits found for {date_str}, skipping file creation.")
        return

    # Clean processor columns
    columns_to_clean = [
        'proc_date', 'proc_firstname', 'proc_lastname', 'proc_email', 'proc_tp', 'proc_amount', 'proc_currency',
        'proc_processor_name', 'proc_transaction_id'
    ]
    for col in columns_to_clean:
        if col in unmatched_proc.columns:
            unmatched_proc.loc[:, col] = unmatched_proc[col].apply(clean_value)

    # Format proc_date
    unmatched_proc.loc[:, 'proc_date'] = unmatched_proc['proc_date'].apply(format_date)

    # Select specified columns
    columns = [
        'proc_date', 'proc_firstname', 'proc_lastname', 'proc_email', 'proc_tp', 'proc_amount', 'proc_currency',
        'proc_processor_name', 'proc_transaction_id'
    ]
    unmatched_proc = unmatched_proc[columns]

    # Save to output/dated/unmatched_proc_deposits.xlsx
    output_dir = OUTPUT_DIR / date_str
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "unmatched_proc_deposits.xlsx"
    unmatched_proc.to_excel(output_path, index=False)
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
            if '*' in email:  # masked
                if lower_email not in masked_emails and lower_email not in full_emails:
                    masked_emails[lower_email] = email
            else:  # full
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
    warnings_df.to_excel(output_path, index=False)
    print(f"Warnings withdrawals saved to {output_path}")


def generate_unmatched_proc_withdrawals(date_str):
    withdrawals_matching_path = LISTS_DIR / date_str / "withdrawals_matching.xlsx"
    if not withdrawals_matching_path.exists():
        print(f"Withdrawals matching file not found: {withdrawals_matching_path}")
        return

    df = pd.read_excel(withdrawals_matching_path)

    # Filter rows where comment == "No matching CRM row found"
    unmatched_proc = df[df['comment'] == "No matching CRM row found"]

    if unmatched_proc.empty:
        print(f"No unmatched processor withdrawals found for {date_str}, skipping file creation.")
        return

    # Clean processor columns
    columns_to_clean = [
        'proc_date', 'proc_email', 'proc_tp', 'proc_firstname', 'proc_lastname',
        'proc_last4', 'proc_currency', 'proc_amount', 'proc_amount_crm_currency', 'proc_processor_name'
    ]
    for col in columns_to_clean:
        if col in unmatched_proc.columns:
            unmatched_proc.loc[:, col] = unmatched_proc[col].apply(clean_value)

    # Format proc_date
    unmatched_proc.loc[:, 'proc_date'] = unmatched_proc['proc_date'].apply(format_date)

    # Make amounts negative
    unmatched_proc.loc[:, 'proc_amount'] = unmatched_proc['proc_amount'].apply(lambda x: -abs(x) if pd.notna(x) else x)
    unmatched_proc.loc[:, 'proc_amount_crm_currency'] = unmatched_proc['proc_amount_crm_currency'].apply(lambda x: -abs(x) if pd.notna(x) else x)

    # Select specified columns (excluding comment)
    columns = [
        'proc_date', 'proc_email', 'proc_tp', 'proc_firstname', 'proc_lastname', 'proc_last4',
        'proc_currency', 'proc_amount', 'proc_amount_crm_currency', 'proc_processor_name'
    ]
    unmatched_proc = unmatched_proc[columns]

    # Save to output/dated/unmatched_proc_withdrawals.xlsx
    output_dir = OUTPUT_DIR / date_str
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "unmatched_proc_withdrawals.xlsx"
    unmatched_proc.to_excel(output_path, index=False)
    print(f"Unmatched processor withdrawals saved to {output_path}")


if __name__ == "__main__":
    DATE = sys.argv[1] if len(
        sys.argv) > 1 else "2025-08-26"  # Default date for testing; use command-line arg in production
    matched_sums = handle_shifts(DATE)
    if matched_sums:
        output_dir = OUTPUT_DIR / DATE
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "total_shifts_by_currency.csv"
        df = pd.DataFrame([matched_sums])
        if df.empty:
            print(f"No shifts data for {DATE}, skipping file creation.")
        else:
            df.to_csv(output_path, index=False)
            print(f"Total shifts by currency saved to {output_path}")

    # Generate unmatched_crm_deposits
    generate_unmatched_crm_deposits(DATE)

    # Generate unmatched_proc_deposits
    generate_unmatched_proc_deposits(DATE)

    # Generate warning_withdrawals
    generate_warning_withdrawals(DATE)

    # Generate unmatched_proc_withdrawals
    generate_unmatched_proc_withdrawals(DATE)