# src/output.py

import sys
import pandas as pd
from pathlib import Path
from src.config import OUTPUT_DIR, LISTS_DIR
from src.shifts_handler import main as handle_shifts


def generate_unmatched_crm_deposits(date_str):
    deposits_matching_path = LISTS_DIR / date_str / "deposits_matching.xlsx"
    if not deposits_matching_path.exists():
        print(f"Deposits matching file not found: {deposits_matching_path}")
        return

    df = pd.read_excel(deposits_matching_path)

    # Filter unmatched CRM deposits: match_status == 0 and proc_date is NaN (indicating CRM unmatched)
    unmatched_crm = df[(df['match_status'] == 0) & (df['proc_date'].isna())]

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


if __name__ == "__main__":
    DATE = sys.argv[1] if len(
        sys.argv) > 1 else "2025-09-01"  # Default date for testing; use command-line arg in production
    matched_sums = handle_shifts(DATE)
    if matched_sums:
        output_dir = OUTPUT_DIR / DATE
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "total_shifts_by_currency.csv"
        df = pd.DataFrame([matched_sums])
        df.to_csv(output_path, index=False)
        print(f"Total shifts by currency saved to {output_path}")

    # Generate unmatched_crm_deposits
    generate_unmatched_crm_deposits(DATE)

    # Generate unmatched_proc_deposits
    generate_unmatched_proc_deposits(DATE)