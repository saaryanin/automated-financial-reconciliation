import pandas as pd
import re
from pathlib import Path
from datetime import datetime, timedelta
from src.preprocess import PSP_NAME_MAP, extract_date_from_filename
from src.config import DATA_DIR, RAW_TRACKING_DIR, PROCESSED_DIR, LISTS_DIR
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# --- Configuration ---
DATE = "2025-07-11"  # Adjust as needed; can be made configurable

def extract_crm_transaction_id(comment: str, processor: str, method: str = ''):
    text = str(comment)
    processor = processor.lower()
    method = str(method).upper()  # Normalize method to uppercase for matching
    patterns = {
        "paypal": r"PSP TransactionId:([A-Z0-9]+)",
        "safecharge": r"PSP TransactionId:([12]\d{18})|More Comment:[^$]*\$(\d{19})|,\s*([12]\d{18})\s*,",
        "powercash": r"PSP TransactionId:(\d+)",
        "shift4": r"More Comment:[^$]*\$(\w+)",
        "skrill": r"More Comment:[^$]*\$(\d+)",
        "neteller": r"More Comment:[^$]*\$(\d+)",
        "trustpayments": r"PSP TransactionId:([\d\-]+)|More Comment:[^$]*\$(\d{2}-\d{2}-\d+)",
        "bitpay": r"PSP TransactionId:([A-Za-z0-9]+)",
        "ezeebill": r"(\d{7}-\d{18})",
    }
    # Special handling for zotapay_paymentasia (covers both zotapay and paymentasia cases)
    if processor == "zotapay_paymentasia":
        if 'PA-MY' in method:  # For paymentasia format
            pattern = r"(\d{7}-\d{18})"
        elif 'ZOTAPAY-CUP' in method:  # For zotapay format
            pattern = r"PSP TransactionId:(\d+)"
        else:
            pattern = r"PSP TransactionId:(\d+)|(\d{7}-\d{18})"  # Fallback: try both
    else:
        pattern = patterns.get(processor)

    if not pattern:
        return None
    match = re.search(pattern, text)
    if match:
        return next((g for g in match.groups() if g), None)
    return None


def combine_tracker_df(df: pd.DataFrame, date_str: str, tracker_name: str, target_cols: list, save_combined=True) -> pd.DataFrame:
    """
    Combine rows in the DF based on crm_tp, crm_last4, and crm_processor_name.
    - Sum crm_amount.
    - List crm_date sorted from older to newer.
    - List crm_transaction_id if present.
    - Keep other columns from the first row.
    - Reorder columns to match target_cols order.
    - Save to lists/combined/<date>/preprocessed_<tracker_name>_combined.xlsx if save_combined.
    """
    if 'crm_tp' not in df or 'crm_last4' not in df or 'crm_processor_name' not in df or df.empty:
        return df  # No combining if keys missing or empty

    # Ensure crm_date is datetime for sorting
    if 'crm_date' in df.columns:
        df['crm_date'] = pd.to_datetime(df['crm_date'], errors='coerce')

    agg_dict = {
        'crm_amount': 'sum',
        'crm_date': lambda x: sorted(x.dropna().tolist()),  # List sorted dates
    }
    if 'crm_transaction_id' in df.columns:
        agg_dict['crm_transaction_id'] = lambda x: x.dropna().tolist()  # List transaction_ids

    # Other columns: take first
    other_cols = [col for col in df.columns if col not in ['crm_tp', 'crm_last4', 'crm_processor_name', 'crm_amount', 'crm_date', 'crm_transaction_id']]
    agg_dict.update({col: 'first' for col in other_cols})

    combined_df = df.groupby(['crm_tp', 'crm_last4', 'crm_processor_name']).agg(agg_dict).reset_index()

    # Reorder columns to match target_cols
    existing_cols = [col for col in target_cols if col in combined_df.columns]
    combined_df = combined_df[existing_cols]

    # Convert lists to string for Excel if needed, but keep as list for DF
    if save_combined:
        combined_dir = LISTS_DIR / "combined" / date_str
        combined_dir.mkdir(parents=True, exist_ok=True)
        combined_path = combined_dir / f'preprocessed_{tracker_name}_combined.xlsx'
        # For saving, convert lists to comma-separated strings to avoid issues in Excel
        save_df = combined_df.copy()
        if 'crm_date' in save_df.columns:
            save_df['crm_date'] = save_df['crm_date'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
        if 'crm_transaction_id' in save_df.columns:
            save_df['crm_transaction_id'] = save_df['crm_transaction_id'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
        save_df.to_excel(combined_path, index=False)
        print(f"✅ Combined {tracker_name} saved to {combined_path}")

    return combined_df


def preprocess_unapproved_deposits(filepath: Path, save_clean=True, custom_date: str = None) -> pd.DataFrame:
    """
    Preprocess the unapproved deposits tracking list to match the structure of combined CRM deposits.
    - Load the Excel file.
    - Trim rows after at least 2 consecutive fully empty rows.
    - Rename and transform columns as specified.
    - Generate crm_transaction_id from Internal Comment using existing extraction logic.
    - Convert Approved to crm_approved (Yes/No -> 1/0).
    - Standardize PSP names.
    - Clean data types (amount to numeric abs, date to datetime string, strings stripped).
    - Drop unnecessary columns.
    - Add empty 'comment' column at the end.
    - Optionally save to lists/unapproved_dep/<extracted_date>/preprocessed_unapproved_deposits.xlsx.
    - Use custom_date (e.g., '2025-07-11') to override the extracted date for the folder.
    """
    if not filepath.exists():
        raise FileNotFoundError(f"Unapproved deposits file not found: {filepath}")

    # Load the file
    df = pd.read_excel(filepath, engine='openpyxl')
    df.columns = df.columns.str.strip()

    # Find the end of relevant data: first occurrence of at least 2 consecutive fully NaN rows
    nan_rows = df.isna().all(axis=1)
    consecutive_nan = nan_rows & nan_rows.shift(-1, fill_value=False)
    first_double_nan = consecutive_nan[consecutive_nan].index.min()
    if pd.notna(first_double_nan):
        df = df.iloc[:first_double_nan]

    # Drop any remaining trailing fully NaN rows
    df = df.dropna(how='all')

    # Standardize PSP name using the map
    if 'PSP name' in df.columns:
        df['PSP name'] = (
            df['PSP name']
            .astype(str)
            .str.strip()
            .str.lower()
            .replace(PSP_NAME_MAP)
        )

    # Rename columns to match combined CRM deposits
    rename_dict = {
        'Name': 'crm_type',
        'Created On': 'crm_date',
        'First Name (Account) (Account)': 'crm_firstname',
        'Last Name (Account) (Account)': 'crm_lastname',
        'Email (Account) (Account)': 'crm_email',
        'Amount': 'crm_amount',
        'Base Currency (TP Account) (TP Account)': 'crm_currency',
        'Approved': 'crm_approved',
        'TP Account': 'crm_tp',
        'Site (Account) (Account)': 'crm_regulation',
        'PSP name': 'crm_processor_name',
        'CC Last 4 Digits': 'crm_last4',
        'Transaction id': 'crm_transaction_id'  # Will be overwritten
    }
    df = df.rename(columns=rename_dict)

    # Standardize currencies: US Dollar -> USD, Euro -> EUR
    if 'crm_currency' in df.columns:
        df['crm_currency'] = df['crm_currency'].replace({"US Dollar": "USD", "Euro": "EUR"})

    # Transform crm_approved: Yes -> 1, No -> 0
    if 'crm_approved' in df.columns:
        df['crm_approved'] = df['crm_approved'].astype(str).str.strip().str.lower().map({'yes': 1, 'no': 0}).fillna(0)

    # Generate crm_transaction_id from Internal Comment using per-row PSP name and Method of Payment
    if 'Internal Comment' in df.columns and 'crm_processor_name' in df.columns and 'Method of Payment' in df.columns:
        df['crm_transaction_id'] = df.apply(
            lambda row: extract_crm_transaction_id(row['Internal Comment'], row['crm_processor_name'],
                                                   row['Method of Payment']),
            axis=1
        )
        df['crm_transaction_id'] = df['crm_transaction_id'].astype(str).fillna('')

    # Clean data types, similar to other preprocessing
    if 'crm_amount' in df.columns:
        df['crm_amount'] = abs(pd.to_numeric(df['crm_amount'], errors='coerce').fillna(0))

    if 'crm_date' in df.columns:
        df['crm_date'] = pd.to_datetime(df['crm_date'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')

    str_cols = ['crm_type', 'crm_firstname', 'crm_lastname', 'crm_email', 'crm_currency', 'crm_tp',
                'Method of Payment', 'crm_regulation', 'crm_processor_name', 'crm_last4', 'crm_transaction_id']
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().fillna('')

    # Drop unnecessary columns
    drop_cols = ['Approved On', 'Internal Comment', 'Internal Type', 'Master Country (Account) (Account)']
    df = df.drop(columns=[col for col in drop_cols if col in df.columns])

    # Ensure only relevant columns are kept (matching combined CRM deposits structure)
    target_cols = [
        'crm_type', 'crm_date', 'crm_firstname', 'crm_lastname', 'crm_email',
        'crm_amount', 'crm_currency', 'crm_approved', 'crm_tp', 'Method of Payment',
        'crm_regulation', 'crm_processor_name', 'crm_last4', 'crm_transaction_id'
    ]
    df = df[[col for col in target_cols if col in df.columns]]

    # Add empty 'comment' column at the end
    df['comment'] = ''

    # Optionally save preprocessed
    if save_clean:
        date_str = extract_date_from_filename(str(filepath))  # Convert to str before calling
        if date_str == 'unknown_date':
            date_str = datetime.now().strftime('%Y-%m-%d')  # Fallback to current date if not found
        if custom_date:
            date_str = custom_date  # Override with custom date if provided
        out_dir = LISTS_DIR / "unapproved_dep" / date_str
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / 'preprocessed_unapproved_deposits.xlsx'
        with pd.ExcelWriter(out_path, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Sheet1')
            if 'crm_transaction_id' in df.columns:
                worksheet = writer.sheets['Sheet1']
                trans_col_idx = df.columns.get_loc('crm_transaction_id') + 1
                for row in range(2, len(df) + 2):
                    worksheet.cell(row=row, column=trans_col_idx).number_format = '@'
        print(f"✅ Preprocessed unapproved deposits saved to {out_path}")

    # Combine and save combined version, passing target_cols for order
    combined_df = combine_tracker_df(df, date_str, 'unapproved_deposits', target_cols + ['comment'])

    return combined_df  # Return combined for further use if needed


def preprocess_unexecuted_withdrawals(filepath: Path, save_clean=True, custom_date: str = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Preprocess the unexecuted withdrawals tracking list and split into underpay and overpay files.
    - Load the Excel file.
    - Trim rows until at least 1 fully empty row.
    - Rename and transform columns as specified.
    - Standardize currencies (US Dollar -> USD, Euro -> EUR).
    - Split into underpays (negative amounts) and overpays (positive amounts).
    - Add 'comment' column from 'Comments', keep as is.
    - Drop unnecessary columns.
    - Optionally save preprocessed to lists/underpays_wd/<extracted_date>/preprocessed_underpays_withdrawals.xlsx and overpays_wd similarly.
    - Combine each and save to lists/combined/<date>/preprocessed_underpays_withdrawals_combined.xlsx etc.
    - Use custom_date to override the extracted date for the folder.
    - Returns (combined_underpays_df, combined_overpays_df)
    """
    if not filepath.exists():
        raise FileNotFoundError(f"Unexecuted withdrawals file not found: {filepath}")

    # Load the file
    df = pd.read_excel(filepath, engine='openpyxl')
    df.columns = df.columns.str.strip()

    # Find the end of relevant data: first fully NaN row
    nan_rows = df.isna().all(axis=1)
    first_nan = nan_rows[nan_rows].index.min()
    if pd.notna(first_nan):
        df = df.iloc[:first_nan]

    # Drop any remaining trailing fully NaN rows (though unlikely)
    df = df.dropna(how='all')

    # Standardize PSP name using the map
    if 'PSP name' in df.columns:
        df['PSP name'] = (
            df['PSP name']
            .astype(str)
            .str.strip()
            .str.lower()
            .replace(PSP_NAME_MAP)
        )

    # Rename columns as specified
    rename_dict = {
        'Name': 'crm_type',
        'Created On': 'crm_date',
        'First Name (Account) (Account)': 'crm_firstname',
        'Last Name (Account) (Account)': 'crm_lastname',
        'Email (Account) (Account)': 'crm_email',
        'Amount': 'crm_amount',
        'Currency': 'crm_currency',
        'TP Account': 'crm_tp',
        'Site (Account) (Account)': 'crm_regulation',
        'PSP name': 'crm_processor_name',
        'CC Last 4 Digits': 'crm_last4',
        'Comments': 'comment'
    }
    df = df.rename(columns=rename_dict)

    # Standardize currencies: US Dollar -> USD, Euro -> EUR
    if 'crm_currency' in df.columns:
        df['crm_currency'] = df['crm_currency'].replace({"US Dollar": "USD", "Euro": "EUR"})

    # Clean data types
    if 'crm_amount' in df.columns:
        df['crm_amount'] = pd.to_numeric(df['crm_amount'], errors='coerce').fillna(0)

    if 'crm_date' in df.columns:
        df['crm_date'] = pd.to_datetime(df['crm_date'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')

    str_cols = ['crm_type', 'crm_firstname', 'crm_lastname', 'crm_email', 'crm_currency', 'crm_tp',
                'Method of Payment', 'crm_regulation', 'crm_processor_name', 'crm_last4', 'comment']
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().fillna('')

    # Drop unnecessary columns
    drop_cols = ['Approved', 'Approved On', 'Internal Comment', 'Internal Type', 'Country Of Residence (Account) (Account)']
    df = df.drop(columns=[col for col in drop_cols if col in df.columns])

    # Ensure only relevant columns are kept
    target_cols = [
        'crm_type', 'crm_date', 'crm_firstname', 'crm_lastname', 'crm_email',
        'crm_amount', 'crm_currency', 'crm_tp', 'Method of Payment',
        'crm_regulation', 'crm_processor_name', 'crm_last4', 'comment'
    ]
    df = df[[col for col in target_cols if col in df.columns]]

    # Split into underpay (negative) and overpay (positive)
    underpay_df = df[df['crm_amount'] < 0].copy()
    overpay_df = df[df['crm_amount'] > 0].copy()

    # Optionally save preprocessed
    if save_clean:
        date_str = extract_date_from_filename(str(filepath))  # Convert to str before calling
        if date_str == 'unknown_date':
            date_str = datetime.now().strftime('%Y-%m-%d')  # Fallback to current date if not found
        if custom_date:
            date_str = custom_date  # Override with custom date if provided

        # Save underpay preprocessed
        under_dir = LISTS_DIR / "underpays_wd" / date_str
        under_dir.mkdir(parents=True, exist_ok=True)
        under_path = under_dir / 'preprocessed_underpays_withdrawals.xlsx'
        underpay_df.to_excel(under_path, index=False)
        print(f"✅ Preprocessed underpay withdrawals saved to {under_path}")

        # Save overpay preprocessed
        over_dir = LISTS_DIR / "overpays_wd" / date_str
        over_dir.mkdir(parents=True, exist_ok=True)
        over_path = over_dir / 'preprocessed_overpay_withdrawals.xlsx'
        overpay_df.to_excel(over_path, index=False)
        print(f"✅ Preprocessed overpay withdrawals saved to {over_path}")

    # Combine and save combined versions, passing target_cols for order
    combined_underpay = combine_tracker_df(underpay_df, date_str, 'underpays_withdrawals', target_cols)
    combined_overpay = combine_tracker_df(overpay_df, date_str, 'overpay_withdrawals', target_cols)

    return combined_underpay, combined_overpay


def preprocess_all_trackers(save_clean=True):
    """
    Preprocess all tracker files in parallel using multithreading.
    - Uses DATE for filenames and sync.
    - Times the execution and prints the duration.
    - Returns dict of results for each tracker (combined DFs).
    """
    start_time = time.time()

    results = {}

    def process_unapproved():
        raw_dep_file = RAW_TRACKING_DIR / f'unapproved_dep_{DATE}.xlsx'
        dep_df = preprocess_unapproved_deposits(raw_dep_file, save_clean=save_clean, custom_date=DATE)
        results['unapproved_deposits'] = dep_df

    def process_unexecuted():
        raw_wd_file = RAW_TRACKING_DIR / f'unexecuted_wd_{DATE}.xlsx'
        underpay_df, overpay_df = preprocess_unexecuted_withdrawals(raw_wd_file, save_clean=save_clean, custom_date=DATE)
        results['underpay_withdrawals'] = underpay_df
        results['overpay_withdrawals'] = overpay_df

    with ThreadPoolExecutor(max_workers=2) as executor:  # Adjust max_workers as needed
        futures = [
            executor.submit(process_unapproved),
            executor.submit(process_unexecuted)
        ]
        for future in as_completed(futures):
            future.result()  # Raise any exceptions

    end_time = time.time()
    duration = end_time - start_time
    print(f"Preprocessing all trackers took {duration:.2f} seconds")

    return results


# Example usage (can be called from main.py later)
if __name__ == "__main__":
    # Run all preprocessing in parallel
    all_results = preprocess_all_trackers()
    print("Unapproved Deposits DF head:", all_results['unapproved_deposits'].head())
    print("Underpay Withdrawals DF head:", all_results['underpay_withdrawals'].head())
    print("Overpay Withdrawals DF head:", all_results['overpay_withdrawals'].head())