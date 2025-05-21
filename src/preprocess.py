import pandas as pd
import re
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from src.config import PROCESSED_CRM_DIR, PROCESSED_PROCESSOR_DIR


# ----------------------------
# Processor Handling
# ----------------------------
def standardize_processor_columns(df: pd.DataFrame, processor: str) -> pd.DataFrame:
    processor = processor.lower()

    if processor == "paypal":
        keep_cols = [
            "Date", "Time", "Time zone", "Name", "Type", "Status", "Currency",
            "Gross", "Fee", "Net", "From Email Address", "To Email Address", "Transaction ID"
        ]
        df = df[keep_cols]

        allowed_types = ["Express Checkout Payment", "Mass Payment", "Payment Refund"]
        df = df[
            (df["Status"] == "Completed") &
            (df["Type"].isin(allowed_types)) &
            (df["Currency"] != "GBP")
        ]

        df = df.rename(columns={
            "Transaction ID": "transaction_id",
            "Gross": "amount",
            "Date": "date"
        })

    else:
        raise ValueError(f"Processor not supported yet: {processor}")

    return df.reset_index(drop=True)


def load_processor_file(filepath: str, processor_name: str, save_clean=False) -> pd.DataFrame:
    ext = Path(filepath).suffix.lower()
    dtype = {"Transaction ID": str}

    if ext == ".csv":
        df = pd.read_csv(filepath, dtype=dtype, encoding="utf-8-sig")
    elif ext == ".xlsx":
        df = pd.read_excel(filepath, dtype=dtype, engine="openpyxl")
    else:
        raise ValueError("Unsupported file type")

    df_clean = standardize_processor_columns(df, processor_name)

    if save_clean:
        date_str = extract_date_from_filename(filepath)
        out_path = (
            PROCESSED_PROCESSOR_DIR / processor_name / date_str / f"{processor_name}_deposits.xlsx"
        )
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df_clean.to_excel(out_path, index=False)
        print(f"Saved cleaned {processor_name} deposits to {out_path}")

    return df_clean


# ----------------------------
# CRM Handling
# ----------------------------
def load_crm_file(filepath: str, save_clean=False) -> pd.DataFrame:
    df = pd.read_excel(filepath, engine="openpyxl")

    def extract_txn_id(comment):
        match = re.search(r"PSP TransactionId:([A-Z0-9]+)", str(comment))
        return match.group(1) if match else None

    df["transaction_id"] = df["Internal Comment"].apply(extract_txn_id)

    df = df[
        (df["Name"].str.lower() == "deposit") &
        (df["PSP name"].str.lower() == "paypal")
    ]

    df = df.reset_index(drop=True)

    if save_clean:
        date_str = extract_date_from_filename(filepath)
        out_path = (
            PROCESSED_CRM_DIR / "paypal" / date_str / f"paypal_deposits_{date_str}.xlsx"
        )
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_excel(out_path, index=False)
        print(f"Saved cleaned CRM PayPal deposits to {out_path}")

    return df


# ----------------------------
# Utility
# ----------------------------
def extract_date_from_filename(filepath: str) -> str:
    match = re.search(r"(\d{4}-\d{2}-\d{2})", filepath)
    if match:
        return match.group(1)
    match_alt = re.search(r"(\d{2}\.\d{2}\.\d{4})", filepath)
    if match_alt:
        return datetime.strptime(match_alt.group(1), "%d.%m.%Y").strftime("%Y-%m-%d")
    match_slash = re.search(r"(\d{2}_\d{2}_\d{4})", filepath)
    if match_slash:
        return datetime.strptime(match_slash.group(1), "%d_%m_%Y").strftime("%Y-%m-%d")
    return "unknown_date"


# ----------------------------
# Parallel Batch Processor
# ----------------------------
def process_files_in_parallel(file_paths, processor_name=None, is_crm=False, save_clean=True):
    with ThreadPoolExecutor() as executor:
        futures = []
        for path in file_paths:
            if is_crm:
                futures.append(executor.submit(load_crm_file, str(path), save_clean))
            else:
                futures.append(executor.submit(load_processor_file, str(path), processor_name, save_clean))
        results = [f.result() for f in futures]
    return results
