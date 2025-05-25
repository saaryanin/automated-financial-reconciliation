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

    elif processor == "safecharge":
        df = df[
            (df["Transaction Type"].str.lower() == "sale") &
            (df["Transaction Result"].str.lower() == "approved")
        ]

        df = df.rename(columns={
            "Transaction ID": "transaction_id",
            "Transaction Date": "date",
            "Amount": "amount",
            "Currency": "currency"
        })

    elif processor == "powercash":
        df = df[
            (df["Tx-Type"].str.lower().isin(["capture", "aft"])) &
            (df["Status"].str.lower() == "successful") &
            (df["Currency"].str.upper() != "CAD")
        ]

        keep_cols = [
            "Tx-Id", "Tx-Type", "Date", "Time", "Currency", "Amount", "Status",
            "Firstname", "Lastname", "EMail", "Custom 3",
            "Credit Card Brand", "Credit Card Number"
        ]
        df = df[keep_cols]

        df = df.rename(columns={
            "Tx-Id": "transaction_id",
            "Amount": "amount",
            "Date": "date"
        })

    elif processor == "shift4":
        df = df[
            (df["Operation Type"].str.lower() == "sale") &
            (df["Response"].str.lower() == "completed successfully")
        ]

        keep_cols = [
            "Transaction Date", "Request ID (a1)", "Currency", "Amount", "Card Number",
            "Card Scheme", "Cardholder Email"
        ]
        df = df[keep_cols]

        df = df.rename(columns={
            "Transaction Date": "date",
            "Request ID (a1)": "transaction_id"
        })

    else:
        raise ValueError(f"Processor not supported yet: {processor}")

    return df.reset_index(drop=True)



def load_processor_file(filepath: str, processor_name: str, save_clean=False) -> pd.DataFrame:
    ext = Path(filepath).suffix.lower()
    dtype = {"Transaction ID": str, "Tx-Id": str, "Request ID (a1)": str}
    skip = 11 if processor_name.lower() == "safecharge" else 0

    if ext == ".csv":
        df = pd.read_csv(filepath, dtype=dtype, encoding="utf-8-sig")
    elif ext == ".xlsx":
        df = pd.read_excel(filepath, dtype=dtype, skiprows=skip, engine="openpyxl")
    else:
        raise ValueError("Unsupported file type")

    df.columns = df.columns.str.strip()
    df_clean = standardize_processor_columns(df, processor_name)

    if save_clean:
        date_str = extract_date_from_filename(filepath)
        out_path = (
            PROCESSED_PROCESSOR_DIR / processor_name / date_str / f"{processor_name}_deposits.xlsx"
        )
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df_clean.to_excel(out_path, index=False)
        print(f"✅ Saved cleaned {processor_name} deposits to {out_path}")

    return df_clean


# ----------------------------
# CRM Handling
# ----------------------------
def load_crm_file(filepath: str, processor_name: str, save_clean=False) -> pd.DataFrame:
    df = pd.read_excel(filepath, engine="openpyxl")

    def extract_crm_transaction_id(comment: str, processor: str):
        if processor.lower() == "paypal":
            match = re.search(r"PSP TransactionId:([A-Z0-9]+)", str(comment))
            return match.group(1) if match else None

        elif processor.lower() == "safecharge":
            matches = re.findall(r"\b[12]\d{18}\b", str(comment))
            return matches[0] if matches else None

        elif processor.lower() == "powercash":
            match = re.search(r"PSP TransactionId:(\d+)", str(comment))
            return match.group(1) if match else None

        elif processor.lower() == "shift4":
            match = re.search(r"More Comment:[^$]*\$([a-f0-9]{32})", str(comment))
            return match.group(1) if match else None

        else:
            return None

    df["transaction_id"] = df["Internal Comment"].apply(lambda c: extract_crm_transaction_id(c, processor_name))

    df = df[
        (df["Name"].str.lower() == "deposit") &
        (df["PSP name"].str.lower() == processor_name.lower())
    ]

    df = df.reset_index(drop=True)

    if save_clean:
        date_str = extract_date_from_filename(filepath)
        out_path = (
                PROCESSED_CRM_DIR / processor_name.lower() / date_str / f"{processor_name.lower()}_deposits.xlsx"
        )
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_excel(out_path, index=False)
        print(f"Saved cleaned CRM {processor_name} deposits to {out_path}")
    if processor_name is None:
        raise ValueError("CRM loader requires a processor_name (e.g., 'paypal', 'safecharge')")

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
                futures.append(executor.submit(load_crm_file, str(path), processor_name, save_clean))
            else:
                futures.append(executor.submit(load_processor_file, str(path), processor_name, save_clean))
        results = [f.result() for f in futures]
    return results
