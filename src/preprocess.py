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
    df.columns = df.columns.str.strip()

    if processor == "paypal":
        keep_cols = [
            "Date", "Time", "Time zone", "Name", "Type", "Status", "Currency",
            "Gross", "Fee", "Net", "From Email Address", "To Email Address", "Transaction ID"
        ]
        df = df[keep_cols]
        allowed_types = ["Express Checkout Payment", "Mass Payment", "Payment Refund"]
        df = df[(df["Status"] == "Completed") & (df["Type"].isin(allowed_types)) & (df["Currency"] != "GBP")]
        df = df.rename(columns={"Transaction ID": "transaction_id", "Gross": "amount", "Date": "date"})

    elif processor == "safecharge":
        df = df[(df["Transaction Type"].str.lower() == "sale") & (df["Transaction Result"].str.lower() == "approved")]
        df = df[["Transaction ID", "Date", "Amount", "Currency", "Transaction Type", "Transaction Result"]]
        df = df.rename(columns={"Transaction ID": "transaction_id", "Date": "date", "Amount": "amount", "Currency": "currency"})

    elif processor == "powercash":
        df = df[(df["Tx-Type"].str.lower().isin(["capture", "aft"])) & (df["Status"].str.lower() == "successful") & (df["Currency"].str.upper() != "CAD")]
        df = df[["Tx-Id", "Tx-Type", "Date", "Time", "Currency", "Amount", "Status", "Firstname", "Lastname", "EMail", "Custom 3", "Credit Card Brand", "Credit Card Number"]]
        df = df.rename(columns={"Tx-Id": "transaction_id", "Amount": "amount", "Date": "date"})

    elif processor == "shift4":
        df = df[(df["Operation Type"].str.lower() == "sale") & (df["Response"].str.lower() == "completed successfully")]
        df = df[["Transaction Date", "Request ID (a1)", "Currency", "Amount", "Card Number", "Card Scheme", "Cardholder Email"]]
        df = df.rename(columns={"Transaction Date": "date", "Request ID (a1)": "transaction_id"})

    elif processor in ["skrill", "netteller"]:
        df = df.rename(columns={
            "Time (CET)": "date", "Time (UTC)": "date",
            "ID of the corresponding Skrill transaction": "transaction_id",
            "ID of the corresponding Neteller transaction": "transaction_id",
            "[+]": "amount", "Currency Sent": "currency"
        })
        df = df[(df["Type"].str.lower() == "receive money") & (df["Status"].str.lower() == "processed") & df["amount"].notna()]
        df = df[~df["Transaction Details"].str.contains("fee", case=False, na=False)]
        df = df[["date", "transaction_id", "amount", "currency", "Transaction Details", "Reference"]]

    elif processor == "trustpayments":
        df = df[(df["errorcode"] == 0) & (df["requesttypedescription"].str.upper() == "AUTH")]
        df = df.rename(columns={
            "transactionreference": "transaction_id",
            "transactionstartedtimestamp": "date",
            "mainamount": "amount",
            "currencyiso3a": "currency"
        })
        df = df[["transaction_id", "billingfullname", "paymenttypedescription", "date", "currency", "amount", "maskedpan", "orderreference"]]

    elif processor == "zotapay":
        df = df.copy()
        df.columns = df.iloc[0].str.strip()
        df = df.iloc[1:]
        df = df[(df["Type"].str.upper() == "SALE") & (df["Status"].str.lower() == "approved")]
        df = df.rename(columns={
            "ID": "transaction_id",
            "Order Currency": "currency",
            "Order Amount": "amount",
            "Created At": "date"
        })
        keep_cols = [
            "transaction_id", "Type", "Status", "currency", "amount", "Merchant Order Description",
            "Payment Method", "date", "Ended At", "Customer Email", "Customer First Name", "Customer Last Name"
        ]
        df = df[keep_cols]

    else:
        raise ValueError(f"Processor not supported yet: {processor}")

    return df.reset_index(drop=True)


# ----------------------------
# CRM Handling
# ----------------------------
def load_crm_file(filepath: str, processor_name: str, save_clean=False) -> pd.DataFrame:
    df = pd.read_excel(filepath, engine="openpyxl")
    df.columns = df.columns.str.strip()
    df["PSP name"] = df["PSP name"].str.strip().str.lower()
    normalized_processor = processor_name.lower()

    def extract_crm_transaction_id(comment: str, processor: str):
        text = str(comment)
        processor = processor.lower()
        patterns = {
            "paypal": r"PSP TransactionId:([A-Z0-9]+)",
            "safecharge": r"PSP TransactionId:([12]\d{18})|More Comment:[^$]*\$(\d{19})",
            "powercash": r"PSP TransactionId:(\d+)",
            "shift4": r"More Comment:[^$]*\$(\w+)",
            "skrill": r"More Comment:[^$]*\$(\d+)",
            "netteller": r"More Comment:[^$]*\$(\d+)",
            "trustpayments": r"PSP TransactionId:([\d\-]+)|More Comment:[^$]*\$(\d{2}-\d{2}-\d+)",
            "zotapay": r"PSP TransactionId:(\d+)"
        }
        pattern = patterns.get(processor)
        if not pattern:
            return None
        match = re.search(pattern, text)
        if match:
            return next((g for g in match.groups() if g), None)
        return None

    df["transaction_id"] = df["Internal Comment"].apply(lambda c: extract_crm_transaction_id(c, processor_name))

    if normalized_processor == "netteller":
        psp_mask = df["PSP name"].isin(["netteller", "neteller"])
    elif normalized_processor == "trustpayments":
        psp_mask = df["PSP name"] == "acquiringcom"
    elif normalized_processor == "zotapay":
        psp_mask = df["PSP name"].isin(["zotapay"])
    else:
        psp_mask = df["PSP name"] == normalized_processor

    df = df[(df["Name"].str.lower() == "deposit") & psp_mask]
    df = df.reset_index(drop=True)

    if save_clean:
        date_str = extract_date_from_filename(filepath)
        out_path = PROCESSED_CRM_DIR / normalized_processor / date_str / f"{normalized_processor}_deposits.xlsx"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_excel(out_path, index=False)
        print(f"Saved cleaned CRM {processor_name} deposits to {out_path}")

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


# ----------------------------
# Processor File Loader
# ----------------------------
def load_processor_file(filepath: str, processor_name: str, save_clean=False) -> pd.DataFrame:
    ext = Path(filepath).suffix.lower()
    dtype = {
        "Transaction ID": str,
        "Tx-Id": str,
        "Request ID (a1)": str,
        "ID of the corresponding Skrill transaction": str,
        "ID of the corresponding Neteller transaction": str
    }
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
        out_path = PROCESSED_PROCESSOR_DIR / processor_name / date_str / f"{processor_name}_deposits.xlsx"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df_clean.to_excel(out_path, index=False)
        print(f"✅ Saved cleaned {processor_name} deposits to {out_path}")

    return df_clean
