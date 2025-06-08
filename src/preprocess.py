import pandas as pd
import re
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from src.config import PROCESSED_CRM_DIR, PROCESSED_PROCESSOR_DIR

# ----------------------------
# Processor Handling
# ----------------------------
def standardize_processor_columns_deposits(df: pd.DataFrame, processor: str) -> pd.DataFrame:
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
        df = df.rename(
            columns={"Transaction ID": "transaction_id", "Date": "date", "Amount": "amount", "Currency": "currency"})

    elif processor == "powercash":
        df = df[(df["Tx-Type"].str.lower().isin(["capture", "aft"])) & (df["Status"].str.lower() == "successful") & (df["Currency"].str.upper() != "CAD")]
        df = df[["Tx-Id", "Tx-Type", "Date", "Time", "Currency", "Amount", "Status", "Firstname", "Lastname", "EMail", "Custom 3", "Credit Card Brand", "Credit Card Number"]]
        df = df.rename(columns={"Tx-Id": "transaction_id", "Amount": "amount", "Date": "date"})

    elif processor == "shift4":
        df = df[(df["Operation Type"].str.lower() == "sale") & (df["Response"].str.lower() == "completed successfully")]
        df = df[["Transaction Date", "Request ID (a1)", "Currency", "Amount", "Card Number", "Card Scheme", "Cardholder Email"]]
        df = df.rename(columns={"Transaction Date": "date", "Request ID (a1)": "transaction_id"})

    elif processor in ["skrill", "neteller"]:
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

    elif processor == "bitpay":
        df = df[df["txtype"].str.lower() == "sale"]
        df = df.rename(columns={
            "invoiceid": "transaction_id"
        })
        keep_cols = [
            "date", "time", "transaction_id", "payoutamount",
            "invoiceprice", "buyerName", "buyerEmail"
        ]
        df = df[keep_cols]

    elif processor == "ezeebill":
        df.columns = df.columns.str.replace(" ", "").str.strip()
        df = df[df["Action"].str.upper() == "SALE"]
        if df.empty:
            return None
        df = df.rename(columns={
            "MerchantTxnID": "transaction_id",
            "OriginalAmount": "amount"
        })
        df = df[["transaction_id", "amount"]]  # Time dropped
    elif processor == "paymentasia":
        df = df[(df["Type"].str.upper() == "SALE") & (df["Status"].str.upper() == "SUCCESS")]
        df = df.rename(columns={
            "Merchant Reference": "transaction_id",
            "Order Amount": "amount",
            "Order Currency": "currency",
            "Created Time": "date"
        })
        df = df[["transaction_id", "amount", "currency", "date"]]

        return df.reset_index(drop=True)

def standardize_processor_columns_withdrawals(df: pd.DataFrame, processor: str) -> pd.DataFrame:
    if processor.lower() == "paypal":
        df.columns = df.columns.str.strip()

        # Filter by Type and Status
        allowed_types = ["Mass Payment", "Payment Refund"]
        allowed_status = ["Completed", "Unclaimed"]
        df = df[
            df["Type"].isin(allowed_types) &
            df["Status"].isin(allowed_status) &
            (df["Currency"] != "GBP")
            ]
        if df.empty:
            print(f"No PayPal Withdrawals found after filtering.")
            return pd.DataFrame()

        # Rename for consistency
        df = df.rename(columns={
            "Date": "date",
            "Gross": "amount",
            "Currency": "currency",
            "To Email Address": "email"
        })
        # Remove comma separators in amount (e.g., "3,000.00" -> "3000.00")
        df["amount"] = df["amount"].astype(str).str.replace(",", "", regex=False)

        df["last_4cc"] = ""  # PayPal doesn't provide card digits
        df["processor_name"] = "paypal"

        # Handle names
        if "Name" in df.columns and not df["Name"].isna().all():
            name_split = df["Name"].astype(str).str.strip().str.split(n=1, expand=True)
            if isinstance(name_split, pd.DataFrame) and name_split.shape[1] >= 1:
                df["first_name"] = name_split[0]
                df["last_name"] = name_split[1] if name_split.shape[1] > 1 else ""
            else:
                df["first_name"] = ""
                df["last_name"] = ""
        else:
            df["first_name"] = ""
            df["last_name"] = ""

        # Final column order
        df = df[[
            "amount", "currency", "date", "last_4cc",
            "email", "first_name", "last_name", "processor_name"
        ]]
        return df


    elif processor.lower() == "safecharge":
        # Check if Safecharge-specific pattern exists
        processor_name_value = "safecharge"
        if "Acquiring Bank" in df.columns and df["Acquiring Bank"].astype(str).str.contains("Nuvei", case=False).any():
            processor_name_value = "safecharge"

        df = df[(df["Transaction Type"].isin(["Credit", "Voidcheque"])) & (df["Transaction Result"] == "Approved")]
        cancel_indexes = df[df["Transaction Type"] == "Voidcheque"].index
        remove_indexes = set(cancel_indexes) | set(cancel_indexes - 1)
        df = df.drop(remove_indexes, errors='ignore')
        df = df.rename(columns={
            "Amount": "amount",
            "Currency": "currency",
            "Date": "date",
            "Email Address": "email",
            "PAN": "last_4cc"
        })
        df["last_4cc"] = df["last_4cc"].astype(str).str.extract(r"(\d{4})$")
        df["currency"] = df["currency"].replace({
            "Euro": "EUR",
            "US Dollar": "USD"
        })
        df["processor_name"] = processor_name_value
        df = df[["amount", "currency", "date", "last_4cc", "email", "processor_name"]]
        return df

        cancel_indexes = df[df["Transaction Type"] == "Voidcheque"].index
        remove_indexes = set(cancel_indexes) | set(cancel_indexes - 1)
        df = df.drop(remove_indexes, errors='ignore')
        df = df.rename(columns={
            "Amount": "amount",
            "Currency": "currency",
            "Date": "date",
            "Email Address": "email",
            "PAN": "last_4cc"
        })
        df["last_4cc"] = df["last_4cc"].astype(str).str.extract(r"(\d{4})$")
        df = df[["amount", "currency", "date", "last_4cc", "email"]]
        df["currency"] = df["currency"].replace({
            "Euro": "EUR",
            "US Dollar": "USD"
        })
        return df

    elif processor.lower() == "powercash":
        # — filter to only refunds or CFTs, successful EUR/USD rows
        df.columns = df.columns.str.strip()
        df = df[
            df["Tx-Type"].str.lower().isin(["refund", "cft"]) &
            (df["Status"].str.lower() == "successful") &
            (df["Currency"].str.upper().isin(["EUR", "USD"]))
        ]
        if df.empty:
            print("No PowerCash withdrawals found after filtering.")
            return pd.DataFrame()

        # — rename to the common schema
        df = df.rename(columns={
            "Date": "date",
            "Amount": "amount",
            "Currency": "currency",
            "EMail": "email",
        })

        # — last four of card
        df["last_4cc"] = (
            df["Credit Card Number"]
              .astype(str).str[-4:]
        )

        # — names
        df["first_name"] = df.get("Firstname", "").astype(str)
        df["last_name"]  = df.get("Lastname",  "").astype(str)

        # — tag processor
        df["processor_name"] = "powercash"

        # — enforce same column order as PayPal/SafeCharge
        df = df[[
            "amount", "currency", "date",
            "last_4cc", "email",
            "first_name", "last_name",
            "processor_name"
        ]]

        return df
    elif processor.lower() == "shift4":
        # 1) strip headers & filter to only Referral Credit + Completed Successfully
        df.columns = df.columns.str.strip()
        df = df[
            (df["Operation Type"].str.lower() == "referral credit") &
            (df["Response"].str.lower() == "completed successfully")
        ]
        if df.empty:
            print("No Shift4 withdrawals found after filtering.")
            return pd.DataFrame()

        # 2) select only the needed columns
        df = df[[
            "Transaction Date",
            "Card Number",
            "Currency",
            "Amount",
            "Cardholder Name",
            "Cardholder Email"
        ]]

        # 3) rename into our unified schema
        df = df.rename(columns={
            "Transaction Date": "date",
            "Currency": "currency",
            "Amount": "amount",
            "Cardholder Email": "email"
        })

        # 4) extract last-4 digits from the card number
        df["last_4cc"] = df["Card Number"].astype(str).str.extract(r"(\d{4})$").fillna("")

        # 5) split the masked Cardholder Name (e.g. "Lui* Lor***")
        name_split = df["Cardholder Name"].astype(str).str.split(n=1, expand=True)
        df["first_name"] = name_split[0].str.rstrip("*")
        df["last_name"] = (
            name_split[1].str.rstrip("*")
            if name_split.shape[1] > 1 else ""
        )

        # 6) tag the processor and reorder
        df["processor_name"] = "shift4"
        return df[[
            "amount",
            "currency",
            "date",
            "last_4cc",
            "email",
            "first_name",
            "last_name",
            "processor_name"
        ]]


    return pd.DataFrame()



# ----------------------------
# CRM Handling
# ----------------------------
def load_crm_file(filepath: str, processor_name: str, save_clean=False, transaction_type="deposit") -> pd.DataFrame:
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
            "neteller": r"More Comment:[^$]*\$(\d+)",
            "trustpayments": r"PSP TransactionId:([\d\-]+)|More Comment:[^$]*\$(\d{2}-\d{2}-\d+)",
            "zotapay": r"PSP TransactionId:(\d+)",
            "bitpay": r"PSP TransactionId:([A-Za-z0-9]+)",
            "ezeebill": r"(\d{7}-\d{18})",
            "paymentasia": r"(\d{7}-\d{18})",
        }
        pattern = patterns.get(processor)
        if not pattern:
            return None
        match = re.search(pattern, text)
        if match:
            return next((g for g in match.groups() if g), None)
        return None

    df["transaction_id"] = df["Internal Comment"].apply(lambda c: extract_crm_transaction_id(c, processor_name))

    if normalized_processor == "neteller":
        psp_mask = df["PSP name"].isin(["neteller"])
    elif normalized_processor == "trustpayments":
        psp_mask = df["PSP name"] == "acquiringcom"
    elif normalized_processor == "zotapay":
        psp_mask = df["PSP name"] == "zotapay"
    elif normalized_processor == "paymentasia":
        psp_mask = df["PSP name"] == "pamy"

    else:
        psp_mask = df["PSP name"] == normalized_processor

    tx_type = "deposit" if transaction_type == "deposit" else "withdrawal"
    df = df[(df["Name"].str.lower() == tx_type) & psp_mask].reset_index(drop=True)

    if "Currency" in df.columns:
        df["Currency"] = df["Currency"].replace({
            "Euro": "EUR",
            "US Dollar": "USD"
        })

    if save_clean:
        date_str = extract_date_from_filename(filepath)
        folder = f"{normalized_processor}_{transaction_type}s.xlsx"
        out_path = PROCESSED_CRM_DIR / normalized_processor / date_str / folder
        out_path.parent.mkdir(parents=True, exist_ok=True)

        if transaction_type == "withdrawal" and "transaction_id" in df.columns:
            needed_columns = [
                "Created On",
                "First Name (Account) (Account)",
                "Last Name (Account) (Account)",
                "Email (Account) (Account)",
                "Amount",
                "Currency",
                "Method of Payment",
                "PSP name",
                "CC Last 4 Digits"
            ]
            if "transaction_id" in df.columns:
                df = df.drop(columns=["transaction_id"])
            df = df[[col for col in needed_columns if col in df.columns]]

        df.to_excel(out_path, index=False)
        print(f"✅ Saved cleaned CRM {processor_name} {transaction_type}s to {out_path}")

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
def process_files_in_parallel(file_paths, processor_name=None, is_crm=False, save_clean=True,transaction_type="deposit"):
    with ThreadPoolExecutor() as executor:
        futures = []
        for path in file_paths:
            if is_crm:
                futures.append(executor.submit(load_crm_file, str(path), processor_name, save_clean, transaction_type))
            else:
                futures.append(executor.submit(load_processor_file, str(path), processor_name, save_clean, transaction_type=transaction_type))
        results = [f.result() for f in futures]
    return results


# ----------------------------
# Processor File Loader
# ----------------------------
def load_processor_file(filepath: str, processor_name: str, save_clean=False, transaction_type="deposit") -> pd.DataFrame:
    ext = Path(filepath).suffix.lower()
    dtype = {
        "Transaction ID": str,
        "Tx-Id": str,
        "Request ID (a1)": str,
        "ID of the corresponding Skrill transaction": str,
        "ID of the corresponding Neteller transaction": str
    }
    skip = 15 if processor_name.lower() == "ezeebill" else 11 if processor_name.lower() == "safecharge" else 0

    if ext == ".csv":
        df = pd.read_csv(filepath, dtype=dtype, encoding="utf-8-sig", skiprows=skip)
    elif ext == ".xlsx":
        df = pd.read_excel(filepath, dtype=dtype, skiprows=skip, engine="openpyxl")
    else:
        raise ValueError("Unsupported file type")

    if transaction_type == "deposit":
        df_clean = standardize_processor_columns_deposits(df, processor_name)
    else:
        df_clean = standardize_processor_columns_withdrawals(df, processor_name)

    if df_clean is None or df_clean.empty:
        return None

    if save_clean:
        date_str = extract_date_from_filename(filepath)
        out_filename = f"{processor_name}_{transaction_type}s.xlsx"
        out_path = PROCESSED_PROCESSOR_DIR / processor_name / date_str / out_filename
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df_clean.to_excel(out_path, index=False)
        print(f"✅ Saved cleaned {processor_name} {transaction_type}s to {out_path}")

    return df_clean

