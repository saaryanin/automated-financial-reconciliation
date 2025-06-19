import pandas as pd
import re
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from src.config import PROCESSED_CRM_DIR, PROCESSED_PROCESSOR_DIR
import logging


def clean_amount(val):
    """
    Convert accounting-style amounts like '(100.00)' to -100.00,
    and plain '100.00' or '-100.00' to numbers.
    """
    s = str(val).replace(',', '').strip()
    # Parentheses denote negative numbers
    if re.match(r'^\(\s*-?[\d,\.]+\s*\)$', s):
        s = s.strip('()')
        try:
            return -float(s)
        except Exception:
            return None
    try:
        return float(s)
    except Exception:
        return None
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


def patch_standardize_zotapay_paymentasia_withdrawals(df, processor):
    import pandas as pd
    import re

    processor_tag = processor.lower()
    df.columns = df.columns.str.strip().str.replace(u'\xa0', ' ', regex=False)

    # Handle Zotapay
    if processor.lower() == "zotapay":
        print("✅ Entered Zotapay block")

        if df.columns[0].lower() not in ["type", "status", "order amount"]:
            df.columns = df.iloc[0].astype(str).str.strip()
            df = df.iloc[1:].copy()

        if "Type" not in df.columns or "Status" not in df.columns:
            print("❌ Zotapay: required columns missing.")
            return pd.DataFrame()

        df = df[(df["Type"].astype(str).str.upper() == "PAYOUT") &
                (df["Status"].astype(str).str.upper() == "APPROVED")]

        df = df.rename(columns={
            "Order Amount": "amount",
            "Order Currency": "currency",
            "Ended At": "date",
            "Customer Email": "email",
            "Customer Bank Account Name": "full_name",
            "Merchant Order ID": "tp"
        })

        # Robust TP extraction
        df["tp"] = df["tp"].astype(str).apply(lambda x: re.search(r'\d{7,8}', x).group(0)
        if re.search(r'\d{7,8}', x) else "")

        # Standardize date format
        df["date"] = pd.to_datetime(df["date"], errors='coerce', utc=True)
        df["date"] = df["date"].dt.strftime("%Y-%m-%d %H:%M:%S")

        df["full_name"] = df["full_name"].astype(str).str.strip()
        df["first_name"] = df["full_name"].str[:2]
        df["last_name"] = df["full_name"].str[2:]
        df["email"] = df["email"].fillna("")

    # Handle PaymentAsia
    elif processor.lower() == "paymentasia":
        print("✅ Entered PaymentAsia block")

        required_cols = ["Status", "Order Amount", "Completed Time", "Beneficiary Name"]
        normalized_cols = [col.strip().replace(u'\xa0', ' ') for col in df.columns]
        missing_cols = [col for col in required_cols if col not in normalized_cols]

        if missing_cols:
            print(f"❌ PaymentAsia: missing required columns: {missing_cols}")
            return pd.DataFrame()

        df = df[df["Status"].astype(str).str.upper() == "SUCCESS"]

        df = df.rename(columns={
            "Order Amount": "amount",
            "Order Currency": "currency",
            "Completed Time": "date",
            "Beneficiary Name": "full_name",
            "Request Reference": "tp"
        })

        # Robust TP extraction
        df["tp"] = df["tp"].astype(str).apply(lambda x: re.search(r'\d{7,8}', x).group(0)
        if re.search(r'\d{7,8}', x) else "")

        # Standardize date format
        df["date"] = pd.to_datetime(df["date"], errors='coerce', infer_datetime_format=True)
        df["date"] = df["date"].dt.strftime("%Y-%m-%d %H:%M:%S")

        df["full_name"] = df["full_name"].astype(str).str.strip()
        full_split = df["full_name"].str.split(n=2, expand=True)
        df["first_name"] = full_split[0].fillna("") + " " + full_split[1].fillna("")
        df["last_name"] = full_split[2].fillna("")
        df["email"] = ""

    # Final clean-up
    df["amount"] = pd.to_numeric(
        df["amount"].astype(str).str.replace(",", "", regex=False),
        errors='coerce'
    )
    df["last_4cc"] = ""
    df["processor_name"] = processor_tag

    return df[[
        "amount", "currency", "date", "last_4cc",
        "email", "first_name", "last_name", "processor_name", "tp"
    ]]


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

        # --- FIX: Make sure amount is numeric even if in (100.00) form
        df["amount"] = df["amount"].apply(clean_amount)

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

    elif processor.lower() in ("skrill", "neteller"):
        df.columns = df.columns.str.strip()

        # both Skrill and Neteller withdrawals are "send money" + processed
        df = df.loc[
             (df["Type"].str.lower() == "send money") &
             (df["Status"].str.lower() == "processed"),
             :
             ]
        if df.empty:
            print(f"No {processor} withdrawals found after filtering.")
            return pd.DataFrame()

        # pick whichever column holds the amount
        amt_col = "Amount Sent" if "Amount Sent" in df.columns else "[+]"
        df = df.loc[
             df[amt_col].notna() & df[amt_col].astype(str).str.strip().ne(""),
             :
             ]
        if df.empty:
            print(f"No {processor} withdrawals with amount found after filtering.")
            return pd.DataFrame()

        # pull the numeric TP out of Reference
        df["tp"] = (
            df["Reference"]
            .astype(str)
            .str.extract(r"(\d+)")
            .fillna("")
        )

        # normalize date into a single "date" column
        if "Time (CET)" in df.columns:
            df["date"] = pd.to_datetime(df["Time (CET)"])
        elif "Time (UTC)" in df.columns:
            df["date"] = pd.to_datetime(df["Time (UTC)"])
        elif "Date" in df.columns:
            df["date"] = pd.to_datetime(df["Date"])
        else:
            raise ValueError(f"Could not find a date column for {processor}")

        # rename amount & currency to your unified names
        df = df.rename(columns={
            amt_col: "amount",
            "Currency Sent": "currency"
        })

        # strip the leading "to " off the Transaction Details to get email
        df["email"] = (
            df["Transaction Details"]
            .astype(str)
            .str.replace(r"^\s*to\s*", "", regex=True)
            .str.strip()
        )

        # fill out the rest
        df["last_4cc"] = ""
        df["first_name"] = ""
        df["last_name"] = ""
        df["processor_name"] = processor.lower()

        # return exactly the columns you want
        return df[[
            "amount", "currency", "date", "last_4cc",
            "email", "first_name", "last_name",
            "processor_name", "tp"
        ]]
    elif processor == "bitpay":
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
        df = df[df["tx_type"].str.lower() == "invoice refund"]
        if df.empty:
            print("No BitPay withdrawals found after filtering.")
            return pd.DataFrame()

        df = df.rename(columns={
            "invoice_id": "transaction_id",
            "payout_amount": "amount",
            "payout_currency": "currency",
            "buyername": "full_name",
            "buyeremail": "email"
        })

        # Split buyer full name into first and last names
        name_split = df["full_name"].astype(str).str.strip().str.split(n=1, expand=True)
        df["first_name"] = name_split[0]
        df["last_name"] = name_split[1] if name_split.shape[1] > 1 else ""

        df["last_4cc"] = ""  # No card digits in BitPay
        df["processor_name"] = "bitpay"

        df = df[[
            "amount", "currency", "date", "last_4cc",
            "email", "first_name", "last_name", "processor_name"
        ]]
        return df




    elif processor.lower() in ["zotapay", "paymentasia", "zotapay_paymentasia"]:
        return patch_standardize_zotapay_paymentasia_withdrawals(df, processor)

    return pd.DataFrame()



# ----------------------------
# CRM Handling
# ----------------------------
def load_crm_file(filepath: str, processor_name: str, save_clean=False, transaction_type="deposit") -> pd.DataFrame:
    df = pd.read_excel(filepath, engine="openpyxl")
    df.columns = df.columns.str.strip()
    df["PSP name"] = df["PSP name"].str.strip().str.lower()
    df["tp"] = df["TP Account"] if "TP Account" in df.columns else ""
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

    if normalized_processor in ["zotapay", "paymentasia"]:
        name_has_chinese = (
                df["First Name (Account) (Account)"].astype(str).str.contains(r'[\u4e00-\u9fff]') |
                df["Last Name (Account) (Account)"].astype(str).str.contains(r'[\u4e00-\u9fff]')
        )

        # Filter by known indicators
        name_col_match = df["Name"].str.lower() == "withdrawal"
        psp_match = df["PSP name"].str.contains("pamy|zotapay|wire withdrawal", case=False, na=False)
        method_match = df["Method of Payment"].astype(str).str.contains("paymentasia|zotapay-cup", case=False, na=False)

        full_mask = name_col_match & (psp_match | method_match)
        df = df[full_mask].reset_index(drop=True)
    else:
        psp_mask = df["PSP name"] == normalized_processor
        df = df[(df["Name"].str.lower() == transaction_type) & psp_mask].reset_index(drop=True)

    if "Currency" in df.columns:
        df["Currency"] = df["Currency"].replace({
            "Euro": "EUR",
            "US Dollar": "USD"
        })

    if save_clean:
        date_str = extract_date_from_filename(filepath)
        folder_name = "zotapay_paymentasia" if normalized_processor in ["zotapay",
                                                                        "paymentasia"] else normalized_processor
        folder = f"{folder_name}_{transaction_type}s.xlsx"
        out_path = PROCESSED_CRM_DIR / folder_name / date_str / folder

        out_path.parent.mkdir(parents=True, exist_ok=True)

        if transaction_type == "withdrawal" and "transaction_id" in df.columns:
            needed_columns = [
                "Created On",
                "First Name (Account) (Account)",
                "Last Name (Account) (Account)",
                "Email (Account) (Account)",
                "tp",
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

    return df.reset_index(drop=True)




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
def process_files_in_parallel(file_paths, processor_name=None, is_crm=False, save_clean=True,
                              transaction_type="deposit"):
    valid_paths = [str(p) for p in file_paths if Path(p).exists()]
    if not valid_paths:
        return []
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
    # Check if file exists before processing
    if not Path(filepath).exists():

        return None  # Return None instead of raising error
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
        # Only save if we have data
        if df_clean is not None and not df_clean.empty:
            date_str = extract_date_from_filename(filepath)
            folder_name = processor_name.lower()
            out_filename = f"{folder_name}_{transaction_type}s.xlsx"
            out_path = PROCESSED_PROCESSOR_DIR / folder_name / date_str / out_filename
            out_path.parent.mkdir(parents=True, exist_ok=True)
            df_clean.to_excel(out_path, index=False)
            print(f"✅ Saved cleaned {processor_name} {transaction_type}s to {out_path}")
        else:
            print(f"⚠️ Not saving empty {processor_name} {transaction_type}s")

    return df_clean

