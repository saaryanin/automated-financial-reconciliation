"""
Copyright 2026 Saar Yanin
Licensed under the Apache License 2.0 with Commons Clause
See LICENSE for details.

Script: preprocess.py
Description: This script preprocesses raw data from various payment processors and CRM files for deposits and withdrawals, standardizing formats, cleaning data, handling voids/cancellations, and combining files for reconciliation. It supports parallel processing, regulation-specific filtering, currency conversions, and appending prior unmatched shifted deposits, ensuring data readiness for matching across ROW and UK.

Key Features:
- Standardization: Renames columns, converts data types, filters for approved/completed statuses per processor (e.g., PayPal, SafeCharge, Skrill), and normalizes fields like emails, amounts, and dates.
- Edge case handling: Identifies and removes paired reversals/voids/refunds by grouping and netting amounts; treats cancellations as zero-net by dropping pairs.
- Grouping: Aggregates withdrawals by email/last4 or names, summing amounts with optional conversion to a target currency using exchange rates.
- ID extraction: Uses regex to pull transaction IDs from CRM comments, tailored to processors like skrill, neteller, paypal.
- Parallel processing: Employs ThreadPoolExecutor for concurrent file loading and preprocessing to improve efficiency.
- Cleaning: Robust amount cleaning (handles strings/formats), email normalization, date parsing with dateutil for mixed formats.
- Regulation filtering: Applies UK/ROW filters based on PSP/site, excludes specific PSPs for regions (e.g., barclays for ROW).
- Shifted deposits: Appends previous unmatched shifted deposits to combined CRM, checking for duplicates via transaction_id.
- Saving: Preserves string formats for IDs in Excel outputs using openpyxl.
- Edge cases: Uses SequenceMatcher for similarity checks in pairings, logs counts/warnings, handles various file formats (xlsx, xls, csv).

Dependencies:
- pandas (for DataFrame operations and file I/O)
- re (for regex in ID extraction and cleaning)
- pathlib (for file path handling)
- datetime (for date manipulations)
- concurrent.futures (for ThreadPoolExecutor parallel processing)
- Counter (from collections) (for counting occurrences)
- parser (from dateutil) (for flexible date parsing)
- numpy (for NaN handling)
- logging (for process logging)
- SequenceMatcher (from difflib) (for similarity computations)
- openpyxl (for Excel writing with formatting)
- xlrd (for reading older Excel formats)
- src.config (for directory setups)
- src.utils (for clean_amount, clean_last4, load_uk_holidays, categorize_regulation, extract_date_from_filename functions)
"""
import pandas as pd
import re
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import src.config as config
from collections import Counter
from dateutil import parser
import dateutil.parser
from src.utils import clean_amount, clean_last4, load_uk_holidays, categorize_regulation, extract_date_from_filename
import logging
from difflib import SequenceMatcher
from collections import defaultdict

# Mapping for standardizing PSP names
PSP_NAME_MAP = {
    'netteler': 'neteller',
    'skrilll': 'skrill',
    'skrill ': 'skrill',
    'skrll': 'skrill',
    'paypal ': 'paypal',
    'safecharge ': 'safecharge',
    'powercash ': 'powercash',
    'shift4 ': 'shift4',
    'zotapay': 'zotapay_paymentasia',
    'paymentasia': 'zotapay_paymentasia',
    'pamy': 'zotapay_paymentasia',
    'payment asia': 'zotapay_paymentasia',
    'acquiringcom': 'trustpayments',
    'acquiring com': 'trustpayments',
    'trust payments': 'trustpayments',
    'paysafe': 'skrill',
    'barclay card': 'barclays',
    'barclays': 'barclays',
    'bridgerpay': 'bridgerpay',
    'xbo': 'xbo',
    'XBO': 'xbo',
    # Add any other known aliases as needed
}

processed_unmatched_files = set()


# ----------------------------
# Utility Functions
# ----------------------------

def enhance_email_similarity(e1, e2):
    """
    Compute similarity between two emails, focusing on the part before '@'.
    Uses SequenceMatcher as a fallback.
    """

    e1 = '' if pd.isna(e1) else str(e1).strip().lower()
    e2 = '' if pd.isna(e2) else str(e2).strip().lower()
    if not e1 or not e2:
        return 0.0
    return SequenceMatcher(None, e1.split('@')[0], e2.split('@')[0]).ratio()


def get_previous_business_day(current_date_str):
    """
    Get the previous business day, skipping weekends and UK holidays.
    """
    current_date = datetime.strptime(current_date_str, '%Y-%m-%d')
    prev_date = current_date - timedelta(days=1)
    holidays = set(load_uk_holidays())
    skipped_dates = []  # Track skipped for logging
    while prev_date.weekday() >= 5 or prev_date.strftime('%Y-%m-%d') in holidays:
        skipped_dates.append(prev_date.strftime('%Y-%m-%d'))  # Log skipped date
        prev_date -= timedelta(days=1)
    if skipped_dates:
        logging.info(f"Skipped dates for {current_date_str}: {skipped_dates} (weekends/holidays)")
    else:
        logging.info(f"No skips for {current_date_str}; using direct previous: {prev_date.strftime('%Y-%m-%d')}")
    return prev_date.strftime('%Y-%m-%d')

def standardize_to_safecharge_date(date_input, dayfirst=False):
    """
    Force SafeCharge format 'YYYY-MM-DD HH:MM:SS'.
    dayfirst=True ONLY for paypal, xbo and powercash (your exact flip request).
    """
    if pd.isna(date_input) or str(date_input).strip() in ['', 'nan', 'NaT']:
        return ''
    try:
        dt = pd.to_datetime(str(date_input), errors='coerce', dayfirst=dayfirst)
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        logging.warning(f"Date standardization failed for value: {date_input}")
        return str(date_input).strip()


# ----------------------------
# Processor Handling for Deposits
# ----------------------------

def standardize_processor_columns_deposits(df: pd.DataFrame, processor: str) -> pd.DataFrame:
    """
    Standardize and filter deposit data for a specific processor.
    """
    processor = processor.lower()
    df.columns = df.columns.str.strip()

    if processor == "paypal":
        keep_cols = [
            "Date", "Time", "Time zone", "Name", "Type", "Status", "Currency",
            "Gross", "Fee", "Net", "From Email Address", "To Email Address", "Transaction ID"
        ]
        df = df[keep_cols]
        allowed_types = ["Express Checkout Payment"]
        df = df[(df["Status"] == "Completed") & (df["Type"].isin(allowed_types))]
        if df.empty:
            return df
        df = df.rename(columns={"Transaction ID": "transaction_id", "Net": "amount", "From Email Address": "email",
                                "Currency": "currency"})
        df['amount'] = abs(
            df['amount'].astype(str).str.replace(',', '', regex=False).apply(pd.to_numeric, errors='coerce').fillna(0))
        df['date'] = df['Date'].astype(str) + ' ' + df['Time'].astype(str)
        name_split = df['Name'].astype(str).str.strip().str.split(n=1, expand=True)
        df['first_name'] = name_split[0].fillna('')
        df['last_name'] = name_split[1].fillna('')
        df['processor_name'] = processor
        drop_cols = ["Time zone", "Status", "Fee", "Gross", "To Email Address", "Date", "Time", "Name", "Type"]
        df = df.drop(columns=[col for col in drop_cols if col in df.columns])
        if 'date' in df.columns:
            df['date'] = df['date'].apply(lambda x: standardize_to_safecharge_date(x, dayfirst=True))

    elif processor in ["safecharge", "safechargeuk"]:
        df = df[(df["Transaction Type"].str.lower() == "sale") & (df["Transaction Result"].str.lower() == "approved")]
        if df.empty:
            return df
        keep_cols = ["Transaction ID", "Date", "Amount", "Currency", "Transaction Type", "Transaction Result", "PAN",
                     "Email Address"]
        df = df[keep_cols]
        df = df.rename(
            columns={"Transaction ID": "transaction_id", "Date": "date", "Amount": "amount", "Currency": "currency"})
        df['amount'] = abs(pd.to_numeric(df['amount'], errors='coerce').fillna(0))
        df['last_4digits'] = df['PAN'].astype(str).str[-4:].str.zfill(4)
        df['email'] = df['Email Address'].astype(str).str.strip()
        df['processor_name'] = processor
        df = df.drop(columns=['Transaction Type', 'Transaction Result', 'PAN', 'Email Address'])
        if 'date' in df.columns:
            df['date'] = df['date'].apply(standardize_to_safecharge_date)

    elif processor == "powercash":
        df = df[(df["Tx-Type"].str.lower().isin(["capture", "aft"])) & (df["Status"].str.lower() == "successful") & (
            ~df["Currency"].str.upper().isin(["CAD"]))]
        if df.empty:
            return df
        df = df[["Tx-Id", "Date", "Time", "Currency", "Amount", "Firstname", "Lastname", "EMail", "Custom 3",
                 "Credit Card Number"]]
        df = df.rename(
            columns={"Tx-Id": "transaction_id", "Amount": "amount", "Currency": "currency", "Firstname": "first_name",
                     "Lastname": "last_name", "EMail": "email"})
        df['amount'] = abs(pd.to_numeric(df['amount'], errors='coerce').fillna(0))
        df['date'] = df['Date'].astype(str) + ' ' + df['Time'].astype(str)
        df['tp'] = df['Custom 3'].astype(str).str.split('-').str[0].str.strip()
        df['last_4digits'] = df['Credit Card Number'].astype(str).str[-4:].str.zfill(4)
        df['processor_name'] = processor
        drop_cols = ["Date", "Time", "Custom 3", "Credit Card Number"]
        df = df.drop(columns=[col for col in drop_cols if col in df.columns])
        if 'date' in df.columns:
            df['date'] = df['date'].apply(lambda x: standardize_to_safecharge_date(x, dayfirst=True))

    elif processor == "shift4":
        df = df[(df["Operation Type"].str.lower() == "sale") & (df["Response"].str.lower() == "completed successfully")]
        if df.empty:
            return df
        df = df[["Transaction Date", "Request ID (a1)", "Currency", "Amount", "Card Number", "Card Scheme",
                 "Cardholder Email", "Cardholder Name"]]
        df = df.rename(columns={"Transaction Date": "date", "Request ID (a1)": "transaction_id", "Amount": "amount",
                                "Currency": "currency", "Cardholder Email": "email"})
        df['amount'] = abs(pd.to_numeric(df['amount'], errors='coerce').fillna(0))
        df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
        df['last_4digits'] = df['Card Number'].astype(str).str[-4:].str.zfill(4)
        name_split = df['Cardholder Name'].astype(str).str.strip().str.split(n=1, expand=True)
        df['first_name'] = name_split[0].fillna('')
        df['last_name'] = name_split[1].fillna('')
        df['processor_name'] = processor
        drop_cols = ["Card Scheme", "Card Number", "Cardholder Name"]
        df = df.drop(columns=[col for col in drop_cols if col in df.columns])
        if 'date' in df.columns:
            df['date'] = df['date'].apply(standardize_to_safecharge_date)

    elif processor in ["skrill", "neteller"]:
        df = df.rename(columns={
            "Time (CET)": "date", "Time (UTC)": "date",
            "ID of the corresponding Skrill transaction": "transaction_id",
            "ID of the corresponding Neteller transaction": "transaction_id",
            "[+]": "amount", "Currency Sent": "currency"
        })
        df = df[(df["Type"].str.lower() == "receive money") & (df["Status"].str.lower() == "processed") & df[
            "amount"].notna()]
        if df.empty:
            return df
        df = df[~df["Transaction Details"].str.contains("fee", case=False, na=False)]
        df = df[["date", "transaction_id", "amount", "currency", "Transaction Details"]]
        df['amount'] = abs(pd.to_numeric(df['amount'], errors='coerce').fillna(0))
        df['email'] = df['Transaction Details'].astype(str).str.replace(r'^\s*from\s+', '', regex=True,
                                                                        case=False).str.strip()
        df['processor_name'] = processor
        df = df.drop(columns=['Transaction Details'])
        if 'date' in df.columns:
            df['date'] = df['date'].apply(standardize_to_safecharge_date)

    elif processor == "trustpayments":
        df = df[
            (df["Transaction Type"].astype(str).str.strip() == "Purchase") &
            (df["Status"].astype(str).str.strip() == "Cleared")
            ].copy()
        if df.empty:
            return df
        df = df.rename(columns={
            "Posting Date (UTC)": "date",
            "Transaction Currency": "currency",
            "Transaction Amount": "amount",
            "Gateway Transaction Reference": "transaction_id",
            "Card Number": "Card Number"
        })
        df = df[["date", "currency", "amount", "transaction_id", "Card Number"]]
        df['amount'] = abs(pd.to_numeric(df['amount'], errors='coerce').fillna(0))
        df['last_4digits'] = df['Card Number'].astype(str).str.extract(r'(\d{4})$').fillna('')
        df['transaction_id'] = df['transaction_id'].astype(str).str.strip().apply(
            lambda x: re.sub(r'^(\d+)-', r'\1-70-', x) if '-' in x else x
        )
        df['first_name'] = ''
        df['last_name'] = ''
        df['processor_name'] = processor
        df = df.drop(columns=['Card Number'])
        if 'date' in df.columns:
            df['date'] = df['date'].apply(standardize_to_safecharge_date)
        return df

    elif processor == "zotapay":
        df = df.copy()
        df.columns = df.iloc[0].str.strip()
        df = df.iloc[1:]
        df = df[(df["Type"].str.upper() == "SALE") & (df["Status"].str.lower() == "approved")]
        if df.empty:
            return df
        df = df.rename(columns={
            "ID": "transaction_id",
            "Order Currency": "currency",
            "Order Amount": "amount",
            "Created At": "date",
            "Ended At": "date",
            "Customer Email": "email",
            "Customer First Name": "first_name",
            "Customer Last Name": "last_name"
        })
        keep_cols = ["transaction_id", "currency", "amount", "Merchant Order Description", "date", "email",
                     "first_name", "last_name"]
        df = df[keep_cols]
        df['amount'] = abs(pd.to_numeric(df['amount'].astype(str).str.replace(',', ''), errors='coerce').fillna(0))
        df['tp'] = df['Merchant Order Description'].astype(str).str.split('-').str[0].str.strip()
        df['processor_name'] = processor
        df = df.drop(columns=['Merchant Order Description'])
        if 'date' in df.columns:
            df['date'] = df['date'].apply(standardize_to_safecharge_date)

    elif processor == "bitpay":
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
        df = df[df["tx_type"].str.lower() == "sale"]
        if df.empty:
            return df
        df = df.rename(columns={
            "invoice_id": "transaction_id",
            "payout_amount": "amount",
            "payout_currency": "currency",
            "buyeremail": "email"
        })
        keep_cols = ["date", "time", "transaction_id", "amount", "currency", "buyername", "email"]
        df = df[keep_cols]
        df['amount'] = abs(pd.to_numeric(df['amount'], errors='coerce').fillna(0))
        df['date'] = df['date'].astype(str) + ' ' + df['time'].astype(str)
        name_split = df['buyername'].astype(str).str.strip().str.split(n=1, expand=True)
        df['first_name'] = name_split[0].fillna('')
        df['last_name'] = name_split[1].fillna('')
        df['processor_name'] = processor
        drop_cols = ["time", "buyername"]
        df = df.drop(columns=[col for col in drop_cols if col in df.columns])
        if 'date' in df.columns:
            df['date'] = df['date'].apply(standardize_to_safecharge_date)

    elif processor == "ezeebill":
        df.columns = df.columns.str.replace(" ", "").str.strip()
        df = df[df["Action"].str.upper() == "SALE"]
        if df.empty:
            return df
        df = df.rename(columns={"MerchantTxnID": "transaction_id", "OriginalAmount": "amount"})
        df = df[["transaction_id", "amount"]]
        df['amount'] = abs(pd.to_numeric(df['amount'], errors='coerce').fillna(0))
        df['tp'] = df['transaction_id'].astype(str).str.split('-').str[0].str.strip()
        df['currency'] = 'MYR'
        df['processor_name'] = processor

    elif processor == "paymentasia":
        df = df[(df["Type"].str.upper() == "SALE") & (df["Status"].str.upper() == "SUCCESS")]
        if df.empty:
            return df
        df = df.rename(columns={
            "Merchant Reference": "transaction_id",
            "Order Amount": "amount",
            "Order Currency": "currency",
            "Completed Time": "date"
        })
        df = df[["transaction_id", "amount", "currency", "date"]]
        df['amount'] = abs(pd.to_numeric(df['amount'], errors='coerce').fillna(0))
        df['tp'] = df['transaction_id'].astype(str).str.split('-').str[0].str.strip()
        df['processor_name'] = processor
        if 'date' in df.columns:
            df['date'] = df['date'].apply(standardize_to_safecharge_date)

    elif processor in ["barclays", "barclaycard"]:
        df = df[df["Current Status"].str.lower() == "captured"]
        df = df[df["Trans Type Code"].str.lower() == "purchase"]
        if df.empty:
            return df
        df["transaction_id"] = df["Audit Reference"]
        df["currency"] = df["Pos ID"].astype(str).str.extract(r'(GBP|USD|EUR|TRY|CAD)')
        df["amount"] = pd.to_numeric(df["Trans Amount(HUC)"], errors='coerce').abs()
        df["date"] = df["Transaction Date"]
        df["tp"] = df["Sales Details"].astype(str).apply(
            lambda x: re.search(r'BGP(\d{6,8})6', x).group(1) if re.search(r'BGP(\d{6,8})6', x) else None)
        df['processor_name'] = processor
        df['last_4digits'] = ''
        keep_cols = ["transaction_id", "date", "amount", "currency", "tp", "processor_name"]
        df = df[keep_cols]
        if 'date' in df.columns:
            df['date'] = df['date'].apply(standardize_to_safecharge_date)

    elif processor == "xbo":
        df = df[df["status"].astype(str).str.lower().str.strip() == "approved"].copy()
        if df.empty:
            return df
        df["tp"] = df["merchantOrderId"].astype(str).str.split('-', n=1).str[0].str.strip()
        df = df.rename(columns={
            "processing_date": "date",
            "transactionId": "transaction_id",
            "firstName": "first_name",
            "lastName": "last_name",
        })
        df["amount"] = abs(pd.to_numeric(df["amount"], errors="coerce").fillna(0))
        df["processor_name"] = "xbo"
        df["last_4digits"] = ""
        keep_cols = ["date", "transaction_id", "email", "tp", "amount", "currency",
                     "first_name", "last_name", "processor_name", "last_4digits"]
        df = df[[col for col in keep_cols if col in df.columns]]

        # === XBO FINAL FLIP FIX (parses correctly then swaps day/month exactly as you requested) ===
        if 'date' in df.columns:
            dt = pd.to_datetime(df['date'], errors='coerce', dayfirst=True)
            df['date'] = dt.apply(lambda x: x.replace(month=x.day, day=x.month) if pd.notna(x) else x)
            df['date'] = df['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        return df

    # Common cleanup
    if 'transaction_id' in df:
        df['transaction_id'] = df['transaction_id'].astype(str).str.strip().fillna('UNKNOWN')
    return df.reset_index(drop=True)


# ----------------------------
# Processor Handling for Withdrawals
# ----------------------------

def patch_standardize_zotapay_paymentasia_withdrawals(df, processor):
    """
    Patch for standardizing Zotapay and PaymentAsia withdrawals.
    """
    processor_tag = processor.lower()
    df.columns = df.columns.str.strip().str.replace(u'\xa0', ' ', regex=False)

    # Handle Zotapay
    if processor.lower() == "zotapay":
        if df.columns[0].lower() not in ["type", "status", "order amount"]:
            df.columns = df.iloc[0].astype(str).str.strip()
            df = df.iloc[1:].copy()
        if "Type" not in df.columns or "Status" not in df.columns:
            return pd.DataFrame()
        df = df[(df["Type"].astype(str).str.upper() == "PAYOUT") &
                (df["Status"].astype(str).str.upper() == "APPROVED")]
        if df.empty:
            return df
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
        required_cols = ["Status", "Order Amount", "Completed Time", "Beneficiary Name"]
        normalized_cols = [col.strip().replace(u'\xa0', ' ') for col in df.columns]
        missing_cols = [col for col in required_cols if col not in normalized_cols]
        if missing_cols:
            return pd.DataFrame()
        df = df[df["Status"].astype(str).str.upper() == "SUCCESS"]
        if df.empty:
            return df
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
    """
    Standardize and filter withdrawal data for a specific processor.
    """
    if processor.lower() == "paypal":
        df.columns = df.columns.str.strip()
        df["Type"] = df["Type"].astype(str).str.strip()
        df["Status"] = df["Status"].astype(str).str.strip()
        df["To Email Address"] = df["To Email Address"].astype(str).str.strip().str.lower()
        df["Gross"] = df["Gross"].astype(str).str.replace(",", "", regex=False)
        df["Currency"] = df["Currency"].astype(str).str.strip()
        allowed_types = ["Mass Payment", "Payment Refund", "Mass Pay Reversal"]
        allowed_status = ["Completed", "Unclaimed"]
        df = df[
            df["Type"].isin(allowed_types) & df["Status"].isin(allowed_status)
            ].copy()
        if df.empty:
            return pd.DataFrame()
        to_remove = set()
        mpr_rows = df[df["Type"] == "Mass Pay Reversal"]
        for idx_mpr, row_mpr in mpr_rows.iterrows():
            email = row_mpr["To Email Address"]
            try:
                amount = float(row_mpr["Gross"])
            except Exception:
                continue
            currency = row_mpr["Currency"]
            mask = (
                    df["Type"].isin(["Mass Payment", "Payment Refund"]) &
                    (df["To Email Address"] == email) &
                    (df["Currency"] == currency) &
                    (df["Gross"].astype(float) == amount)
            )
            matches = df[mask]
            for idx_match in matches.index:
                to_remove.add(idx_mpr)
                to_remove.add(idx_match)
                break
        df = df.drop(index=list(to_remove))
        keep_mask = df["Type"].isin(["Mass Payment", "Payment Refund"])
        df = df[keep_mask]
        if df.empty:
            return pd.DataFrame()
        df = df.rename(columns={
            "Date": "date",
            "Gross": "amount",
            "Currency": "currency",
            "To Email Address": "email"
        })
        df["last_4cc"] = ""
        df["processor_name"] = "paypal"
        if "Name" in df.columns and not df["Name"].isna().all():
            name_split = df["Name"].astype(str).str.strip().str.split(n=1, expand=True)
            df["first_name"] = name_split[0] if isinstance(name_split, pd.DataFrame) else ""
            df["last_name"] = name_split[1] if isinstance(name_split, pd.DataFrame) and name_split.shape[1] > 1 else ""
        else:
            df["first_name"] = ""
            df["last_name"] = ""
        # FLIP DAY/MONTH ONLY FOR PAYPAL
        if 'date' in df.columns:
            df['date'] = df['date'].apply(lambda x: standardize_to_safecharge_date(x, dayfirst=True))
        return df[["amount", "currency", "date", "last_4cc", "email", "first_name", "last_name", "processor_name"]]

    elif processor in ["safecharge", "safechargeuk"]:
        # (unchanged SafeCharge block - no flip)
        df.columns = df.columns.str.strip()
        colmap = {col.lower().replace(" ", ""): col for col in df.columns}
        if 'transactiontype' not in colmap or 'transactionresult' not in colmap:
            return pd.DataFrame()
        credit_type = "Credit"
        void_type = "VoidCredit"
        email_col = colmap.get('emailaddress', None)
        pan_col = colmap.get('pan', None)
        df = df[df[colmap['transactionresult']].str.strip().str.lower() == "approved"].copy()
        df = df[df[colmap['transactiontype']].isin([credit_type, void_type])].copy()
        if df.empty:
            return pd.DataFrame()
        to_remove = set()
        df = df.reset_index(drop=True)
        void_rows = df[df[colmap['transactiontype']] == "VoidCredit"]
        for void_idx, void_row in void_rows.iterrows():
            void_email = str(void_row[email_col]).strip().lower() if email_col else ""
            void_last4 = str(void_row[pan_col])[-4:] if pan_col else ""
            void_amount = float(void_row[colmap['amount']])
            void_currency = str(void_row[colmap['currency']]).upper()
            found = None
            for i in range(void_idx - 1, -1, -1):
                credit_row = df.iloc[i]
                if (credit_row[colmap['transactiontype']] == "Credit" and
                    str(credit_row[email_col]).strip().lower() == void_email and
                    str(credit_row[pan_col])[-4:] == void_last4 and
                    float(credit_row[colmap['amount']]) == void_amount and
                    str(credit_row[colmap['currency']]).upper() == void_currency and
                    i not in to_remove):
                    found = i
                    break
            if found is None:
                for i in range(void_idx + 1, len(df)):
                    credit_row = df.iloc[i]
                    if (credit_row[colmap['transactiontype']] == "Credit" and
                        str(credit_row[email_col]).strip().lower() == void_email and
                        str(credit_row[pan_col])[-4:] == void_last4 and
                        float(credit_row[colmap['amount']]) == void_amount and
                        str(credit_row[colmap['currency']]).upper() == void_currency and
                        i not in to_remove):
                        found = i
                        break
            to_remove.add(void_idx)
            if found is not None:
                to_remove.add(found)
        df = df.drop(index=list(to_remove))
        df = df[df[colmap['transactiontype']] == credit_type]
        if df.empty:
            return pd.DataFrame()
        df = df.rename(columns={
            colmap['amount']: "amount",
            colmap['currency']: "currency",
            colmap['date']: "date",
            email_col: "email" if email_col else "email",
            pan_col: "last_4cc" if pan_col else "last_4cc"
        })
        df["last_4cc"] = df["last_4cc"].astype(str).str.extract(r"(\d{4})$") if pan_col else ""
        df["currency"] = df["currency"].replace({"Euro": "EUR", "US Dollar": "USD", "Canadian Dollar": "CAD", "Australian Dollar": "AUD"})
        df["processor_name"] = processor
        df["first_name"] = ""
        df["last_name"] = ""
        if 'date' in df.columns:
            df['date'] = df['date'].apply(standardize_to_safecharge_date)  # NO flip
        return df[["amount", "currency", "date", "last_4cc", "email", "first_name", "last_name", "processor_name"]]

    elif processor.lower() == "powercash":
        df.columns = df.columns.str.strip()
        df = df[
            df["Tx-Type"].str.lower().isin(["refund", "cft"]) &
            (df["Status"].str.lower() == "successful") &
            (df["Currency"].str.upper().isin(["EUR", "USD", "GBP"]))
            ]
        if df.empty:
            return pd.DataFrame()
        df = df.rename(columns={"Date": "date", "Amount": "amount", "Currency": "currency", "EMail": "email"})
        df["last_4cc"] = df["Credit Card Number"].astype(str).str[-4:]
        df["first_name"] = df.get("Firstname", "").astype(str)
        df["last_name"] = df.get("Lastname", "").astype(str)
        df["processor_name"] = "powercash"
        df = df[["amount", "currency", "date", "last_4cc", "email", "first_name", "last_name", "processor_name"]]
        # FLIP DAY/MONTH ONLY FOR POWERCASH
        if 'date' in df.columns:
            df['date'] = df['date'].apply(lambda x: standardize_to_safecharge_date(x, dayfirst=True))
        return df

    elif processor.lower() == "shift4":
        # (unchanged - no flip)
        df.columns = df.columns.str.strip()
        df["Operation Type"] = df["Operation Type"].astype(str).str.strip().str.lower()
        df["Response"] = df["Response"].astype(str).str.strip().str.lower()
        df["Cardholder Email"] = df["Cardholder Email"].astype(str).str.strip().str.lower()
        df["Card Number"] = df["Card Number"].astype(str).str.strip()
        df["Cardholder Name"] = df["Cardholder Name"].astype(str).str.strip().str.lower()
        df["Merchant Reference Number"] = df["Merchant Reference Number"].astype(str).str.strip() if "Merchant Reference Number" in df.columns else ''
        relevant_mask = df["Operation Type"].isin(["referral credit", "sale void", "refund void", "referral cft"]) & (df["Response"] == "completed successfully")
        df = df[relevant_mask].copy()
        if df.empty:
            return pd.DataFrame()
        to_remove = set()
        refund_voids = df[df["Operation Type"] == "refund void"]
        for idx_rv, refund_row in refund_voids.iterrows():
            email = refund_row["Cardholder Email"]
            amount = abs(clean_amount(refund_row["Amount"]))
            currency = refund_row["Currency"]
            card_num = refund_row["Card Number"]
            name = refund_row["Cardholder Name"]
            merchant_ref = refund_row["Merchant Reference Number"]
            mask = (
                    (df["Operation Type"] == "referral credit") &
                    (df["Currency"] == currency) &
                    (df["Amount"].apply(lambda x: abs(clean_amount(x))) == amount) &
                    (df["Card Number"] == card_num) &
                    (df["Cardholder Name"] == name)
            )
            if pd.notna(email) and str(email).strip() and str(email).lower() != 'nan':
                mask = mask & (df["Cardholder Email"] == email)
            if pd.notna(merchant_ref) and str(merchant_ref).strip() and str(merchant_ref).lower() != 'nan':
                mask = mask & (df["Merchant Reference Number"] == merchant_ref)
            possible_matches = df[mask]
            for idx_ref in possible_matches.index:
                to_remove.add(idx_rv)
                to_remove.add(idx_ref)
                break
        df = df.drop(index=list(to_remove))
        keep_mask = df["Operation Type"].isin(["referral credit", "sale void", "referral cft"])
        df = df[keep_mask]
        if df.empty:
            return pd.DataFrame()
        df = df[["Transaction Date", "Card Number", "Currency", "Amount", "Cardholder Name", "Cardholder Email", "Merchant Reference Number"]].copy()
        df = df.rename(columns={"Transaction Date": "date", "Currency": "currency", "Amount": "amount", "Cardholder Email": "email"})
        df["amount"] = df["amount"].apply(lambda x: -abs(clean_amount(x)))
        df["last_4cc"] = df["Card Number"].astype(str).str.replace(r'\D', '', regex=True).str[-4:].str.zfill(4)
        name_split = df["Cardholder Name"].astype(str).str.split(n=1, expand=True)
        df["first_name"] = name_split[0].str.rstrip("*")
        df["last_name"] = name_split[1].str.rstrip("*") if name_split.shape[1] > 1 else ""
        df["processor_name"] = "shift4"
        drop_cols = ["Card Number", "Cardholder Name", "Merchant Reference Number"]
        df = df.drop(columns=[col for col in drop_cols if col in df.columns])
        if 'date' in df.columns:
            df['date'] = df['date'].apply(standardize_to_safecharge_date)  # NO flip
        return df[["amount", "currency", "date", "last_4cc", "email", "first_name", "last_name", "processor_name"]]

    elif processor.lower() in ("skrill", "neteller"):
        # (unchanged - no flip)
        df.columns = df.columns.str.strip()
        df = df.loc[(df["Type"].str.lower() == "send money") & (df["Status"].str.lower() == "processed"), :]
        if df.empty:
            return pd.DataFrame()
        amt_col = "Amount Sent" if "Amount Sent" in df.columns else "[+]"
        df = df.loc[df[amt_col].notna() & df[amt_col].astype(str).str.strip().ne(""), :]
        if df.empty:
            return pd.DataFrame()
        df["tp"] = df["Reference"].astype(str).str.extract(r"(\d+)").fillna("")
        if "Time (CET)" in df.columns:
            df["date"] = pd.to_datetime(df["Time (CET)"])
        elif "Time (UTC)" in df.columns:
            df["date"] = pd.to_datetime(df["Time (UTC)"])
        elif "Date" in df.columns:
            df["date"] = pd.to_datetime(df["Date"])
        else:
            raise ValueError(f"Could not find a date column for {processor}")
        df = df.rename(columns={amt_col: "amount", "Currency Sent": "currency"})
        df["email"] = df["Transaction Details"].astype(str).str.replace(r"^\s*to\s*", "", regex=True).str.strip()
        df["last_4cc"] = ""
        df["first_name"] = ""
        df["last_name"] = ""
        df["processor_name"] = processor.lower()
        if 'date' in df.columns:
            df['date'] = df['date'].apply(standardize_to_safecharge_date)  # NO flip
        return df[["amount", "currency", "date", "last_4cc", "email", "first_name", "last_name", "processor_name", "tp"]]

    elif processor == "bitpay":
        # (unchanged - no flip)
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
        df = df[df["tx_type"].str.lower() == "invoice refund"]
        if df.empty:
            return pd.DataFrame()
        df = df.rename(columns={"invoice_id": "transaction_id", "payout_amount": "amount", "payout_currency": "currency", "buyername": "full_name", "buyeremail": "email"})
        name_split = df["full_name"].astype(str).str.strip().str.split(n=1, expand=True)
        df["first_name"] = name_split[0]
        df["last_name"] = name_split[1] if name_split.shape[1] > 1 else ""
        df["last_4cc"] = ""
        df["processor_name"] = "bitpay"
        df = df[["amount", "currency", "date", "last_4cc", "email", "first_name", "last_name", "processor_name"]]
        if 'date' in df.columns:
            df['date'] = df['date'].apply(standardize_to_safecharge_date)  # NO flip
        return df

    elif processor.lower() in ["zotapay", "paymentasia", "zotapay_paymentasia"]:
        return patch_standardize_zotapay_paymentasia_withdrawals(df, processor)

    elif processor == "trustpayments":
        # (unchanged - no flip)
        df = df[(df["Transaction Type"].astype(str).str.strip() == "Refund (Credit)") & (df["Status"].astype(str).str.strip() == "Cleared")].copy()
        if df.empty:
            return pd.DataFrame()
        df = df.rename(columns={"Posting Date (UTC)": "date", "Transaction Currency": "currency", "Transaction Amount": "amount", "Gateway Transaction Reference": "transaction_id", "Card Number": "Card Number"})
        df = df[["date", "currency", "amount", "transaction_id", "Card Number"]]
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0)
        df['last_4cc'] = df['Card Number'].astype(str).str.extract(r'(\d{4})$').fillna('')
        df['transaction_id'] = df['transaction_id'].astype(str).str.strip().apply(lambda x: re.sub(r'^(\d+)-', r'\1-70-', x) if '-' in x else x)
        df["first_name"] = ""
        df["last_name"] = ""
        df["email"] = ""
        df["processor_name"] = "trustpayments"
        if 'date' in df.columns:
            df['date'] = df['date'].apply(standardize_to_safecharge_date)  # NO flip
        return df[["amount", "currency", "date", "last_4cc", "email", "first_name", "last_name", "processor_name"]]

    elif processor == "xbo":
        print("XBO withdrawals intentionally left unmatched (CRM side only)")
        return pd.DataFrame()

    elif processor in ["barclays", "barclaycard"]:
        df = df[df["Current Status"].str.lower() == "captured"]
        df = df[df["Trans Type Code"].str.lower() == "refund"]
        if df.empty:
            return df
        df["transaction_id"] = df["Audit Reference"]
        df["currency"] = df["Pos ID"].astype(str).str.extract(r'(GBP|USD|EUR|TRY|CAD)')
        df["amount"] = pd.to_numeric(df["Trans Amount(HUC)"], errors='coerce').abs()
        df["date"] = df["Transaction Date"]
        df["tp"] = df["Sales Details"].astype(str).apply(lambda x: re.search(r'BGP(\d{6,8})6', x).group(1) if re.search(r'BGP(\d{6,8})6', x) else None)
        df['last_4cc'] = df['Online Token'].astype(str).str[-4:]
        df['processor_name'] = processor
        df['first_name'] = ''
        df['last_name'] = ''
        df['email'] = ''
        keep_cols = ["amount", "currency", "date", "last_4cc", "email", "first_name", "last_name", "processor_name", "tp"]
        df = df[keep_cols]
        if 'date' in df.columns:
            df['date'] = df['date'].apply(standardize_to_safecharge_date)  # NO flip
        return df

    return pd.DataFrame()


# ----------------------------
# CRM Handling
# ----------------------------

def extract_crm_transaction_id(comment: str, processor: str):
    """
    Extract transaction ID from CRM comment based on processor patterns.
    """
    text = str(comment)
    processor = processor.lower()

    # === XBO - completely separate case
    if processor == "xbo":
        match = re.search(r"PSP TransactionId:([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})", text)
        return match.group(1) if match else None

    # All other processors
    patterns = {
        "paypal": r"PSP TransactionId:([A-Z0-9]+)",
        "safecharge": r"PSP TransactionId:([12]\d{18})|More Comment:[^$]*\$(\d{19})|,\s*([12]\d{18})\s*,",
        "powercash": r"PSP TransactionId:(\d+)",
        "shift4": r"More Comment:[^$]*\$(\w+)",
        "skrill": r"More Comment:[^$]*\$(\d+)",
        "neteller": r"More Comment:[^$]*\$(\d+)",
        "trustpayments": r"PSP TransactionId:([\d\-]+)|More Comment:[^$]*\$(\d{2}-\d{2}-\d+)",
        "zotapay": r"PSP TransactionId:(\d+)",
        "bitpay": r"PSP TransactionId:([A-Za-z0-9]+)",
        "ezeebill": r"(\d{7}-\d{18})",
        "paymentasia": r"(\d{7}-\d{18})",
        "barclays": r"PSP TransactionId:([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})",
        'safechargeuk': r"PSP TransactionId:([12]\d{18})|More Comment:[^$]*\$(\d{19})|,\s*([12]\d{18})\s*,",
    }
    if processor == "bridgerpay":
        for p, pattern in patterns.items():
            match = re.search(pattern, text)
            if match:
                return next((g for g in match.groups() if g), None)
        return None
    pattern = patterns.get(processor)
    if not pattern:
        return None
    match = re.search(pattern, text)
    return next((g for g in match.groups() if g), None) if match else None


def clean_crm_amount(amt):
    """
    Clean and convert CRM amount to float.
    """
    if pd.isna(amt):
        return 0.0
    if isinstance(amt, (int, float)):
        return amt
    amt_str = str(amt).strip()
    amt_str = re.sub(r'[^\d.-]', '', amt_str)
    if amt_str.startswith('(') and amt_str.endswith(')'):
        amt_str = '-' + amt_str[1:-1]
    try:
        return float(amt_str)
    except ValueError:
        return 0.0


def load_crm_file(filepath: str, processor_name: str, regulation: str, save_clean=False, transaction_type="deposit",
                  lists_dir=None, processed_unmatched_shifted_deposits_dir=None, processed_crm_dir=None):
    """
    Load and process CRM file for a specific processor and regulation.
    """
    lists_dir = lists_dir or config.LISTS_DIR
    processed_unmatched_shifted_deposits_dir = processed_unmatched_shifted_deposits_dir or config.PROCESSED_UNMATCHED_SHIFTED_DEPOSITS_DIR
    processed_crm_dir = processed_crm_dir or config.PROCESSED_CRM_DIR
    print(f"Using processed_crm_dir for load_crm_file: {processed_crm_dir}")
    # Define normalized_processor at the start
    normalized_processor = processor_name.lower()
    # Load current raw CRM file
    df = pd.read_excel(filepath, engine="openpyxl")
    df.columns = df.columns.str.strip()
    df['regulation'] = df['Site (Account) (Account)'].apply(categorize_regulation)
    # Filter for the specified regulation
    if regulation == 'row':
        row_regs = ['mauritius', 'cyprus', 'australia','dubai']
        df = df[df['regulation'].isin(row_regs)]
    elif regulation == 'uk':
        df = df[df['regulation'] == 'uk']
    # Filter out paypal and inpendium for australia regulation (only for row)
    if regulation == 'row':
        mask_aus = df['regulation'] == 'australia'
        mask_psp = df["PSP name"].str.lower().isin(['paypal', 'inpendium'])
        df = df[~(mask_aus & mask_psp)]
    df["PSP name"] = (
        df["PSP name"]
        .astype(str)
        .str.strip()
        .str.lower()
        .replace(PSP_NAME_MAP)
    )
    if regulation == 'uk':
        df["PSP name"] = df["PSP name"].replace({'safecharge': 'safechargeuk'})
    # Override to Neteller if Method Of Payment is "Neteller", regardless of PSP name
    if "Method of Payment" in df.columns:
        neteller_mask = df["Method of Payment"].astype(str).str.strip().str.lower() == "neteller"
        df.loc[neteller_mask, "PSP name"] = "neteller"

    # === XBO normalization (only looks for XBO in Method of Payment or PSP name) ===
    if "Method of Payment" in df.columns:
        xbo_mask = df["Method of Payment"].astype(str).str.strip().str.upper() == "XBO"
        df.loc[xbo_mask, "PSP name"] = "xbo"
    if "PSP name" in df.columns:
        xbo_name_mask = df["PSP name"].astype(str).str.strip().str.upper() == "XBO"
        df.loc[xbo_name_mask, "PSP name"] = "xbo"

    df["tp"] = df["TP Account"] if "TP Account" in df.columns else ""
    if normalized_processor == 'bridgerpay':
        df['transaction_id'] = df.get('Internal Comment', pd.Series(index=df.index, dtype=str)).astype(str).str.strip()
    else:
        df['transaction_id'] = df['Internal Comment'].apply(
            lambda c: extract_crm_transaction_id(c, normalized_processor))
    df['transaction_id'] = df['transaction_id'].astype(str).fillna('UNKNOWN')
    if normalized_processor in ["zotapay", "paymentasia", "zotapay_paymentasia"]:
        name_col_match = df["Name"].str.lower() == transaction_type
        method_match = df["Method of Payment"].astype(str).str.contains("paymentasia|zotapay-cup|pa-my", case=False,
                                                                        na=False)
        psp_match = df["PSP name"].str.contains("zotapay_paymentasia", case=False, na=False)
        wire_match = df["PSP name"].str.contains("wire ?transfer", case=False, regex=True, na=False)
        full_mask = name_col_match & (psp_match | (wire_match & method_match))
        df = df[full_mask].reset_index(drop=True)
    else:
        psp_mask = df["PSP name"] == normalized_processor
        if transaction_type == "withdrawal":
            name_mask = df["Name"].str.lower().isin(["withdrawal", "withdrawal cancelled"])  # ← Supports cancelled
            df = df[name_mask & psp_mask].reset_index(drop=True)
        else:
            df = df[(df["Name"].str.lower() == transaction_type) & psp_mask].reset_index(drop=True)
        if regulation == 'uk' and normalized_processor == 'safechargeuk':
            df["PSP name"] = 'safecharge'
    if "Currency" in df.columns:
        df["Currency"] = df["Currency"].replace({
            "Euro": "EUR",
            "US Dollar": "USD",
            "Canadian Dollar": "CAD",
            "Australian Dollar": "AUD"
        })
    if df.empty:
        return None
    if transaction_type == "withdrawal":
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
            "CC Last 4 Digits",
            "Name",
            "Site (Account) (Account)",
            "Internal Comment"
        ]
        df = df[[col for col in needed_columns if col in df.columns]]
        if "transaction_id" in df.columns:
            df = df.drop(columns=["transaction_id"])
    elif transaction_type == "deposit":
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
            "CC Last 4 Digits",
            "transaction_id",
            "Approved",
            "Name",
            "Site (Account) (Account)"
        ]
        df = df[[col for col in needed_columns if col in df.columns]]
        df['crm_approved'] = df['Approved'].str.strip().str.lower().map({'yes': 1, 'no': 0}).fillna(
            0) if 'Approved' in df.columns else pd.Series(0, index=df.index)
        df['crm_transaction_id'] = df['transaction_id'].fillna('UNKNOWN')
    if save_clean:
        date_str = extract_date_from_filename(filepath)
        # Save processed CRM file without extra regulation folder
        folder_name = 'safecharge' if regulation == 'uk' and normalized_processor == 'safechargeuk' else (
            "zotapay_paymentasia" if normalized_processor in ["zotapay", "paymentasia",
                                                              "zotapay_paymentasia"] else normalized_processor)
        folder = f"{folder_name}_{transaction_type}s.xlsx"
        out_path = processed_crm_dir / folder_name / date_str / folder
        print(f"Calculated out_path: {out_path}")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with pd.ExcelWriter(out_path, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Sheet1')
            if "transaction_id" in df.columns and transaction_type == "deposit":
                worksheet = writer.sheets['Sheet1']
                trans_col = df.columns.get_loc('transaction_id') + 1
                for row in range(2, len(df) + 2):
                    worksheet.cell(row=row, column=trans_col).number_format = '@'
    return df.reset_index(drop=True)


def process_crm_subset(df: pd.DataFrame, processor: str, regulation: str, transaction_type: str, save_clean: bool,
                       processed_crm_dir: Path, date_str: str):
    """
    Process a subset of CRM data for a processor.
    """
    normalized_processor = processor.lower()
    if normalized_processor in ["zotapay", "paymentasia", "zotapay_paymentasia"] and transaction_type == "withdrawal":
        method_match = df["Method of Payment"].astype(str).str.contains("paymentasia|zotapay-cup|pa-my", case=False,
                                                                        na=False)
        psp_match = df["PSP name"].str.contains("zotapay_paymentasia", case=False, na=False)
        wire_match = df["PSP name"].str.contains("wire ?transfer", case=False, regex=True, na=False)
        full_mask = method_match | psp_match | (wire_match & method_match)
        df = df[full_mask].reset_index(drop=True)
    if df.empty:
        return None
    df["tp"] = df.get("TP Account", "")

    # === XBO normalization (only looks for XBO in Method of Payment or PSP name) ===
    if "Method of Payment" in df.columns:
        xbo_mask = df["Method of Payment"].astype(str).str.strip().str.upper() == "XBO"
        df.loc[xbo_mask, "PSP name"] = "xbo"
    if "PSP name" in df.columns:
        xbo_name_mask = df["PSP name"].astype(str).str.strip().str.upper() == "XBO"
        df.loc[xbo_name_mask, "PSP name"] = "xbo"

    if normalized_processor == 'bridgerpay':
        df['transaction_id'] = df.get('Internal Comment', pd.Series(index=df.index, dtype=str)).astype(str).str.strip()
    else:
        df['transaction_id'] = df['Internal Comment'].apply(
            lambda c: extract_crm_transaction_id(c, normalized_processor) if pd.notna(c) else 'UNKNOWN')
    df['transaction_id'] = df['transaction_id'].astype(str).fillna('UNKNOWN')
    if "Currency" in df.columns:
        df["Currency"] = df["Currency"].replace(
            {"Euro": "EUR", "US Dollar": "USD", "Canadian Dollar": "CAD", "Australian Dollar": "AUD"})
    if transaction_type == "withdrawal":
        needed_columns = ["Created On", "First Name (Account) (Account)", "Last Name (Account) (Account)",
                          "Email (Account) (Account)", "tp", "Amount", "Currency", "Method of Payment", "PSP name",
                          "CC Last 4 Digits", "Name", "Site (Account) (Account)", "Internal Comment"]
        df = df[[col for col in needed_columns if col in df.columns]]
        if "transaction_id" in df.columns:
            df = df.drop(columns=["transaction_id"])
    elif transaction_type == "deposit":
        needed_columns = ["Created On", "First Name (Account) (Account)", "Last Name (Account) (Account)",
                          "Email (Account) (Account)", "tp", "Amount", "Currency", "Method of Payment", "PSP name",
                          "CC Last 4 Digits", "transaction_id", "Approved", "Name", "Site (Account) (Account)"]
        df = df[[col for col in needed_columns if col in df.columns]]
        df['crm_approved'] = df.get('Approved', 0).str.strip().str.lower().map({'yes': 1, 'no': 0}).fillna(0)
        df['crm_transaction_id'] = df['transaction_id'].fillna('UNKNOWN')
    if save_clean:
        folder_name = 'safecharge' if regulation == 'uk' and normalized_processor == 'safechargeuk' else (
            "zotapay_paymentasia" if normalized_processor in ["zotapay", "paymentasia",
                                                              "zotapay_paymentasia"] else normalized_processor)
        out_filename = f"{folder_name}_{transaction_type}s.xlsx"
        out_path = processed_crm_dir / folder_name / date_str / out_filename
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with pd.ExcelWriter(out_path, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Sheet1')
            if "transaction_id" in df.columns and transaction_type == "deposit":
                worksheet = writer.sheets['Sheet1']
                trans_col = df.columns.get_loc('transaction_id') + 1
                for row in range(2, len(df) + 2):
                    worksheet.cell(row=row, column=trans_col).number_format = '@'
    return df.reset_index(drop=True)


# ----------------------------
# Parallel Processing
# ----------------------------

def process_files_in_parallel(file_paths, processor_names=None, is_crm=False, save_clean=True,
                              transaction_type="deposit", regulation=None, lists_dir=None,
                              processed_unmatched_shifted_deposits_dir=None, processed_crm_dir=None,
                              processed_processor_dir=None):
    """
    Process multiple files in parallel using ThreadPoolExecutor.
    """
    if isinstance(processor_names, str):
        processor_names = [processor_names]  # Convert single str to list for consistency
    if processor_names and len(processor_names) != len(file_paths):
        raise ValueError("processor_names must match length of file_paths if provided")
    # Get valid indices (skip None or non-existing paths)
    valid_indices = [i for i, p in enumerate(file_paths) if p is not None and Path(p).exists()]
    if not valid_indices:
        return []
    with ThreadPoolExecutor() as executor:
        futures = []
        for i in valid_indices:
            path = file_paths[i]
            p_name = processor_names[i] if processor_names else None
            if p_name is None:
                raise ValueError("processor_names must be provided for multiple processors")
            if is_crm:
                futures.append(
                    executor.submit(load_crm_file, str(path), p_name, regulation, save_clean, transaction_type,
                                    lists_dir=lists_dir,
                                    processed_unmatched_shifted_deposits_dir=processed_unmatched_shifted_deposits_dir,
                                    processed_crm_dir=processed_crm_dir))
            else:
                futures.append(executor.submit(load_processor_file, str(path), p_name, save_clean,
                                               transaction_type=transaction_type,
                                               processed_processor_dir=processed_processor_dir, regulation=regulation))
        results = [f.result() for f in futures]
    return results


# ----------------------------
# Processor File Loader
# ----------------------------

def load_processor_file(filepath: str, processor_name: str, save_clean=False, transaction_type="deposit",
                        processed_processor_dir=None, regulation: str = None) -> pd.DataFrame:
    """
    Load and process a processor file.
    """
    processed_processor_dir = processed_processor_dir or config.PROCESSED_PROCESSOR_DIR
    print(f"Using processed_processor_dir for load_processor_file: {processed_processor_dir}")
    # Check if file exists before processing
    if not Path(filepath).exists():
        return None  # Return None instead of raising error
    ext = Path(filepath).suffix.lower()
    dtype = {
        "Transaction ID": str,
        "Tx-Id": str,
        "Request ID (a1)": str,
        "ID of the corresponding Skrill transaction": str,
        "ID of the corresponding Neteller transaction": str,
        'Pan': str,
    }
    skip = 15 if processor_name.lower() == "ezeebill" else 11 if processor_name.lower() in ["safecharge",
                                                                                            "safechargeuk"] else 4 if processor_name.lower() == "barclays" else 0
    if ext == ".csv":
        df = pd.read_csv(filepath, dtype=dtype, encoding="utf-8-sig", skiprows=skip)
    elif ext in [".xlsx", ".xls"]:
        engine = 'xlrd' if ext == ".xls" else 'openpyxl'
        df = pd.read_excel(filepath, dtype=dtype, skiprows=skip, engine=engine)
    else:
        raise ValueError("Unsupported file type")
    print(f"Loaded raw file for {processor_name} {transaction_type}: shape {df.shape}, columns {df.columns.tolist()}")
    if transaction_type == "deposit":
        df_clean = standardize_processor_columns_deposits(df, processor_name)
        if 'currency' in df_clean.columns:
            df_clean['currency'] = df_clean['currency'].astype(str).str.upper()
    else:
        df_clean = standardize_processor_columns_withdrawals(df, processor_name)
        # Special handling for Barclays declined refunds (UK only, withdrawals)
        if transaction_type == "withdrawal" and processor_name.lower() in ["barclays", "barclaycard"]:
            declined_raw = df[
                (df["Current Status"].str.lower() == "declined") &
                (df["Trans Type Code"].str.lower() == "refund")
                ].copy()
            if not declined_raw.empty:
                declined_df = declined_raw.copy()
                declined_df["currency"] = declined_df["Pos ID"].astype(str).str.extract(r'(GBP|USD|EUR|TRY|CAD)')
                declined_df["amount"] = pd.to_numeric(declined_df["Trans Amount(HUC)"], errors='coerce').abs()
                declined_df["date"] = declined_df["Transaction Date"]
                declined_df["tp"] = declined_df["Sales Details"].astype(str).apply(
                    lambda x: re.search(r'BGP(\d{6,8})6', x).group(1) if re.search(r'BGP(\d{6,8})6', x) else None)
                declined_df['last_4cc'] = declined_df['Online Token'].astype(str).str[-4:]
                declined_df['processor_name'] = processor_name.lower()
                declined_df['first_name'] = ''
                declined_df['last_name'] = ''
                declined_df['email'] = ''
                keep_cols = ["amount", "currency", "date", "last_4cc", "email", "first_name", "last_name",
                             "processor_name", "tp"]
                declined_df = declined_df[[col for col in keep_cols if col in declined_df.columns]]
                date_str = extract_date_from_filename(filepath)
                folder_name = processor_name.lower()
                out_filename = f"{folder_name}_declined_withdrawals.xlsx"
                out_path = processed_processor_dir / folder_name / date_str / out_filename
                out_path.parent.mkdir(parents=True, exist_ok=True)
                declined_df.to_excel(out_path, index=False)
                print(f"Saved Barclays declined withdrawals to {out_path}")
        if 'currency' in df_clean.columns:
            df_clean['currency'] = df_clean['currency'].astype(str).str.upper()
    if not df_clean.empty and 'currency' in df_clean.columns:
        shared_processors = ['trustpayments', 'shift4', 'skrill', 'powercash', 'paypal', 'neteller']
        if processor_name.lower() in shared_processors:
            if regulation == 'row':
                df_clean = df_clean[df_clean['currency'] != 'GBP']
            elif regulation == 'uk':
                df_clean = df_clean[df_clean['currency'] == 'GBP']
    if df_clean is None or df_clean.empty:
        print(f"Cleaned df for {processor_name} {transaction_type} is empty or None")
        return None
    if save_clean:
        # Only save if we have data
        if df_clean is not None and not df_clean.empty:
            date_str = extract_date_from_filename(filepath)
            folder_name = processor_name.lower()
            out_filename = f"{folder_name}_{transaction_type}s.xlsx"
            out_path = processed_processor_dir / folder_name / date_str / out_filename
            print(f"Calculated out_path: {out_path}")
            out_path.parent.mkdir(parents=True, exist_ok=True)
            df_clean.to_excel(out_path, index=False)
        else:
            print(f" Not saving empty {processor_name} {transaction_type}s")
    return df_clean


# ----------------------------
# Combine Processed Files
# ----------------------------

def combine_processed_files(
        date, processors, processor_name=None,
        processed_crm_dir=None,
        processed_proc_dir=None,
        out_crm_dir=None,
        out_proc_dir=None,
        transaction_type="withdrawal",
        exchange_rate_map=None,
        extra_processors=None,
        regulation: str = None,
        crm_dir=None
):
    """
    Combine processed CRM and processor files, with grouping for withdrawals.
    """
    processed_crm_dir = processed_crm_dir or config.LISTS_DIR
    processed_proc_dir = processed_proc_dir or config.LISTS_DIR
    if extra_processors is None:
        extra_processors = []
    all_processors = list(processors) + list(extra_processors)
    if out_crm_dir is None:
        out_crm_dir = processed_crm_dir / "combined"
    if out_proc_dir is None:
        out_proc_dir = processed_proc_dir / "combined"
    dirs = config.setup_dirs_for_reg(regulation, create=True)
    crm_dfs, proc_dfs = [], []
    crm_file_template = f"{{}}_{transaction_type}s.xlsx"
    proc_file_template = f"{{}}_{transaction_type}s.xlsx"
    # Load other processed CRM files (removed regulation.upper() from path)
    for proc in all_processors:
        crm_f = processed_crm_dir / (proc if not (
                    regulation == 'uk' and proc == 'safechargeuk') else 'safecharge') / date / f"{proc if not (regulation == 'uk' and proc == 'safechargeuk') else 'safecharge'}_{transaction_type}s.xlsx"
        print(f"Looking for CRM file: {crm_f}")
        if crm_f.exists():
            df = pd.read_excel(crm_f, dtype={'transaction_id': str} if transaction_type == "deposit" else None)
            crm_dfs.append(df)
        else:
            print(f" CRM processed file not found for {proc}: {crm_f}")
    for proc in all_processors:  # Only original processors for processor files
        proc_f = processed_proc_dir / proc / date / proc_file_template.format(proc)
        print(f"Looking for processor file: {proc_f}")
        if proc_f.exists():
            df = pd.read_excel(proc_f, dtype={'transaction_id': str} if transaction_type == "deposit" else None)
            proc_dfs.append(df)
        else:
            print(f" Processor processed file not found for {proc}: {proc_f}")

    def choose_target_currency(currencies):
        cur_set = set(currencies)
        if 'USD' in cur_set:
            return 'USD'
        elif 'EUR' in cur_set:
            return 'EUR'
        else:
            return Counter(currencies).most_common(1)[0][0]

    def group_crm_withdrawals(df, exchange_rate_map):
        df = df.copy()
        # Always clean
        for col in ['Name', 'CC Last 4 Digits', 'PSP name', 'Email (Account) (Account)',
                    'First Name (Account) (Account)', 'Last Name (Account) (Account)', 'Internal Comment']:
            if col in df.columns:
                df[col] = df[col].fillna('').astype(str).str.strip().str.lower()
        if 'Currency' not in df.columns:
            df['Currency'] = 'USD'
        if 'Amount' not in df.columns:
            df['Amount'] = 0.0
        # Clean Amount
        df['Amount'] = df['Amount'].apply(clean_crm_amount)
        # Add abs_amount for grouping
        df['abs_amount'] = df['Amount'].abs()
        # Decide if last4 is available
        last4_nonblank = (df['CC Last 4 Digits'].astype(str).str.strip() != '').any()
        # Identify emails with cancellations
        canc_emails = set(df[df['Name'] == 'withdrawal cancelled']['Email (Account) (Account)'].unique())
        grouped_rows = []
        # Group by email first
        for email, group in df.groupby('Email (Account) (Account)'):
            if group.empty:
                continue
            has_canc = email in canc_emails
            # Define group cols based on has_canc
            if last4_nonblank:
                base_cols = ['CC Last 4 Digits']
            else:
                base_cols = ['First Name (Account) (Account)', 'Last Name (Account) (Account)']
            if has_canc:
                group_cols = base_cols + ['Currency', 'Internal Comment']
            else:
                group_cols = base_cols + ['PSP name', 'Currency']
            for keys, sub_group in group.groupby(group_cols):
                sub_group = sub_group[sub_group['Amount'].notna() & sub_group['Currency'].notna()]
                if sub_group.empty:
                    continue
                currencies = sub_group['Currency'].tolist()
                tgt_cur = choose_target_currency(currencies)
                converted_amounts = []
                for _, row in sub_group.iterrows():
                    amt = float(row['Amount'])
                    if row.get('Name', '') == 'withdrawal cancelled':
                        amt = abs(amt)  # Positive for cancellations
                    else:
                        amt = -abs(amt)  # Negative for regular withdrawals
                    src_cur = row['Currency']
                    if src_cur == tgt_cur:
                        converted = amt
                    else:
                        key = (src_cur, tgt_cur)
                        if exchange_rate_map and key in exchange_rate_map:
                            converted = amt * exchange_rate_map[key]
                        else:
                            converted = amt  # Fallback if no rate
                    converted_amounts.append(converted)
                total_amt = sum(converted_amounts)
                if abs(total_amt) < 1e-6:
                    continue
                # Determine type based on sign
                if total_amt > 0:
                    target_type = 'withdrawal cancelled'
                else:
                    target_type = 'withdrawal'
                # Select rows of target_type
                type_rows = sub_group[sub_group['Name'].str.lower() == target_type.lower()]
                if not type_rows.empty:
                    # Prefer non-na PSP
                    non_na_psp = type_rows[type_rows['PSP name'].notna()]
                    if not non_na_psp.empty:
                        row0 = non_na_psp.iloc[0].copy()
                    else:
                        row0 = type_rows.iloc[0].copy()
                else:
                    # Fallback to any
                    non_na_psp = sub_group[sub_group['PSP name'].notna()]
                    if not non_na_psp.empty:
                        row0 = non_na_psp.iloc[0].copy()
                    else:
                        row0 = sub_group.iloc[0].copy()
                # Set
                row0['Amount'] = -total_amt  # FIXED: flip sign for expected convention (negative for net CANCEL)
                row0['Currency'] = tgt_cur
                row0['Name'] = target_type
                grouped_rows.append(row0)
        out_df = pd.DataFrame(grouped_rows)
        return out_df

    def group_processor_withdrawals(df, exchange_rate_map):
        df = df.copy()
        # Clean string columns used in grouping to avoid issues
        for col in ['processor_name', 'first_name', 'last_name', 'email', 'last_4cc']:
            if col not in df.columns:
                df[col] = ''
            df[col] = df[col].astype(str).fillna('').str.strip()
        df['last_4cc'] = df['last_4cc'].apply(clean_last4)
        out_dfs = []
        for pname, group in df.groupby('processor_name'):
            grouped_rows = []
            if pname == 'safechargeuk':
                # Special handling for safechargeuk: group by email
                for email, sub_group in group.groupby('email'):
                    if sub_group.empty:
                        continue
                    currencies = sub_group['currency'].tolist()
                    tgt_cur = choose_target_currency(currencies)
                    converted_amounts = []
                    for _, row in sub_group.iterrows():
                        amt = float(row['amount'])
                        src_cur = row['currency']
                        if src_cur == tgt_cur:
                            converted = amt
                        else:
                            key = (src_cur, tgt_cur)
                            if exchange_rate_map and key in exchange_rate_map:
                                converted = amt * exchange_rate_map[key]
                            else:
                                converted = amt
                        converted_amounts.append(converted)
                    total_amt = sum(converted_amounts)
                    if abs(total_amt) < 1e-6:
                        continue
                    agg_row = sub_group.iloc[0].copy()
                    agg_row['amount'] = total_amt
                    agg_row['currency'] = tgt_cur
                    # Collect unique last_4cc into a comma-separated string
                    unique_last4 = sorted(set(str(l4).strip() for l4 in sub_group['last_4cc'] if str(l4).strip()))
                    agg_row['last_4cc'] = ','.join(unique_last4)
                    grouped_rows.append(agg_row)
            else:
                if 'last_4cc' in group.columns and (group['last_4cc'].astype(str).str.strip() != '').any():
                    # Group by last_4cc
                    for last4, subg in group.groupby('last_4cc', dropna=False):
                        if subg.empty or pd.isna(last4) or str(last4).strip() == '':
                            continue
                        # Assume all rows have email; filter to non-NaN for safety
                        non_na_mask = subg['email'].notna() & (subg['email'].str.strip() != '')
                        if not non_na_mask.any():
                            # If all NaN emails, append rows as-is (or skip, depending on needs)
                            grouped_rows.extend([row.copy() for _, row in subg.iterrows()])
                            continue
                        subg = subg[non_na_mask].reset_index(drop=True)  # Reset index for iloc
                        emails = subg['email'].tolist()
                        if len(emails) <= 1:
                            grouped_rows.append(subg.iloc[0].copy())
                            continue
                        # Compute pairwise similarities
                        high_similar = []
                        for i, email1 in enumerate(emails):
                            for j, email2 in enumerate(emails[i + 1:], i + 1):
                                sim = enhance_email_similarity(email1, email2)
                                if sim >= 0.8:
                                    high_similar.append((i, j, sim))
                        if not high_similar:
                            # No similarities: keep all rows separate
                            grouped_rows.extend([row.copy() for _, row in subg.iterrows()])
                            continue
                        # Build graph for connected components

                        graph = defaultdict(list)
                        for i, j, _ in high_similar:
                            graph[i].append(j)
                            graph[j].append(i)
                        # DFS to find components
                        visited = set()
                        components = []
                        for idx in range(len(emails)):
                            if idx not in visited:
                                component = []
                                stack = [idx]
                                while stack:
                                    node = stack.pop()
                                    if node not in visited:
                                        visited.add(node)
                                        component.append(node)
                                        for neigh in graph[node]:
                                            if neigh not in visited:
                                                stack.append(neigh)
                                if component:
                                    components.append(component)
                        # Aggregate within each component
                        for component in components:
                            if not component:
                                continue
                            agg_rows = subg.iloc[component]
                            currencies = agg_rows['currency'].tolist()
                            tgt_cur = choose_target_currency(currencies)
                            converted_amounts = []
                            for _, row in agg_rows.iterrows():
                                amt = float(row['amount'])
                                src_cur = row['currency']
                                if src_cur == tgt_cur:
                                    converted = amt
                                else:
                                    key = (src_cur, tgt_cur)
                                    if exchange_rate_map and key in exchange_rate_map:
                                        converted = amt * exchange_rate_map[key]
                                    else:
                                        converted = amt  # Fallback if no rate
                                converted_amounts.append(converted)
                            total_amt = sum(converted_amounts)
                            agg_row = agg_rows.iloc[0].copy()  # Use first row in component as base
                            agg_row['amount'] = total_amt
                            agg_row['currency'] = tgt_cur
                            agg_row['email'] = list(set(emails[idx] for idx in component))
                            grouped_rows.append(agg_row)
                else:
                    # Fallback if no last_4cc: no aggregation
                    grouped_rows.extend([row.copy() for _, row in group.iterrows()])
            if grouped_rows:
                out_dfs.append(pd.DataFrame(grouped_rows))
        if out_dfs:
            out_df = pd.concat(out_dfs, ignore_index=True)
        else:
            out_df = pd.DataFrame()
        return out_df

    # Combine and save
    if crm_dfs:
        # Filter non-empty before concat to avoid FutureWarning
        crm_dfs = [df for df in crm_dfs if not df.empty]
        combined_crm = pd.concat(crm_dfs, ignore_index=True)
        if transaction_type == "withdrawal":
            # In preprocess_test.py, update the df_cancels regulation filtering in combine_processed_files
            # Find this section (around line where df_cancels is processed):

            raw_crm_path = crm_dir / f"crm_{date}.xlsx"  # Use passed crm_dir
            if raw_crm_path.exists():
                df_raw = pd.read_excel(raw_crm_path, engine="openpyxl")
                df_raw.columns = df_raw.columns.str.strip()
                cancel_mask = df_raw["Name"].astype(str).str.strip().str.lower() == "withdrawal cancelled"
                df_cancels = df_raw[cancel_mask].copy()
                # The part of the code where it filters the rows that have value in the PSP name column is commented because there might be scenarios when there is a value there
                # cancel_psp_na = df_cancels["PSP name"].isna() | (df_cancels["PSP name"].str.strip() == "")
                # df_cancels = df_cancels[cancel_psp_na]
                # Exclude cancellations with 'Wire Transfer' in Method of Payment
                if 'Method of Payment' in df_cancels.columns:
                    df_cancels = df_cancels[
                        ~df_cancels['Method of Payment'].astype(str).str.strip().str.lower().eq('wire transfer')]
                df_cancels['regulation'] = df_cancels['Site (Account) (Account)'].apply(categorize_regulation)
                # FIXED: Use isin for ROW regulation (mauritius/cyprus/australia), exact match for UK
                row_regs = ['mauritius', 'cyprus', 'australia','dubai'] if regulation == 'row' else [regulation]
                df_cancels = df_cancels[df_cancels['regulation'].isin(row_regs)]
                mask_aus = df_cancels['regulation'] == 'australia'
                mask_psp = df_cancels["PSP name"].str.lower().isin(['paypal', 'inpendium'])
                df_cancels = df_cancels[~(mask_aus & mask_psp)]
                mask_can = df_cancels['regulation'] == 'canada'
                df_cancels = df_cancels[~mask_can]
                if "Currency" in df_cancels.columns:
                    df_cancels["Currency"] = df_cancels["Currency"].replace({
                        "Euro": "EUR",
                        "US Dollar": "USD",
                        "Canadian Dollar": "CAD",
                        "Australian Dollar": "AUD"
                    })
                if "TP Account" in df_cancels.columns:
                    df_cancels["tp"] = df_cancels["TP Account"]
                # Select only needed columns for df_cancels
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
                    "CC Last 4 Digits",
                    "Name",
                    "Site (Account) (Account)",
                    "Internal Comment"
                ]
                df_cancels = df_cancels[[col for col in needed_columns if col in df_cancels.columns]]
                # Infer PSP name for cancellations based on Internal Comment patterns
                patterns = {
                    "paypal": r"PSP TransactionId:([A-Z0-9]+)",
                    "safecharge": r"PSP TransactionId:([12]\d{18})|More Comment:[^$]*\$(\d{19})|,\s*([12]\d{18})\s*,",
                    "powercash": r"PSP TransactionId:(\d+)",
                    "shift4": r"More Comment:[^$]*\$(\w+)",
                    "skrill": r"More Comment:[^$]*\$(\d+)",
                    "neteller": r"More Comment:[^$]*\$(\d+)",
                    "trustpayments": r"PSP TransactionId:([\d\-]+)|More Comment:[^$]*\$(\d{2}-\d{2}-\d+)",
                    "zotapay": r"PSP TransactionId:(\d+)",
                    "bitpay": r"PSP TransactionId:([A-Za-z0-9]+)",
                    "ezeebill": r"(\d{7}-\d{18})",
                    "paymentasia": r"(\d{7}-\d{18})",
                    "barclays": r"PSP TransactionId:([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})",
                    'safechargeuk': r"PSP TransactionId:([12]\d{18})|More Comment:[^$]*\$(\d{19})|,\s*([12]\d{18})\s*,",
                }
                for idx, row in df_cancels.iterrows():
                    comment = str(row.get('Internal Comment', ''))
                    inferred_proc = None
                    for proc, pattern in patterns.items():
                        if re.search(pattern, comment):
                            inferred_proc = proc
                            break
                    if inferred_proc:
                        df_cancels.at[idx, 'PSP name'] = inferred_proc
                combined_crm = pd.concat([combined_crm, df_cancels], ignore_index=True)
            combined_crm = group_crm_withdrawals(combined_crm, exchange_rate_map)
        # No grouping for deposits
        if 'crm_transaction_id' in combined_crm.columns:
            combined_crm['crm_transaction_id'] = combined_crm['crm_transaction_id'].astype(str)
        # In combine_processed_files function, update the rename_map to include transaction_id
        rename_map = {
            'Created On': 'crm_date',
            'First Name (Account) (Account)': 'crm_firstname',
            'Last Name (Account) (Account)': 'crm_lastname',
            'Email (Account) (Account)': 'crm_email',
            'tp': 'crm_tp',
            'Amount': 'crm_amount',
            'Currency': 'crm_currency',
            'Method of Payment': 'payment_method',  # Renamed
            'Approved': 'crm_approved',
            'PSP name': 'crm_processor_name',
            'CC Last 4 Digits': 'crm_last4',
            'Site (Account) (Account)': 'regulation',
            'transaction_id': 'crm_transaction_id',
            'Name': 'crm_type'  # Keep for traceability, can remove if unwanted
        }
        # Rename columns and remove duplicates
        for old_col, new_col in rename_map.items():
            if old_col in combined_crm.columns:
                combined_crm[new_col] = combined_crm[old_col]  # Overwrite with new name
                if old_col != new_col:  # Only drop if different to avoid self-drop
                    combined_crm = combined_crm.drop(columns=[old_col], errors='ignore')
        combined_crm = combined_crm.loc[:, ~combined_crm.columns.duplicated()]  # Ensure no duplicates
        if 'Approved' in combined_crm.columns:
            combined_crm = combined_crm.drop(columns=['Approved'])
        # Remove unwanted columns if present
        unwanted_columns = [
            '(Do Not Modify) Monetary Transaction', '(Do Not Modify) Row Checksum', '(Do Not Modify) Modified On',
            'Approved On', 'TP Account', 'Internal Comment', 'Internal Type', 'Country Of Residence (Account) (Account)'
        ]
        combined_crm = combined_crm.drop(columns=[col for col in unwanted_columns if col in combined_crm.columns],
                                         errors='ignore')
        if not combined_crm.empty:
            combined_crm['regulation'] = combined_crm['regulation'].apply(categorize_regulation)
            combined_crm = combined_crm[combined_crm['regulation'] != 'canada']
        # Define base column order with regulation between crm_last4 and crm_transaction_id
        base_columns = [
            'crm_date', 'crm_firstname', 'crm_lastname', 'crm_email', 'crm_tp', 'crm_amount', 'crm_currency',
            'payment_method', 'crm_approved', 'crm_processor_name', 'crm_last4', 'regulation', 'crm_transaction_id'
        ]
        # Get all existing columns
        existing_columns = combined_crm.columns.tolist()
        print(
            f"Debug: combined_crm type: {type(combined_crm)}, shape: {combined_crm.shape}, columns: {existing_columns}")  # Debug
        # Append any additional columns not in base
        custom_columns = base_columns + [col for col in existing_columns if col not in base_columns]
        # Reorder columns, using only those that exist
        combined_crm = combined_crm[[col for col in custom_columns if col in existing_columns]]
        out_crm_date_dir = out_crm_dir / date  # Removed regulation.upper()
        out_crm_date_dir.mkdir(parents=True, exist_ok=True)
        combined_crm_path = out_crm_date_dir / f"combined_crm_{transaction_type}s.xlsx"
        with pd.ExcelWriter(combined_crm_path, engine='openpyxl') as writer:
            combined_crm.to_excel(writer, index=False, sheet_name='Sheet1')
            if 'crm_transaction_id' in combined_crm.columns and not combined_crm.empty:
                worksheet = writer.sheets['Sheet1']
                trans_col = combined_crm.columns.get_loc('crm_transaction_id') + 1
                for row in range(2, len(combined_crm) + 2):
                    worksheet.cell(row=row, column=trans_col).number_format = '@'
        print(f"Combined CRM columns: {combined_crm.columns.tolist()}")  # Debug print
        print(f"Combined CRM {transaction_type}s saved to {combined_crm_path}")
    else:
        print("No CRM files found to combine.")
    if proc_dfs:
        # Filter non-empty before concat to avoid FutureWarning
        proc_dfs = [df for df in proc_dfs if not df.empty]
        combined_proc = pd.concat(proc_dfs, ignore_index=True)
        if transaction_type == "withdrawal":
            combined_proc = group_processor_withdrawals(combined_proc, exchange_rate_map)
        # No grouping for deposits
        # Rename columns for processor
        rename_map_proc = {
            'amount': 'proc_amount',
            'currency': 'proc_currency',
            'date': 'proc_date',
            'last_4cc': 'proc_last4',  # Renamed from proc_last4digits to proc_last4
            'last_4digits': 'proc_last4',
            'email': 'proc_email',
            'first_name': 'proc_firstname',
            'last_name': 'proc_lastname',
            'processor_name': 'proc_processor_name',
            'tp': 'proc_tp',
            'transaction_id': 'proc_transaction_id',
        }
        for old_col, new_col in rename_map_proc.items():
            if old_col in combined_proc.columns:
                combined_proc.rename(columns={old_col: new_col}, inplace=True)

        # Robust date parsing for proc_date using dateutil.parser to handle all formats
        def parse_mixed_date(date_str):
            if pd.isna(date_str) or str(date_str).strip() == '':
                return pd.NaT
            try:
                dt = dateutil.parser.parse(str(date_str), dayfirst=True)
                return dt  # Keep full datetime to include time
            except Exception:
                return pd.NaT

        if 'proc_date' in combined_proc.columns:
            combined_proc['proc_date'] = combined_proc['proc_date'].apply(parse_mixed_date)
            # Optional: Standardize to string format 'YYYY-MM-DD HH:MM:SS' for consistency
            combined_proc['proc_date'] = combined_proc['proc_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        # Normalize proc_amount to numeric absolute
        if 'proc_amount' in combined_proc.columns:
            combined_proc['proc_amount'] = pd.to_numeric(combined_proc['proc_amount'], errors='coerce').abs()
        if 'proc_last4' in combined_proc.columns:
            combined_proc['proc_last4'] = combined_proc['proc_last4'].apply(clean_last4)

        # Clean proc_tp to remove trailing '.0' if present and strip spaces
        def clean_proc_tp(val):
            if pd.isna(val):
                return ''
            val_str = str(val).strip()
            if val_str.endswith('.0'):
                val_str = val_str[:-2]
            return val_str

        if 'proc_tp' in combined_proc.columns:
            combined_proc['proc_tp'] = combined_proc['proc_tp'].apply(clean_proc_tp)
        # Clean and strip other string columns
        str_cols_proc = ['proc_email', 'proc_firstname', 'proc_lastname', 'proc_processor_name']
        for col in str_cols_proc:
            if col in combined_proc.columns:
                combined_proc[col] = combined_proc[col].fillna('').astype(str).str.strip()
            else:
                combined_proc[col] = ''
        # Define custom column order for processor columns
        custom_proc_columns = [
            'proc_date', 'proc_firstname', 'proc_lastname', 'proc_email', 'proc_tp',
            'proc_amount', 'proc_currency', 'proc_processor_name', 'proc_last4', 'proc_transaction_id'
        ]
        # Reorder columns, using only those that exist
        existing_proc_columns = combined_proc.columns.tolist()
        combined_proc = combined_proc[[col for col in custom_proc_columns if col in existing_proc_columns]]
        out_proc_date_dir = out_proc_dir / date  # Removed regulation.upper()
        out_proc_date_dir.mkdir(parents=True, exist_ok=True)
        combined_proc_path = out_proc_date_dir / f"combined_processor_{transaction_type}s.xlsx"
        combined_proc.to_excel(combined_proc_path, index=False)
        print(f"Combined processor {transaction_type}s saved to {combined_proc_path}")
        print(f"Combined proc columns: {combined_proc.columns.tolist()}")  # Debug print
    else:
        print("No processor files found to combine.")


# ----------------------------
# Append Unmatched
# ----------------------------

def append_unmatched_to_combined(date_str, unmatched_path_str, regulation: str, combined_crm_dir=None):
    """
    Append unmatched shifted deposits to combined CRM deposits.
    """
    combined_crm_dir = combined_crm_dir or config.COMBINED_CRM_DIR
    combined_path = combined_crm_dir / date_str / "combined_crm_deposits.xlsx"  # Removed regulation.upper()
    unmatched_path = Path(unmatched_path_str)
    if not combined_path.exists():
        logging.info(f"Combined CRM file not found: {combined_path}")
        return
    if not unmatched_path.exists():
        logging.info(f"Unmatched shifted deposits file not found: {unmatched_path}")
        return
    df_combined = pd.read_excel(combined_path, dtype={'crm_transaction_id': str})
    df_unmatched = pd.read_excel(unmatched_path, dtype={'crm_transaction_id': str})
    combined_cols = set(df_combined.columns)
    unmatched_cols = set(df_unmatched.columns)
    if combined_cols != unmatched_cols:
        logging.warning(
            f"Column mismatch between combined ({combined_cols}) and unmatched ({unmatched_cols}). Appending anyway.")
    existing_ids = set(df_combined['crm_transaction_id'].dropna().unique())
    if 'crm_transaction_id' in df_unmatched.columns:
        df_unmatched_to_append = df_unmatched[~df_unmatched['crm_transaction_id'].isin(existing_ids)]
    else:
        df_unmatched_to_append = df_unmatched
    logging.info(f"Rows to append after filtering: {df_unmatched_to_append.shape[0]}")
    if not df_unmatched_to_append.empty:
        df_updated = pd.concat([df_combined, df_unmatched_to_append], ignore_index=True)
        if 'crm_date' in df_updated.columns:
            df_updated['crm_date'] = pd.to_datetime(df_updated['crm_date'], errors='coerce').dt.strftime(
                '%m/%d/%Y %I:%M:%S %p')
        key_cols = ['crm_transaction_id', 'crm_tp', 'crm_email', 'crm_amount']
        df_updated = df_updated.drop_duplicates(subset=[col for col in key_cols if col in df_updated.columns])
        logging.info(f"Updated combined_crm_deposits shape after append and dedup: {df_updated.shape}")
        with pd.ExcelWriter(combined_path, engine='openpyxl') as writer:
            df_updated.to_excel(writer, index=False, sheet_name='Sheet1')
            if 'crm_transaction_id' in df_updated.columns:
                worksheet = writer.sheets['Sheet1']
                trans_col = df_updated.columns.get_loc('crm_transaction_id') + 1
                for row in range(2, len(df_updated) + 2):
                    worksheet.cell(row=row, column=trans_col).number_format = '@'
        logging.info(f"Appended unmatched shifted deposits to {combined_path}")
    else:
        logging.info("No new rows to append.")