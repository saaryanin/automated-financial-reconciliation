import pandas as pd
import re
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from src.config import PROCESSED_CRM_DIR, PROCESSED_PROCESSOR_DIR, LISTS_DIR, CRM_DIR, COMBINED_CRM_DIR
from collections import Counter
from dateutil import parser
import dateutil.parser
from src.utils import clean_amount, clean_last4, load_uk_holidays
from src.withdrawals_matcher import ReconciliationEngine  # Import for enhanced_email_similarity
import numpy as np
from datetime import timedelta
import logging

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
    # Add any other known aliases as needed
}



processed_unmatched_files = set()

# ----------------------------

def enhance_email_similarity(e1, e2):
    # Fallback if ReconciliationEngine isn't directly accessible
    from difflib import SequenceMatcher
    e1 = '' if pd.isna(e1) else str(e1).strip().lower()
    e2 = '' if pd.isna(e2) else str(e2).strip().lower()
    if not e1 or not e2:
        return 0.0
    return SequenceMatcher(None, e1.split('@')[0], e2.split('@')[0]).ratio()

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
        allowed_types = ["Express Checkout Payment"]
        df = df[(df["Status"] == "Completed") & (df["Type"].isin(allowed_types)) & (df["Currency"] != "GBP")]
        df = df.rename(columns={"Transaction ID": "transaction_id", "Net": "amount", "From Email Address": "email",
                                "Currency": "currency"})
        df['amount'] = abs(
            df['amount'].astype(str).str.replace(',', '', regex=False).apply(pd.to_numeric, errors='coerce').fillna(0))
        df['date'] = df['Date'].astype(str) + ' ' + df['Time'].astype(str)  # Combine date and time as string
        # Split Name into first_name and last_name
        name_split = df['Name'].astype(str).str.strip().str.split(n=1, expand=True)
        df['first_name'] = name_split[0].fillna('')
        df['last_name'] = name_split[1].fillna('')
        df['processor_name'] = processor
        # Drop unneeded columns
        drop_cols = ["Time zone", "Status", "Fee", "Gross", "To Email Address", "Date", "Time", "Name", "Type"]
        df = df.drop(columns=[col for col in drop_cols if col in df.columns])

    elif processor == "safecharge":
        df = df[(df["Transaction Type"].str.lower() == "sale") & (df["Transaction Result"].str.lower() == "approved")]
        keep_cols = ["Transaction ID", "Date", "Amount", "Currency", "Transaction Type", "Transaction Result", "PAN",
                     "Email Address"]  # Added "Email Address"
        df = df[keep_cols]
        df = df.rename(
            columns={"Transaction ID": "transaction_id", "Date": "date", "Amount": "amount", "Currency": "currency"})
        df['amount'] = abs(pd.to_numeric(df['amount'], errors='coerce').fillna(0))
        df['last_4digits'] = df['PAN'].astype(str).str[-4:].str.zfill(4)  # Extract last 4, zfill handles leading zeros
        df['email'] = df['Email Address'].astype(str).str.strip()  # Added email column
        df['processor_name'] = processor
        df = df.drop(columns=['Transaction Type', 'Transaction Result', 'PAN',
                              'Email Address'])  # Clean up, added 'Email Address' to drop

    elif processor == "powercash":
        df = df[(df["Tx-Type"].str.lower().isin(["capture", "aft"])) & (df["Status"].str.lower() == "successful") & (
            ~df["Currency"].str.upper().isin(["CAD", "GBP"]))]
        df = df[["Tx-Id", "Date", "Time", "Currency", "Amount", "Firstname", "Lastname", "EMail", "Custom 3",
                 "Credit Card Number"]]
        df = df.rename(
            columns={"Tx-Id": "transaction_id", "Amount": "amount", "Currency": "currency", "Firstname": "first_name",
                     "Lastname": "last_name", "EMail": "email"})
        df['amount'] = abs(pd.to_numeric(df['amount'], errors='coerce').fillna(0))
        df['date'] = df['Date'].astype(str) + ' ' + df['Time'].astype(str)  # Combine date and time
        df['tp'] = df['Custom 3'].astype(str).str.split('-').str[0].str.strip()  # Extract tp before '-'
        df['last_4digits'] = df['Credit Card Number'].astype(str).str[-4:].str.zfill(
            4)  # Extract last 4, zfill for leading zeros
        df['processor_name'] = processor
        # Drop unneeded columns (Date and Time after combining, Custom 3 and Credit Card Number after extraction)
        drop_cols = ["Date", "Time", "Custom 3", "Credit Card Number"]
        df = df.drop(columns=[col for col in drop_cols if col in df.columns])

    elif processor == "shift4":
        df = df[(df["Operation Type"].str.lower() == "sale") & (df["Response"].str.lower() == "completed successfully")]
        df = df[["Transaction Date", "Request ID (a1)", "Currency", "Amount", "Card Number", "Card Scheme",
                 "Cardholder Email", "Cardholder Name"]]
        df = df.rename(columns={"Transaction Date": "date", "Request ID (a1)": "transaction_id", "Amount": "amount",
                                "Currency": "currency", "Cardholder Email": "email"})
        df['amount'] = abs(pd.to_numeric(df['amount'], errors='coerce').fillna(0))
        df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.strftime(
            '%Y-%m-%d %H:%M:%S')  # Reformat date to numerical
        df['last_4digits'] = df['Card Number'].astype(str).str[-4:].str.zfill(
            4)  # Extract last 4, zfill for leading zeros
        # Split Cardholder Name into first_name and last_name
        name_split = df['Cardholder Name'].astype(str).str.strip().str.split(n=1, expand=True)
        df['first_name'] = name_split[0].fillna('')
        df['last_name'] = name_split[1].fillna('')
        df['processor_name'] = processor
        # Drop unneeded columns
        drop_cols = ["Card Scheme", "Card Number", "Cardholder Name"]
        df = df.drop(columns=[col for col in drop_cols if col in df.columns])

    elif processor in ["skrill", "neteller"]:
        df = df.rename(columns={
            "Time (CET)": "date", "Time (UTC)": "date",
            "ID of the corresponding Skrill transaction": "transaction_id",
            "ID of the corresponding Neteller transaction": "transaction_id",
            "[+]": "amount", "Currency Sent": "currency"
        })
        df = df[(df["Type"].str.lower() == "receive money") & (df["Status"].str.lower() == "processed") & df[
            "amount"].notna()]
        df = df[~df["Transaction Details"].str.contains("fee", case=False, na=False)]
        df = df[["date", "transaction_id", "amount", "currency", "Transaction Details"]]
        df['amount'] = abs(pd.to_numeric(df['amount'], errors='coerce').fillna(0))
        df['email'] = df['Transaction Details'].astype(str).str.replace(r'^\s*from\s+', '', regex=True,
                                                                        case=False).str.strip()
        df['date'] = df['date'].apply(
            lambda x: parser.parse(str(x)).strftime('%m/%d/%Y %I:%M:%S %p') if pd.notna(x) else '')
        df['processor_name'] = processor
        df = df.drop(columns=['Transaction Details'])

    elif processor == "trustpayments":
        df = df[(df["errorcode"] == 0) & (df["requesttypedescription"].str.upper() == "AUTH")]
        df = df[df["currencyiso3a"].str.upper().isin(["USD", "EUR"])].copy()
        df = df.rename(columns={
            "transactionreference": "transaction_id",
            "transactionstartedtimestamp": "date",
            "mainamount": "amount",
            "currencyiso3a": "currency"
        })
        df = df[["transaction_id", "billingfullname", "date", "currency", "amount", "maskedpan",
                 "orderreference"]]  # Removed "paymenttypedescription"
        df['amount'] = abs(pd.to_numeric(df['amount'], errors='coerce').fillna(0))
        # Split billingfullname into first_name and last_name
        name_split = df['billingfullname'].astype(str).str.strip().str.split(n=1, expand=True)
        df['first_name'] = name_split[0].fillna('')
        df['last_name'] = name_split[1].fillna('')
        df['last_4digits'] = df['maskedpan'].astype(str).str[-4:].str.zfill(
            4)  # Extract last 4, zfill for leading zeros
        df['tp'] = df['orderreference'].astype(str).str.split('-').str[0].str.strip()  # Extract tp before '-'
        df['processor_name'] = processor
        # Drop unneeded columns
        drop_cols = ["billingfullname", "maskedpan", "orderreference"]
        df = df.drop(columns=[col for col in drop_cols if col in df.columns])

    elif processor == "zotapay":
        df = df.copy()
        df.columns = df.iloc[0].str.strip()
        df = df.iloc[1:]
        df = df[(df["Type"].str.upper() == "SALE") & (df["Status"].str.lower() == "approved")]
        df = df.rename(columns={
            "ID": "transaction_id",
            "Order Currency": "currency",
            "Order Amount": "amount",
            "Created At": "date",  # Will be dropped later
            "Ended At": "date",  # Renamed to date (overwrites if conflict, but since dropping old date)
            "Customer Email": "email",
            "Customer First Name": "first_name",
            "Customer Last Name": "last_name"
        })
        keep_cols = [
            "transaction_id", "currency", "amount", "Merchant Order Description",
            "date", "email", "first_name", "last_name"  # Removed Type, Status, Payment Method; kept Ended At as date
        ]
        df = df[keep_cols]
        df['amount'] = abs(pd.to_numeric(df['amount'].astype(str).str.replace(',', ''), errors='coerce').fillna(
            0))  # Ensure numeric, handle strings/commas
        df['tp'] = df['Merchant Order Description'].astype(str).str.split('-').str[
            0].str.strip()  # Extract tp before '-'
        df['processor_name'] = processor
        df = df.drop(columns=['Merchant Order Description'])  # Drop after extraction

    elif processor == "bitpay":
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
        df = df[df["tx_type"].str.lower() == "sale"]
        df = df.rename(columns={
            "invoice_id": "transaction_id",
            "payout_amount": "amount",
            "payout_currency": "currency",
            "buyeremail": "email"
        })
        keep_cols = [
            "date", "time", "transaction_id", "amount",
            "currency", "buyername", "email"
        ]
        df = df[keep_cols]
        df['amount'] = abs(pd.to_numeric(df['amount'], errors='coerce').fillna(0))
        df['date'] = df['date'].astype(str) + ' ' + df['time'].astype(str)  # Combine date and time
        # Split buyername into first_name and last_name
        name_split = df['buyername'].astype(str).str.strip().str.split(n=1, expand=True)
        df['first_name'] = name_split[0].fillna('')
        df['last_name'] = name_split[1].fillna('')
        df['processor_name'] = processor
        # Drop unneeded columns
        drop_cols = ["time", "buyername"]
        df = df.drop(columns=[col for col in drop_cols if col in df.columns])

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
        df['amount'] = abs(pd.to_numeric(df['amount'], errors='coerce').fillna(0))
        df['tp'] = df['transaction_id'].astype(str).str.split('-').str[
            0].str.strip()  # Extract tp from transaction_id before '-'
        df['currency'] = 'MYR'  # Always set to MYR
        df['processor_name'] = processor

    elif processor == "paymentasia":
        df = df[(df["Type"].str.upper() == "SALE") & (df["Status"].str.upper() == "SUCCESS")]
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

    # Common cleanup for all processors
    if 'transaction_id' in df:
        df['transaction_id'] = df['transaction_id'].astype(str).str.strip().fillna('UNKNOWN')

    return df.reset_index(drop=True)


def patch_standardize_zotapay_paymentasia_withdrawals(df, processor):
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
        df["Type"] = df["Type"].astype(str).str.strip()
        df["Status"] = df["Status"].astype(str).str.strip()
        df["To Email Address"] = df["To Email Address"].astype(str).str.strip().str.lower()
        df["Gross"] = df["Gross"].astype(str).str.replace(",", "", regex=False)
        df["Currency"] = df["Currency"].astype(str).str.strip()

        # Pull all types: withdrawals, refunds, reversals
        allowed_types = ["Mass Payment", "Payment Refund", "Mass Pay Reversal"]
        allowed_status = ["Completed", "Unclaimed"]
        df = df[
            df["Type"].isin(allowed_types) &
            df["Status"].isin(allowed_status) &
            (df["Currency"] != "GBP")
            ].copy()
        if df.empty:
            return pd.DataFrame()

        # Remove both 'Mass Pay Reversal' and its matching 'Mass Payment' or 'Payment Refund'
        to_remove = set()
        mpr_rows = df[df["Type"] == "Mass Pay Reversal"]
        for idx_mpr, row_mpr in mpr_rows.iterrows():
            email = row_mpr["To Email Address"]
            try:
                amount = float(row_mpr["Gross"])
            except Exception:
                continue
            currency = row_mpr["Currency"]
            # Find matching Mass Payment or Payment Refund
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
                break  # Only remove first found

        df = df.drop(index=list(to_remove))

        # Only keep real withdrawals (Mass Payment, Payment Refund)
        keep_mask = df["Type"].isin(["Mass Payment", "Payment Refund"])
        df = df[keep_mask]
        if df.empty:
            return pd.DataFrame()

        # Standardize schema
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
            if isinstance(name_split, pd.DataFrame) and name_split.shape[1] >= 1:
                df["first_name"] = name_split[0]
                df["last_name"] = name_split[1] if name_split.shape[1] > 1 else ""
            else:
                df["first_name"] = ""
                df["last_name"] = ""
        else:
            df["first_name"] = ""
            df["last_name"] = ""
        return df[[
            "amount", "currency", "date", "last_4cc",
            "email", "first_name", "last_name", "processor_name"
        ]]


    elif processor.lower() == "safecharge":
        df.columns = df.columns.str.strip()
        colmap = {col.lower().replace(" ", ""): col for col in df.columns}

        if 'transactiontype' not in colmap or 'transactionresult' not in colmap:
            return pd.DataFrame()

        credit_type = "Credit"
        void_type = "VoidCredit"
        email_col = colmap.get('emailaddress', None)
        pan_col = colmap.get('pan', None)

        # 1. Pull all Credit and VoidCredit rows, status Approved
        df = df[
            df[colmap['transactionresult']].str.strip().str.lower() == "approved"
            ].copy()
        df = df[
            df[colmap['transactiontype']].isin([credit_type, void_type])
        ].copy()

        if df.empty:
            return pd.DataFrame()

        # 2. Cancel both VoidCredit and its paired Credit row (search up and down)
        to_remove = set()
        df = df.reset_index(drop=True)  # Ensures index is 0...N
        void_rows = df[df[colmap['transactiontype']] == "VoidCredit"]

        for void_idx, void_row in void_rows.iterrows():
            void_email = str(void_row[email_col]).strip().lower() if email_col else ""
            void_last4 = str(void_row[pan_col])[-4:] if pan_col else ""
            void_amount = float(void_row[colmap['amount']])
            void_currency = str(void_row[colmap['currency']]).upper()

            found = None
            # Search above first (older rows)
            for i in range(void_idx - 1, -1, -1):
                credit_row = df.iloc[i]
                if (
                        credit_row[colmap['transactiontype']] == "Credit" and
                        str(credit_row[email_col]).strip().lower() == void_email and
                        str(credit_row[pan_col])[-4:] == void_last4 and
                        float(credit_row[colmap['amount']]) == void_amount and
                        str(credit_row[colmap['currency']]).upper() == void_currency and
                        i not in to_remove
                ):
                    found = i
                    break
            # If not found above, search below
            if found is None:
                for i in range(void_idx + 1, len(df)):
                    credit_row = df.iloc[i]
                    if (
                            credit_row[colmap['transactiontype']] == "Credit" and
                            str(credit_row[email_col]).strip().lower() == void_email and
                            str(credit_row[pan_col])[-4:] == void_last4 and
                            float(credit_row[colmap['amount']]) == void_amount and
                            str(credit_row[colmap['currency']]).upper() == void_currency and
                            i not in to_remove
                    ):
                        found = i
                        break
            to_remove.add(void_idx)
            if found is not None:
                to_remove.add(found)

        df = df.drop(index=list(to_remove))

        # 3. Now filter to just "Credit" for final withdrawals output
        df = df[df[colmap['transactiontype']] == credit_type]

        # --- Standardize columns as before ---
        df = df.rename(columns={
            colmap['amount']: "amount",
            colmap['currency']: "currency",
            colmap['date']: "date",
            email_col: "email" if email_col else "email",
            pan_col: "last_4cc" if pan_col else "last_4cc"
        })
        df["last_4cc"] = df["last_4cc"].astype(str).str.extract(r"(\d{4})$") if pan_col else ""
        df["currency"] = df["currency"].replace({"Euro": "EUR", "US Dollar": "USD", "Canadian Dollar": "CAD", "Australian Dollar": "AUD"})
        df["processor_name"] = "safecharge"
        df["first_name"] = ""
        df["last_name"] = ""
        return df[[
            "amount", "currency", "date", "last_4cc", "email",
            "first_name", "last_name", "processor_name"
        ]]

    elif processor.lower() == "powercash":
        # — filter to only refunds or CFTs, successful EUR/USD rows
        df.columns = df.columns.str.strip()
        df = df[
            df["Tx-Type"].str.lower().isin(["refund", "cft"]) &
            (df["Status"].str.lower() == "successful") &
            (df["Currency"].str.upper().isin(["EUR", "USD"]))
        ]
        if df.empty:
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
        df.columns = df.columns.str.strip()
        df["Operation Type"] = df["Operation Type"].astype(str).str.strip().str.lower()
        df["Response"] = df["Response"].astype(str).str.strip().str.lower()
        df["Cardholder Email"] = df["Cardholder Email"].astype(str).str.strip().str.lower()
        df["Card Number"] = df["Card Number"].astype(str).str.strip()

        # Pull all withdrawals and voids first
        relevant_mask = df["Operation Type"].isin(["referral credit", "sale void", "refund void"]) & (
                    df["Response"] == "completed successfully")
        df = df[relevant_mask].copy()

        if df.empty:
            return pd.DataFrame()

        # Remove both 'refund void' and its matching 'referral credit' (same email+amount+currency)
        to_remove = set()
        refund_voids = df[df["Operation Type"] == "refund void"]

        for idx_rv, refund_row in refund_voids.iterrows():
            email = refund_row["Cardholder Email"]
            amount = clean_amount(refund_row["Amount"])
            currency = refund_row["Currency"]

            # Scan for matching referral credit rows (above/below), same email/amount/currency
            mask = (
                    (df["Operation Type"] == "referral credit") &
                    (df["Cardholder Email"] == email) &
                    (df["Currency"] == currency) &
                    (df["Amount"].apply(clean_amount) == amount)
            )
            possible_matches = df[mask]

            # Remove first match (if any)
            for idx_ref in possible_matches.index:
                to_remove.add(idx_rv)
                to_remove.add(idx_ref)
                break
        df = df.drop(index=list(to_remove))

        # Only keep real withdrawals ("referral credit" or "sale void")
        keep_mask = df["Operation Type"].isin(["referral credit", "sale void"])
        df = df[keep_mask]

        if df.empty:
            return pd.DataFrame()

        # Standardize schema
        df = df[[
            "Transaction Date", "Card Number", "Currency", "Amount", "Cardholder Name", "Cardholder Email"
        ]].copy()

        df = df.rename(columns={
            "Transaction Date": "date",
            "Currency": "currency",
            "Amount": "amount",
            "Cardholder Email": "email"
        })

        df["amount"] = df["amount"].apply(clean_amount)
        df["last_4cc"] = df["Card Number"].astype(str).str.extract(r"(\d{4})$").fillna("")
        name_split = df["Cardholder Name"].astype(str).str.split(n=1, expand=True)
        df["first_name"] = name_split[0].str.rstrip("*")
        df["last_name"] = name_split[1].str.rstrip("*") if name_split.shape[1] > 1 else ""
        df["processor_name"] = "shift4"
        return df[[
            "amount", "currency", "date", "last_4cc", "email",
            "first_name", "last_name", "processor_name"
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
            return pd.DataFrame()

        # pick whichever column holds the amount
        amt_col = "Amount Sent" if "Amount Sent" in df.columns else "[+]"
        df = df.loc[
             df[amt_col].notna() & df[amt_col].astype(str).str.strip().ne(""),
             :
             ]
        if df.empty:
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

    elif processor.lower() == "trustpayments":
        # Filter to only REFUND type and errorcode == 0
        df = df[
            (df["requesttypedescription"].str.upper() == "REFUND") &
            (df["errorcode"] == 0)
            ].copy()
        df = df[df["currencyiso3a"].str.upper().isin(["USD", "EUR"])].copy()
        if df.empty:
            return pd.DataFrame()

        def split_billingfullname(name):
            if pd.isna(name):
                return "", ""
            parts = str(name).strip().split(" ", 1)
            first = parts[0]
            last = parts[1] if len(parts) > 1 else ""
            return first, last
        df["first_name"], df["last_name"] = zip(*df["billingfullname"].apply(split_billingfullname))
        df["date"] = pd.to_datetime(df["transactionstartedtimestamp"], errors="coerce")
        df["last_4cc"] = df["maskedpan"].astype(str).str[-4:]
        df["currency"] = df["currencyiso3a"]
        df["amount"] = pd.to_numeric(df["mainamount"], errors="coerce")
        # --- Robustly clean TP as str, remove decimals, strip spaces ---
        df["tp"] = df["orderreference"].astype(str).str.split("-").str[0].str.strip().str.replace(r"\.0$", "",
                                                                                                  regex=True)
        # Remove any accidental non-digit chars, just in case
        df["tp"] = df["tp"].str.replace(r"[^\d]", "", regex=True)
        df["email"] = ""
        df["processor_name"] = "trustpayments"
        keep = [
            "amount", "currency", "date", "last_4cc", "email",
            "first_name", "last_name", "processor_name", "tp"
        ]
        return df[keep]

    return pd.DataFrame()


# def handle_withdrawal_cancellations(df):
#     if "Name" not in df.columns:
#         return df
#     # Normalize columns for matching
#     for col in ['tp', 'CC Last 4 Digits', 'Email (Account) (Account)', 'Amount', 'Name']:
#         if col in df.columns:
#             df[col] = df[col].fillna('').astype(str).str.strip()
#     df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce').fillna(0)
#     mask_cancel = df["Name"].str.lower().str.replace(' ', '', regex=False) == "withdrawalcancelled"
#     mask_withdrawal = df["Name"].str.lower() == "withdrawal"
#     cancels = df[mask_cancel].copy()
#     withdrawals = df[mask_withdrawal].copy()
#     to_drop = set()
#     for idx_cancel, row_cancel in cancels.iterrows():
#         tp_cancel = str(row_cancel["tp"]).strip()
#         last4_cancel = clean_last4(row_cancel["CC Last 4 Digits"])
#         email_cancel = str(row_cancel["Email (Account) (Account)"]).lower().str.strip()
#         amt_cancel = pd.to_numeric(row_cancel["Amount"], errors='coerce')
#         # Match on TP, last4, email
#         matched = withdrawals[
#             (withdrawals["tp"].astype(str).str.strip() == tp_cancel) &
#             (withdrawals["CC Last 4 Digits"].apply(clean_last4) == last4_cancel) &
#             (withdrawals["Email (Account) (Account)"].astype(str).str.lower().str.strip() == email_cancel)
#         ]
#         if matched.empty:
#             # Unmatched cancellation: make amount positive and keep as 'withdrawal cancelled'
#             df.at[idx_cancel, "Amount"] = abs(amt_cancel)
#             df.at[idx_cancel, "Name"] = "Withdrawal Cancelled"  # Ensure crm_type stays
#             continue
#         # For matched, check if absolute amounts match
#         for idx_withdrawal, row_withdrawal in matched.iterrows():
#             amt_withdrawal = pd.to_numeric(row_withdrawal["Amount"], errors='coerce')
#             if abs(abs(amt_cancel) - abs(amt_withdrawal)) < 1e-6:
#                 to_drop.update([idx_cancel, idx_withdrawal])
#                 break
#             else:
#                 logging.info(f"Matched on keys but amounts don't match magnitude: cancel={abs(amt_cancel)}, withdrawal={abs(amt_withdrawal)}")
#     # Drop the matched pairs
#     df = df.drop(index=list(to_drop))
#     return df

# ----------------------------
# CRM Handling
# ----------------------------

def extract_crm_transaction_id(comment: str, processor: str):
    text = str(comment)
    processor = processor.lower()
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
    }
    pattern = patterns.get(processor)
    if not pattern:
        return None
    match = re.search(pattern, text)
    return next((g for g in match.groups() if g), None) if match else None

def clean_crm_amount(amt):
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

def load_crm_file(filepath: str, processor_name: str, save_clean=False, transaction_type="deposit") -> pd.DataFrame:
    # Define normalized_processor at the start
    normalized_processor = processor_name.lower()
    # Calculate relevant previous date based on the current day
    date_str = extract_date_from_filename(filepath)
    current_date = datetime.strptime(date_str, '%Y-%m-%d')
    previous_date = current_date - timedelta(days=1) # Previous day for unmatched data
    previous_date_str = get_previous_business_day(date_str)
    # Load current raw CRM file
    df = pd.read_excel(filepath, engine="openpyxl")
    df.columns = df.columns.str.strip()
    # Categorize regulation
    def categorize_regulation(site):
        site = str(site).lower().strip()
        if site in ['fortrade.by', 'gcmasia by', 'kapitalrs by']:
            return 'belarus'
        elif site in ['kapitalrs au', 'fortrade.au', 'gcmasia asic']:
            return 'australia'
        elif site in ['fortrade.eu', 'gcmforex', 'gcmasia fsc', 'fortrade fsc', 'kapitalrs fsc']:
            return 'mauritius'
        elif site == 'fortrade.ca':
            return 'canada'
        elif site == 'fortrade.cy':
            return 'cyprus'
        return 'unknown'
    df['regulation'] = df['Site (Account) (Account)'].apply(categorize_regulation)
    # Filter out paypal and inpendium for australia regulation
    mask_aus = df['regulation'] == 'australia'
    mask_psp = df["PSP name"].str.lower().isin(['paypal', 'inpendium'])
    df = df[~(mask_aus & mask_psp)]
    # Load and process unmatched_shifted_deposits from the previous day
    previous_unmatched_path = LISTS_DIR / previous_date_str / "unmatched_shifted_deposits.xlsx"
    if previous_unmatched_path.exists():
        unmatched_df = pd.read_excel(previous_unmatched_path, dtype={'crm_transaction_id': str})
        logging.info(
            f"Loaded unmatched_shifted_deposits from {previous_unmatched_path} with columns: {unmatched_df.columns.tolist()}")
        # Expanded mapping based on sample unmatched file
        mapping = {
            'Name': 'crm_type',
            'Created On': 'crm_date',
            'First Name (Account) (Account)': 'crm_firstname',
            'Last Name (Account) (Account)': 'crm_lastname',
            'Email (Account) (Account)': 'crm_email',
            'Amount': 'crm_amount',
            'Currency': 'crm_currency',
            'Approved': 'crm_approved',
            'Approved On': 'crm_approved_on', # Optional
            'TP Account': 'crm_tp',
            'Internal Comment': 'internal_comment',
            'Method of Payment': 'payment_method',
            'Internal Type': 'internal_type', # Optional
            'Site (Account) (Account)': 'regulation',
            'Country Of Residence (Account) (Account)': 'country_of_residence', # Optional
            'PSP name': 'crm_processor_name',
            'CC Last 4 Digits': 'crm_last4'
        }
        unmatched_mapped = unmatched_df.rename(columns=mapping, errors='ignore')
        # Log columns after mapping
        logging.debug(f"Columns in unmatched_mapped after mapping: {unmatched_mapped.columns.tolist()}")
        # Normalize crm_processor_name
        if 'crm_processor_name' in unmatched_mapped.columns:
            unmatched_mapped['crm_processor_name'] = unmatched_mapped['crm_processor_name'].astype(
                str).str.strip().str.lower().replace(PSP_NAME_MAP)
        else:
            logging.warning(f"'crm_processor_name' column missing in {previous_unmatched_path}")
            unmatched_mapped['crm_processor_name'] = 'unknown'
        # Extract transaction_id from internal_comment using per-row processor
        if 'internal_comment' in unmatched_mapped.columns and 'crm_processor_name' in unmatched_mapped.columns:
            unmatched_mapped['crm_transaction_id'] = unmatched_mapped.apply(
                lambda row: extract_crm_transaction_id(row['internal_comment'], row['crm_processor_name'])
                if pd.notna(row['internal_comment']) and pd.notna(row['crm_processor_name']) else None,
                axis=1
            )
            unmatched_mapped['crm_transaction_id'] = unmatched_mapped['crm_transaction_id'].astype(str).fillna(
                'UNKNOWN')
        else:
            logging.warning(
                f"Internal Comment or crm_processor_name column not found in {previous_unmatched_path}, columns available: {unmatched_mapped.columns.tolist()}")
            unmatched_mapped['crm_transaction_id'] = 'UNKNOWN'
        # Log for debugging
        for idx, row in unmatched_mapped.iterrows():
            if row['crm_transaction_id'] == 'UNKNOWN' and pd.notna(row.get('internal_comment')):
                logging.debug(
                    f"Failed to extract transaction_id for internal_comment: {row['internal_comment']}, processor: {row.get('crm_processor_name', 'N/A')}")
        # Define required columns for final output
        required_columns = ['crm_date', 'crm_firstname', 'crm_lastname', 'crm_email', 'crm_tp', 'crm_amount',
                            'crm_currency', 'payment_method', 'crm_approved', 'crm_processor_name', 'crm_last4',
                            'regulation', 'crm_transaction_id', 'crm_type']
        for col in required_columns:
            if col not in unmatched_mapped.columns:
                unmatched_mapped[col] = pd.NA
        unmatched_mapped = unmatched_mapped[required_columns]
        # Convert data types
        if 'crm_date' in unmatched_mapped.columns:
            unmatched_mapped['crm_date'] = pd.to_datetime(unmatched_mapped['crm_date'], errors='coerce').fillna(
                pd.NaT).dt.strftime('%m/%d/%Y %I:%M:%S %p')
        for col in ['crm_amount', 'crm_last4']:
            if col in unmatched_mapped.columns:
                unmatched_mapped[col] = pd.to_numeric(unmatched_mapped[col], errors='coerce').fillna(0)
        # Standardize crm_currency
        if 'crm_currency' in unmatched_mapped.columns:
            unmatched_mapped['crm_currency'] = unmatched_mapped['crm_currency'].replace({
                'US Dollar': 'USD',
                'Euro': 'EUR'
            })
        # Convert crm_approved to 1/0
        if 'crm_approved' in unmatched_mapped.columns:
            unmatched_mapped['crm_approved'] = unmatched_mapped['crm_approved'].str.strip().str.lower().map(
                {'yes': 'Yes', 'no': 'No'}).fillna(0)
        # Categorize regulation
        if 'regulation' in unmatched_mapped.columns:
            unmatched_mapped['regulation'] = unmatched_mapped['regulation'].apply(categorize_regulation)
        # Append to current CRM if new transactions
        existing_transaction_ids = set(df['Internal Comment'].apply(
            lambda x: extract_crm_transaction_id(x, normalized_processor) if pd.notna(x) else None).dropna().unique())
        new_deposits = unmatched_mapped[~unmatched_mapped['crm_transaction_id'].isin(existing_transaction_ids)]
        if not new_deposits.empty:
            for col in df.columns:
                if col not in new_deposits.columns:
                    new_deposits[col] = pd.NA
            for col in new_deposits.columns:
                if col not in df.columns:
                    df[col] = pd.NA
            df = pd.concat([df, new_deposits], ignore_index=True)
            logging.info(f"Added {len(new_deposits)} new unmatched deposits from {previous_date_str}")
        # Save the processed unmatched data
        if save_clean:
            unmatched_out_path = PROCESSED_CRM_DIR / "unmatched_shifted_deposits" / date_str / "unmatched_shifted_deposits.xlsx"
            unmatched_out_path.parent.mkdir(parents=True, exist_ok=True)
            unmatched_mapped.to_excel(unmatched_out_path, index=False)
            logging.info(f"Saved unmatched deposits to {unmatched_out_path}")
    df["PSP name"] = (
        df["PSP name"]
        .astype(str)
        .str.strip()
        .str.lower()
        .replace(PSP_NAME_MAP)
    )
    df["tp"] = df["TP Account"] if "TP Account" in df.columns else ""
    df['transaction_id'] = df['Internal Comment'].apply(lambda c: extract_crm_transaction_id(c, normalized_processor))
    df['transaction_id'] = df['transaction_id'].astype(str).fillna('UNKNOWN')
    if normalized_processor in ["zotapay", "paymentasia"]:
        name_has_chinese = (
                df["First Name (Account) (Account)"].astype(str).str.contains(r'[\u4e00-\u9fff]') |
                df["Last Name (Account) (Account)"].astype(str).str.contains(r'[\u4e00-\u9fff]')
        )
        name_col_match = df["Name"].str.lower() == "withdrawal"
        psp_match = df["PSP name"].str.contains("pamy|zotapay|wire withdrawal", case=False, na=False)
        method_match = df["Method of Payment"].astype(str).str.contains("paymentasia|zotapay-cup|PA-MY", case=False, na=False)
        full_mask = name_col_match & (psp_match | method_match)
        df = df[full_mask].reset_index(drop=True)
    else:
        psp_mask = df["PSP name"] == normalized_processor
        if transaction_type == "withdrawal":
            name_mask = df["Name"].str.lower() == "withdrawal"
            df = df[name_mask & psp_mask].reset_index(drop=True)

        else:
            df = df[(df["Name"].str.lower() == transaction_type) & psp_mask].reset_index(drop=True)
    if "Currency" in df.columns:
        df["Currency"] = df["Currency"].replace({
            "Euro": "EUR",
            "US Dollar": "USD",
            "Canadian Dollar": "CAD",
            "Australian Dollar": "AUD"
        })
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
            "Site (Account) (Account)"
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
        df['crm_approved'] = df['Approved'].str.strip().str.lower().map({'yes': 1, 'no': 0}).fillna(0) if 'Approved' in df.columns else pd.Series(0, index=df.index)
        df['crm_transaction_id'] = df['transaction_id'].fillna('UNKNOWN')
    if save_clean:
        date_str = extract_date_from_filename(filepath)
        # Save processed CRM file
        folder_name = "zotapay_paymentasia" if normalized_processor in ["zotapay", "paymentasia"] else normalized_processor
        folder = f"{folder_name}_{transaction_type}s.xlsx"
        out_path = PROCESSED_CRM_DIR / folder_name / date_str / folder
        out_path.parent.mkdir(parents=True, exist_ok=True)
        # If df is empty after filtering, create an empty DataFrame with the needed columns
        if df.empty:
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
            ] if transaction_type == "deposit" else [
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
                "Site (Account) (Account)"
            ]
            df = pd.DataFrame(columns=[col for col in needed_columns])
            logging.info(f"Creating empty processed CRM file for {normalized_processor} {transaction_type}s")
        with pd.ExcelWriter(out_path, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Sheet1')
            if "transaction_id" in df.columns and transaction_type == "deposit":
                worksheet = writer.sheets['Sheet1']
                trans_col = df.columns.get_loc('transaction_id') + 1
                for row in range(2, len(df) + 2):
                    worksheet.cell(row=row, column=trans_col).number_format = '@'
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

def get_previous_business_day(current_date_str):
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
        "ID of the corresponding Neteller transaction": str,
        'Pan':str,
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
        else:
            print(f" Not saving empty {processor_name} {transaction_type}s")

    return df_clean

# Updated combine_processed_files to read with dtype for transaction_id
def combine_processed_files(
    date, processors, processor_name=None, # Added processor_name as optional parameter
    processed_crm_dir=PROCESSED_CRM_DIR,
    processed_proc_dir=PROCESSED_PROCESSOR_DIR,
    out_crm_dir=None,
    out_proc_dir=None,
    transaction_type="withdrawal",
    exchange_rate_map=None,
    extra_processors=None
):
    if extra_processors is None:
        extra_processors = []
    all_processors = list(processors) + list(extra_processors)
    if out_crm_dir is None:
        out_crm_dir = processed_crm_dir / "combined"
    if out_proc_dir is None:
        out_proc_dir = processed_proc_dir / "combined"
    # Define current_date and normalized_processor locally
    current_date = datetime.strptime(date, '%Y-%m-%d')
    normalized_processor = processor_name.lower() if processor_name else 'crm'
    crm_dfs, proc_dfs = [], []
    crm_file_template = f"{{}}_{transaction_type}s.xlsx"
    proc_file_template = f"{{}}_{transaction_type}s.xlsx"
    # Load other processed CRM files
    for proc in all_processors:
        crm_f = processed_crm_dir / proc / date / f"{proc}_{transaction_type}s.xlsx"
        if crm_f.exists():
            df = pd.read_excel(crm_f, dtype={'transaction_id': str} if transaction_type == "deposit" else None)
            crm_dfs.append(df)
        else:
            print(f" CRM processed file not found for {proc}: {crm_f}")
    for proc in all_processors: # Only original processors for processor files
        proc_f = processed_proc_dir / proc / date / proc_file_template.format(proc)
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
                    'First Name (Account) (Account)', 'Last Name (Account) (Account)']:
            if col in df.columns:
                df[col] = df[col].fillna('').astype(str).str.strip().str.lower()
        if 'Currency' not in df.columns:
            df['Currency'] = 'USD'
        if 'Amount' not in df.columns:
            df['Amount'] = 0.0
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
                group_cols = base_cols
            else:
                group_cols = base_cols + ['PSP name']
            for keys, sub_group in group.groupby(group_cols):
                sub_group = sub_group[sub_group['Amount'].notna() & sub_group['Currency'].notna()]
                if sub_group.empty:
                    continue
                currencies = sub_group['Currency'].tolist()
                tgt_cur = choose_target_currency(currencies)
                amounts = []
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
                            converted = amt
                    amounts.append(converted)
                total_amt = sum(amounts)
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
                row0['Amount'] = abs(total_amt)
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
            if 'last_4cc' in group.columns and (group['last_4cc'].astype(str).str.strip() != '').any():
                # Group by last_4cc first
                for last4, subg in group.groupby('last_4cc', dropna=False):
                    if subg.empty or pd.isna(last4) or str(last4).strip() == '':
                        continue
                    emails = subg['email'].dropna().tolist()
                    if len(emails) <= 1:
                        grouped_rows.append(subg.iloc[0].copy())
                        continue
                    # Check email similarity for all pairs
                    high_similar = []
                    for i, email1 in enumerate(emails):
                        for j, email2 in enumerate(emails[i + 1:], i + 1):
                            sim = enhance_email_similarity(email1, email2)
                            if sim >= 0.8:
                                high_similar.append((i, j, sim))
                    if high_similar:
                        # Aggregate rows with high similarity
                        unique_rows = set()
                        for i, j, _ in high_similar:
                            unique_rows.add(i)
                            unique_rows.add(j)
                        agg_row = subg.iloc[0].copy()
                        agg_row['amount'] = subg.loc[subg.index[list(unique_rows)], 'amount'].sum()
                        agg_row['email'] = list(set([emails[i] for i in unique_rows])) # Use set to remove duplicates
                        agg_row['currency'] = choose_target_currency(subg['currency'].tolist())
                        grouped_rows.append(agg_row)
                        # Keep non-aggregated rows
                        for idx in subg.index:
                            if idx not in [subg.index[i] for i in unique_rows]:
                                grouped_rows.append(subg.loc[idx].copy())
                    else:
                        grouped_rows.extend([row.copy() for _, row in subg.iterrows()])
            else:
                # Fallback grouping if no last_4cc
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
        combined_crm = pd.concat(crm_dfs, ignore_index=True)
        if transaction_type == "withdrawal":
            from src.config import CRM_DIR
            raw_crm_path = CRM_DIR / f"crm_{date}.xlsx"
            if raw_crm_path.exists():
                df_raw = pd.read_excel(raw_crm_path, engine="openpyxl")
                df_raw.columns = df_raw.columns.str.strip()
                cancel_mask = df_raw["Name"].astype(str).str.strip().str.lower() == "withdrawal cancelled"
                df_cancels = df_raw[cancel_mask].copy()

                # The part of the code where it filters the rows that have balue in the PSP name column is commented because there might be scenarios when there is a value there
                # cancel_psp_na = df_cancels["PSP name"].isna() | (df_cancels["PSP name"].str.strip() == "")
                # df_cancels = df_cancels[cancel_psp_na]

                # Exclude cancellations with 'Wire Transfer' in Method of Payment
                if 'Method of Payment' in df_cancels.columns:
                    df_cancels = df_cancels[~df_cancels['Method of Payment'].astype(str).str.strip().str.lower().eq('wire transfer')]
                def categorize_regulation(site):
                    site = str(site).lower().strip()
                    if site in ['fortrade.by', 'gcmasia by', 'kapitalrs by']:
                        return 'belarus'
                    elif site in ['kapitalrs au', 'fortrade.au', 'gcmasia asic']:
                        return 'australia'
                    elif site in ['fortrade.eu', 'gcmforex', 'gcmasia fsc', 'fortrade fsc', 'kapitalrs fsc']:
                        return 'mauritius'
                    elif site == 'fortrade.ca':
                        return 'canada'
                    elif site == 'fortrade.cy':
                        return 'cyprus'
                    return 'unknown'
                df_cancels['regulation'] = df_cancels['Site (Account) (Account)'].apply(categorize_regulation)
                mask_aus = df_cancels['regulation'] == 'australia'
                mask_psp = df_cancels["PSP name"].str.lower().isin(['paypal', 'inpendium'])
                df_cancels = df_cancels[~(mask_aus & mask_psp)]
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
                    "Site (Account) (Account)"
                ]
                df_cancels = df_cancels[[col for col in needed_columns if col in df_cancels.columns]]
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
            'Method of Payment': 'payment_method', # Renamed
            'Approved': 'crm_approved',
            'PSP name': 'crm_processor_name',
            'CC Last 4 Digits': 'crm_last4',
            'Site (Account) (Account)': 'regulation',
            'transaction_id': 'crm_transaction_id',
            'Name': 'crm_type' # Keep for traceability, can remove if unwanted
        }
        # Rename columns and remove duplicates
        for old_col, new_col in rename_map.items():
            if old_col in combined_crm.columns:
                combined_crm[new_col] = combined_crm[old_col] # Overwrite with new name
                if old_col != new_col: # Only drop if different to avoid self-drop
                    combined_crm = combined_crm.drop(columns=[old_col], errors='ignore')
        combined_crm = combined_crm.loc[:, ~combined_crm.columns.duplicated()] # Ensure no duplicates
        if 'Approved' in combined_crm.columns:
            combined_crm = combined_crm.drop(columns=['Approved'])
        # Remove unwanted columns if present
        unwanted_columns = [
            '(Do Not Modify) Monetary Transaction', '(Do Not Modify) Row Checksum', '(Do Not Modify) Modified On',
            'Approved On', 'TP Account', 'Internal Comment', 'Internal Type', 'Country Of Residence (Account) (Account)'
        ]
        combined_crm = combined_crm.drop(columns=[col for col in unwanted_columns if col in combined_crm.columns], errors='ignore')
        def categorize_regulation(site):
            site = str(site).lower().strip()
            if site in ['fortrade.by', 'gcmasia by', 'kapitalrs by']:
                return 'belarus'
            elif site in ['kapitalrs au', 'fortrade.au', 'gcmasia asic']:
                return 'australia'
            elif site in ['fortrade.eu', 'gcmforex', 'gcmasia fsc', 'fortrade fsc', 'kapitalrs fsc']:
                return 'mauritius'
            elif site == 'fortrade.ca':
                return 'canada'
            elif site == 'fortrade.cy':
                return 'cyprus'
            return 'unknown'
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
            f"Debug: combined_crm type: {type(combined_crm)}, shape: {combined_crm.shape}, columns: {existing_columns}") # Debug
        # Append any additional columns not in base
        custom_columns = base_columns + [col for col in existing_columns if col not in base_columns]
        # Reorder columns, using only those that exist
        combined_crm = combined_crm[[col for col in custom_columns if col in existing_columns]]
        out_crm_date_dir = out_crm_dir / date
        out_crm_date_dir.mkdir(parents=True, exist_ok=True)
        combined_crm_path = out_crm_date_dir / f"combined_crm_{transaction_type}s.xlsx"
        with pd.ExcelWriter(combined_crm_path, engine='openpyxl') as writer:
            combined_crm.to_excel(writer, index=False, sheet_name='Sheet1')
            if 'crm_transaction_id' in combined_crm.columns and not combined_crm.empty:
                worksheet = writer.sheets['Sheet1']
                trans_col = combined_crm.columns.get_loc('crm_transaction_id') + 1
                row_count = int(combined_crm.shape[0]) if not combined_crm.empty and isinstance(combined_crm.shape[0], (
                int, np.integer)) else 0 # Ensure scalar int
                print(f"Debug: row_count: {row_count}") # Debug
                if row_count > 0:
                    for row in range(2, row_count + 2): # Use validated row_count
                        worksheet.cell(row=row, column=trans_col).number_format = '@'
                else:
                    print("Debug: No rows to format in combined_crm")
        print(f"Combined CRM columns: {combined_crm.columns.tolist()}") # Debug print
        print(f"Combined CRM {transaction_type}s saved to {combined_crm_path}")
    else:
        print("No CRM files found to combine.")
    if proc_dfs:
        combined_proc = pd.concat(proc_dfs, ignore_index=True)
        if transaction_type == "withdrawal":
            combined_proc = group_processor_withdrawals(combined_proc, exchange_rate_map)
        # No grouping for deposits
        # Rename columns for processor
        rename_map_proc = {
            'amount': 'proc_amount',
            'currency': 'proc_currency',
            'date': 'proc_date',
            'last_4cc': 'proc_last4', # Renamed from proc_last4digits to proc_last4
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
                return dt # Keep full datetime to include time
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
        out_proc_date_dir = out_proc_dir / date
        out_proc_date_dir.mkdir(parents=True, exist_ok=True)
        combined_proc_path = out_proc_date_dir / f"combined_processor_{transaction_type}s.xlsx"
        combined_proc.to_excel(combined_proc_path, index=False)
        print(f"Combined processor {transaction_type}s saved to {combined_proc_path}")
        print(f"Combined proc columns: {combined_proc.columns.tolist()}") # Debug print
    else:
        print("No processor files found to combine.")

def append_unmatched_to_combined(date_str, unmatched_path_str):
    combined_path = Path(COMBINED_CRM_DIR) / date_str / "combined_crm_deposits.xlsx"
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
        logging.warning(f"Column mismatch between combined ({combined_cols}) and unmatched ({unmatched_cols}). Appending anyway.")

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