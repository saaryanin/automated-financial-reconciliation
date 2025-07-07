import pandas as pd
import re
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from src.config import PROCESSED_CRM_DIR, PROCESSED_PROCESSOR_DIR
import logging
from collections import Counter
from dateutil import parser

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
    # Add any other known aliases as needed
}
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

def clean_crm_last4(val):
    val_str = str(val).strip()
    if val_str.endswith('.0'):
        val_str = val_str[:-2]
    match = re.search(r'(\d{1,4})$', val_str)
    return match.group(1).zfill(4) if match else ""
def clean_proc_last4(val):
    val_str = str(val).strip()
    if val_str.endswith('.0'):
        val_str = val_str[:-2]
    match = re.search(r'(\d{1,4})$', val_str)
    return match.group(1).zfill(4) if match else ""

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
            print("No PayPal Withdrawals found after filtering.")
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
            print("No valid PayPal withdrawals after reversal-cancellation.")
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
            print("SafeCharge: Required columns not found.")
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
            print("No SafeCharge withdrawals found after filtering.")
            return pd.DataFrame()

        # 2. Cancel both VoidCredit and its paired Credit row (search up and down)
        to_remove = set()
        df = df.reset_index(drop=True)  # Ensures index is 0...N
        void_rows = df[df[colmap['transactiontype']] == "VoidCredit"]

        # ... the rest of your logic ...
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
        df["currency"] = df["currency"].replace({"Euro": "EUR", "US Dollar": "USD"})
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
            print("No Shift4 withdrawals found after filtering.")
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
            print("No valid Shift4 withdrawals after void-cancellation.")
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



    elif processor.lower() == "trustpayments":
        # Filter to only REFUND type and errorcode == 0
        df = df[
            (df["requesttypedescription"].str.upper() == "REFUND") &
            (df["errorcode"] == 0)
            ].copy()

        if df.empty:
            print("No TrustPayments withdrawals found after filtering.")
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


def handle_withdrawal_cancellations(df):
    if "Name" not in df.columns:
        return df

    mask_cancel = df["Name"].str.lower().str.replace(' ', '') == "withdrawalcancelled"
    mask_withdrawal = df["Name"].str.lower() == "withdrawal"
    cancels = df[mask_cancel].copy()
    withdrawals = df[mask_withdrawal].copy()

    to_drop = set()

    for idx_cancel, row_cancel in cancels.iterrows():
        # Match withdrawals by 'tp'
        matched = withdrawals[withdrawals["tp"] == row_cancel["tp"]]
        if matched.empty:
            continue
        # Find a row where the amounts cancel out
        for idx_withdrawal, row_withdrawal in matched.iterrows():
            amt_cancel = pd.to_numeric(row_cancel["Amount"], errors='coerce')
            amt_withdrawal = pd.to_numeric(row_withdrawal["Amount"], errors='coerce')
            # If they cancel each other out
            if abs(amt_cancel + amt_withdrawal) < 1e-6:
                print(
                    f"Cancelling withdrawal (idx={idx_withdrawal}) and cancel row (idx={idx_cancel}) for tp={row_cancel['tp']}")
                to_drop.update([idx_cancel, idx_withdrawal])
                break

    df = df.drop(index=list(to_drop))
    return df


# ----------------------------
# CRM Handling
# ----------------------------
def load_crm_file(filepath: str, processor_name: str, save_clean=False, transaction_type="deposit") -> pd.DataFrame:
    df = pd.read_excel(filepath, engine="openpyxl")
    df.columns = df.columns.str.strip()
    df["PSP name"] = (
        df["PSP name"]
        .astype(str)
        .str.strip()
        .str.lower()
        .replace(PSP_NAME_MAP)
    )
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
        method_match = df["Method of Payment"].astype(str).str.contains("paymentasia|zotapay-cup|PA-MY", case=False, na=False)
        full_mask = name_col_match & (psp_match | method_match)
        df = df[full_mask].reset_index(drop=True)
    else:
        psp_mask = df["PSP name"] == normalized_processor

        if transaction_type == "withdrawal":
            name_mask = df["Name"].str.lower().isin(["withdrawal", "withdrawal cancelled"])
            df = df[name_mask & psp_mask].reset_index(drop=True)
        else:
            df = df[(df["Name"].str.lower() == transaction_type) & psp_mask].reset_index(drop=True)

    if "Currency" in df.columns:
        df["Currency"] = df["Currency"].replace({
            "Euro": "EUR",
            "US Dollar": "USD"
        })


    # Drop 'transaction_id' column AFTER handling cancellations
    if "transaction_id" in df.columns:
        df = df.drop(columns=["transaction_id"])

    # --- Only keep needed columns for withdrawal output ---
    if transaction_type == "withdrawal":
        df = handle_withdrawal_cancellations(df)
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
            "Name"  # 🟢 Optional: keep 'Name' for traceability (remove if not wanted)
        ]
        # Only keep columns that exist in the df
        df = df[[col for col in needed_columns if col in df.columns]]

    if save_clean:
        date_str = extract_date_from_filename(filepath)
        folder_name = "zotapay_paymentasia" if normalized_processor in ["zotapay", "paymentasia"] else normalized_processor
        folder = f"{folder_name}_{transaction_type}s.xlsx"
        out_path = PROCESSED_CRM_DIR / folder_name / date_str / folder
        out_path.parent.mkdir(parents=True, exist_ok=True)
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

def combine_processed_files(
    date, processors,
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

    crm_dfs, proc_dfs = [], []
    crm_file_template = f"{{}}_{transaction_type}s.xlsx"
    proc_file_template = f"{{}}_{transaction_type}s.xlsx"

    for proc in all_processors:
        crm_f = processed_crm_dir / proc / date / crm_file_template.format(proc)
        if crm_f.exists():
            df = pd.read_excel(crm_f)
            crm_dfs.append(df)
        else:
            print(f"⚠️ CRM processed file not found for {proc}: {crm_f}")

        proc_f = processed_proc_dir / proc / date / proc_file_template.format(proc)
        if proc_f.exists():
            df = pd.read_excel(proc_f)
            proc_dfs.append(df)
        else:
            print(f"⚠️ Processor processed file not found for {proc}: {proc_f}")

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
        for col in ['CC Last 4 Digits', 'PSP name', 'Email (Account) (Account)',
                    'First Name (Account) (Account)', 'Last Name (Account) (Account)']:
            if col not in df.columns:
                df[col] = ''
            df[col] = df[col].astype(str).fillna('').str.strip()
        if 'Currency' not in df.columns:
            df['Currency'] = 'USD'
        if 'Amount' not in df.columns:
            df['Amount'] = 0.0

        # Decide group key: use last 4 if present, fallback to names
        last4_nonblank = (df['CC Last 4 Digits'].astype(str).str.strip() != '').any()
        if last4_nonblank:
            group_cols = ['CC Last 4 Digits', 'PSP name', 'Email (Account) (Account)']
            print(f"[DEBUG] CRM: Grouping by {group_cols}")
        else:
            group_cols = ['First Name (Account) (Account)', 'Last Name (Account) (Account)', 'PSP name']
            print(f"[DEBUG] CRM: Grouping by {group_cols}")

        grouped_rows = []
        for keys, group in df.groupby(group_cols):
            group = group[group['Amount'].notna() & group['Currency'].notna()]
            if group.empty:
                continue
            currencies = group['Currency'].tolist()
            tgt_cur = choose_target_currency(currencies)
            amounts = []
            for _, row in group.iterrows():
                amt = float(row['Amount'])
                src_cur = row['Currency']
                if src_cur == tgt_cur:
                    converted = amt
                else:
                    key = (src_cur, tgt_cur)
                    if exchange_rate_map and key in exchange_rate_map:
                        converted = amt * exchange_rate_map[key]
                    else:
                        print(f"⚠️ No exchange rate for {key}, leaving amount as is.")
                        converted = amt
                amounts.append(converted)
            total_amt = sum(amounts)
            row0 = group.iloc[0].copy()
            row0['Amount'] = total_amt
            row0['Currency'] = tgt_cur
            grouped_rows.append(row0)
        out_df = pd.DataFrame(grouped_rows)
        return out_df

    import dateutil.parser

    def group_processor_withdrawals(df, exchange_rate_map):
        df = df.copy()

        # Clean string columns used in grouping to avoid issues
        for col in ['processor_name', 'first_name', 'last_name', 'email', 'last_4cc']:
            if col not in df.columns:
                df[col] = ''
            df[col] = df[col].astype(str).fillna('').str.strip()

        # Normalize last_4cc here: remove '.0' if present before grouping
        def clean_last4(val):
            val_str = str(val)
            if val_str.endswith('.0'):
                val_str = val_str[:-2]
            return val_str.strip()

        df['last_4cc'] = df['last_4cc'].apply(clean_last4)

        out_dfs = []
        for pname, group in df.groupby('processor_name'):
            pname_lower = pname.lower()
            has_last4 = (group['last_4cc'].astype(str).str.strip() != '').any()
            if pname_lower == "trustpayments":
                if has_last4:
                    group_cols = ['processor_name', 'first_name', 'last_name', 'last_4cc']
                else:
                    group_cols = ['processor_name', 'first_name', 'last_name']
                print(f"[DEBUG] Grouping {pname} by: {group_cols}")
            elif has_last4:
                group_cols = ['processor_name', 'email', 'last_4cc']
                print(f"[DEBUG] Grouping {pname} by: {group_cols}")
            else:
                group_cols = ['processor_name', 'email']
                print(f"[DEBUG] Grouping {pname} by: {group_cols}")

            grouped_rows = []
            for keys, subg in group.groupby(group_cols, dropna=False):
                subg = subg[subg['amount'].notna() & subg['currency'].notna()]
                if subg.empty:
                    continue
                currencies = subg['currency'].tolist()
                tgt_cur = choose_target_currency(currencies)
                amounts = []
                for _, row in subg.iterrows():
                    amt = float(row['amount'])
                    src_cur = row['currency']
                    if src_cur == tgt_cur:
                        converted = amt
                    else:
                        key = (src_cur, tgt_cur)
                        if exchange_rate_map and key in exchange_rate_map:
                            converted = amt * exchange_rate_map[key]
                        else:
                            print(f"⚠️ No exchange rate for {key}, leaving amount as is.")
                            converted = amt
                    amounts.append(converted)
                total_amt = sum(amounts)
                row0 = subg.iloc[0].copy()
                row0['amount'] = total_amt
                row0['currency'] = tgt_cur
                grouped_rows.append(row0)
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
        combined_crm = group_crm_withdrawals(combined_crm, exchange_rate_map)

        # Rename and format CRM columns as requested
        rename_map = {
            'Created On': 'crm_date',
            'First Name (Account) (Account)': 'crm_firstname',
            'Last Name (Account) (Account)': 'crm_lastname',
            'Email (Account) (Account)': 'crm_email',
            'tp': 'crm_tp',
            'Amount': 'crm_amount',
            'Currency': 'crm_currency',
            'PSP name': 'crm_processor_name',
            'CC Last 4 Digits': 'crm_last4',
            'Name': 'crm_type',
        }

        for old_col, new_col in rename_map.items():
            if old_col in combined_crm.columns:
                combined_crm.rename(columns={old_col: new_col}, inplace=True)

        if 'crm_date' in combined_crm.columns:
            combined_crm['crm_date'] = pd.to_datetime(combined_crm['crm_date'], errors='coerce').dt.date

        if 'crm_last4' in combined_crm.columns:
            combined_crm['crm_last4'] = combined_crm['crm_last4'].apply(clean_crm_last4)

        out_crm_date_dir = out_crm_dir / date
        out_crm_date_dir.mkdir(parents=True, exist_ok=True)
        combined_crm_path = out_crm_date_dir / f"combined_crm_{transaction_type}s.xlsx"
        combined_crm.to_excel(combined_crm_path, index=False)
        print(f"✅ Combined CRM withdrawals saved to {combined_crm_path}")
    else:
        print("⚠️ No CRM files found to combine.")

    if proc_dfs:
        combined_proc = pd.concat(proc_dfs, ignore_index=True)
        combined_proc = group_processor_withdrawals(combined_proc, exchange_rate_map)

        # Rename columns for processor
        rename_map_proc = {
            'amount': 'proc_amount',
            'currency': 'proc_currency',
            'date': 'proc_date',
            'last_4cc': 'proc_last4',
            'email': 'proc_email',
            'first_name': 'proc_firstname',
            'last_name': 'proc_lastname',
            'processor_name': 'proc_processor_name',
            'tp': 'proc_tp',
        }

        for old_col, new_col in rename_map_proc.items():
            if old_col in combined_proc.columns:
                combined_proc.rename(columns={old_col: new_col}, inplace=True)

        # Robust date parsing for proc_date using dateutil.parser to handle all formats
        import dateutil.parser

        def parse_mixed_date(date_str):
            if pd.isna(date_str) or str(date_str).strip() == '':
                return pd.NaT
            try:
                dt = dateutil.parser.parse(str(date_str), dayfirst=True)
                return dt.date()
            except Exception:
                return pd.NaT

        if 'proc_date' in combined_proc.columns:
            combined_proc['proc_date'] = combined_proc['proc_date'].apply(parse_mixed_date)

        # Normalize proc_amount to numeric absolute
        if 'proc_amount' in combined_proc.columns:
            combined_proc['proc_amount'] = pd.to_numeric(combined_proc['proc_amount'], errors='coerce').abs()


        if 'proc_last4' in combined_proc.columns:
            combined_proc['proc_last4'] = combined_proc['proc_last4'].apply(clean_proc_last4)

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
        str_cols_proc = [
            'proc_email', 'proc_firstname', 'proc_lastname', 'proc_processor_name'
        ]
        for col in str_cols_proc:
            if col in combined_proc.columns:
                combined_proc[col] = combined_proc[col].fillna('').astype(str).str.strip()
            else:
                combined_proc[col] = ''

        out_proc_date_dir = out_proc_dir / date
        out_proc_date_dir.mkdir(parents=True, exist_ok=True)
        combined_proc_path = out_proc_date_dir / f"combined_processor_{transaction_type}s.xlsx"
        combined_proc.to_excel(combined_proc_path, index=False)
        print(f"✅ Combined processor withdrawals saved to {combined_proc_path}")
    else:
        print("⚠️ No processor files found to combine.")





