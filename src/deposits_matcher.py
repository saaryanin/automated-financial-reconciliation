import pandas as pd
from pathlib import Path
from src.config import PROCESSED_CRM_DIR, PROCESSED_PROCESSOR_DIR, DATA_DIR


def match_paypal_deposits(date: str):
    # Paths to cleaned files
    crm_path = PROCESSED_CRM_DIR / "paypal" / date / f"paypal_deposits_{date}.xlsx"
    paypal_path = PROCESSED_PROCESSOR_DIR / "paypal" / date / "paypal_deposits.xlsx"

    # Load files
    crm_df = pd.read_excel(crm_path)
    paypal_df = pd.read_excel(paypal_path)

    # Get unmatched PayPal transactions (those not in CRM)
    crm_ids = set(crm_df["transaction_id"].dropna())
    unmatched_df = paypal_df[~paypal_df["transaction_id"].isin(crm_ids)].copy()

    if unmatched_df.empty:
        print(f"✅ All PayPal deposits for {date} matched with CRM.")
        return

    # Save unmatched to: data/lists/unmatched_deposits/2025-05-07.csv
    out_dir = DATA_DIR / "lists" / "unmatched_deposits"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{date}.csv"
    unmatched_df.to_csv(out_path, index=False)
    print(f"❌ Unmatched PayPal deposits saved to {out_path} ({len(unmatched_df)} rows)")
