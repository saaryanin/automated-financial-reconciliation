import pandas as pd
from pathlib import Path
from src.config import PROCESSED_CRM_DIR, PROCESSED_PROCESSOR_DIR, DATA_DIR


def match_paypal_deposits(date: str) -> pd.DataFrame:
    crm_path = PROCESSED_CRM_DIR / "paypal" / date / "paypal_deposits.xlsx"
    paypal_path = PROCESSED_PROCESSOR_DIR / "paypal" / date / "paypal_deposits.xlsx"

    crm_df = pd.read_excel(crm_path, dtype=str)
    paypal_df = pd.read_excel(paypal_path, dtype=str)

    # Keep original column orders
    crm_columns = crm_df.columns.tolist()
    paypal_columns = paypal_df.columns.tolist()

    crm_ids = set(crm_df["transaction_id"].dropna())
    psp_ids = set(paypal_df["transaction_id"].dropna())

    unmatched_psp = paypal_df[~paypal_df["transaction_id"].isin(crm_ids)].copy()
    unmatched_crm = crm_df[~crm_df["transaction_id"].isin(psp_ids)].copy()

    if unmatched_psp.empty and unmatched_crm.empty:
        print(f"✅ All PayPal deposits for {date} matched both directions.")
        return pd.DataFrame()

    out_dir = DATA_DIR / "lists" / "unmatched_deposits" / "paypal" / date
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "paypal_unmatched.xlsx"

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        unmatched_psp.to_excel(writer, index=False, sheet_name="Unmatched_PSP", columns=paypal_columns)
        unmatched_crm.to_excel(writer, index=False, sheet_name="Unmatched_CRM", columns=crm_columns)

    print(f"❌ Unmatched PayPal deposits saved to {out_path} (Processor: {len(unmatched_psp)}, CRM: {len(unmatched_crm)})")

    return unmatched_crm


def match_safecharge_deposits(date: str) -> pd.DataFrame:
    from openpyxl import Workbook
    crm_path = PROCESSED_CRM_DIR / "safecharge" / date / "safecharge_deposits.xlsx"
    sc_path = PROCESSED_PROCESSOR_DIR / "safecharge" / date / "safecharge_deposits.xlsx"

    # Load with full structure preserved
    crm_df = pd.read_excel(crm_path, dtype=str)
    sc_df = pd.read_excel(sc_path, dtype=str)

    # Capture original column orders
    crm_columns = crm_df.columns.tolist()
    sc_columns = sc_df.columns.tolist()

    # Match on transaction_id
    crm_ids = set(crm_df["transaction_id"].dropna())
    sc_ids = set(sc_df["transaction_id"].dropna())

    unmatched_processor = sc_df[~sc_df["transaction_id"].isin(crm_ids)].copy()
    unmatched_crm = crm_df[~crm_df["transaction_id"].isin(sc_ids)].copy()

    if unmatched_processor.empty and unmatched_crm.empty:
        print(f"✅ All SafeCharge deposits for {date} matched both directions.")
        return pd.DataFrame()

    # Output to processor-specific file
    out_dir = DATA_DIR / "lists" / "unmatched_deposits" / "safecharge" / date
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "safecharge_unmatched.xlsx"

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        unmatched_processor.to_excel(writer, index=False, sheet_name="Unmatched_PSP", columns=sc_columns)
        unmatched_crm.to_excel(writer, index=False, sheet_name="Unmatched_CRM", startrow=0, columns=crm_columns)

    print(f"❌ Unmatched SafeCharge deposits saved to {out_path} (Processor: {len(unmatched_processor)}, CRM: {len(unmatched_crm)})")

    return unmatched_crm
import pandas as pd
from pathlib import Path
from src.config import PROCESSED_CRM_DIR, PROCESSED_PROCESSOR_DIR, DATA_DIR


def match_powercash_deposits(date: str) -> pd.DataFrame:
    crm_path = PROCESSED_CRM_DIR / "powercash" / date / "powercash_deposits.xlsx"
    psp_path = PROCESSED_PROCESSOR_DIR / "powercash" / date / "powercash_deposits.xlsx"

    crm_df = pd.read_excel(crm_path, dtype=str)
    psp_df = pd.read_excel(psp_path, dtype=str)

    crm_ids = set(crm_df["transaction_id"].dropna())
    psp_ids = set(psp_df["transaction_id"].dropna())

    unmatched_processor = psp_df[~psp_df["transaction_id"].isin(crm_ids)].copy()
    unmatched_crm = crm_df[~crm_df["transaction_id"].isin(psp_ids)].copy()

    if unmatched_processor.empty and unmatched_crm.empty:
        print(f"✅ All PowerCash deposits for {date} matched both directions.")
        return pd.DataFrame()

    # Save ONLY processor-side unmatched in PSP format
    out_dir = DATA_DIR / "lists" / "unmatched_deposits" / "powercash" / date
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "powercash_unmatched.xlsx"

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        unmatched_processor.to_excel(writer, index=False, sheet_name="Unmatched")

    print(
        f"❌ Unmatched PowerCash deposits saved to {out_path} "
        f"(Processor: {len(unmatched_processor)}, CRM: {len(unmatched_crm)})"
    )

    return unmatched_crm



def save_global_crm_unmatched(date: str, unmatched_frames: list[pd.DataFrame]):
    combined = pd.concat(unmatched_frames, ignore_index=True)
    if combined.empty:
        print(f"✅ No unmatched CRM-side deposits to save for {date}")
        return

    out_dir = DATA_DIR / "lists" / "unmatched_deposits" / "crm" / date
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "crm.xlsx"

    combined.to_excel(out_path, index=False)
    print(f"📄 Combined unmatched CRM deposits saved to {out_path} ({len(combined)} rows)")





