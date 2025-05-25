import pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from src.config import PROCESSED_CRM_DIR, PROCESSED_PROCESSOR_DIR, DATA_DIR


def match_deposits(processor: str, date: str) -> pd.DataFrame:
    crm_path = PROCESSED_CRM_DIR / processor / date / f"{processor}_deposits.xlsx"
    psp_path = PROCESSED_PROCESSOR_DIR / processor / date / f"{processor}_deposits.xlsx"

    crm_df = pd.read_excel(crm_path, dtype=str)
    psp_df = pd.read_excel(psp_path, dtype=str)

    crm_ids = set(crm_df["transaction_id"].dropna())
    psp_ids = set(psp_df["transaction_id"].dropna())

    unmatched_psp = psp_df[~psp_df["transaction_id"].isin(crm_ids)].copy()
    unmatched_crm = crm_df[~crm_df["transaction_id"].isin(psp_ids)].copy()

    if unmatched_psp.empty and unmatched_crm.empty:
        print(f"✅ All {processor.capitalize()} deposits for {date} matched both directions.")
        return pd.DataFrame()

    out_dir = DATA_DIR / "lists" / "unmatched_deposits" / processor / date
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{processor}_unmatched.xlsx"

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        if processor == "powercash":
            unmatched_psp.to_excel(writer, index=False, sheet_name="Unmatched")
        else:
            psp_columns = psp_df.columns.tolist()
            crm_columns = crm_df.columns.tolist()
            unmatched_psp.to_excel(writer, index=False, sheet_name="Unmatched_PSP", columns=psp_columns)
            unmatched_crm.to_excel(writer, index=False, sheet_name="Unmatched_CRM", columns=crm_columns)

    print(f"❌ Unmatched {processor.capitalize()} deposits saved to {out_path} (Processor: {len(unmatched_psp)}, CRM: {len(unmatched_crm)})")
    return unmatched_crm


def match_all_processors_in_parallel(processors: list[str], date: str) -> list[pd.DataFrame]:
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(match_deposits, processor, date) for processor in processors]
        return [f.result() for f in futures if f.result() is not None]


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
