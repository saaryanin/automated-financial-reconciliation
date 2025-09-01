import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from src.config import PROCESSED_CRM_DIR, PROCESSED_PROCESSOR_DIR, DATA_DIR

# Match CRM and processor deposits for a given processor and date, returning unmatched CRM records
def match_deposits(processor: str, date: str) -> pd.DataFrame:
    # Define file paths for CRM and processor deposit data
    crm_path = PROCESSED_CRM_DIR / processor / date / f"{processor}_deposits.xlsx"
    psp_path = PROCESSED_PROCESSOR_DIR / processor / date / f"{processor}_deposits.xlsx"

    # Check if CRM file exists
    if not crm_path.exists():
        print(f"⚠️ CRM file not found for {processor}: {crm_path}")
        return pd.DataFrame()

    # Load CRM deposit data
    crm_df = pd.read_excel(crm_path, dtype=str)

    # Check if processor file exists
    if not psp_path.exists():
        print(f"⚠️ Processor file not found for {processor}: {psp_path}")
        unmatched_crm = crm_df.copy()
        unmatched_psp = pd.DataFrame()
    else:
        # Load processor deposit data
        psp_df = pd.read_excel(psp_path, dtype=str)

        # Identify unmatched transactions by comparing transaction IDs
        crm_ids = set(crm_df["transaction_id"].dropna())
        psp_ids = set(psp_df["transaction_id"].dropna())

        unmatched_psp = psp_df[~psp_df["transaction_id"].isin(crm_ids)].copy()
        unmatched_crm = crm_df[~crm_df["transaction_id"].isin(psp_ids)].copy()

    # If no unmatched records, return empty DataFrame
    if unmatched_psp.empty and unmatched_crm.empty:
        print(f"✅ All {processor.capitalize()} deposits for {date} matched both directions.")
        return pd.DataFrame()

    # Save unmatched records to Excel
    out_dir = DATA_DIR / "lists" / "unmatched_deposits" / processor / date
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{processor}_unmatched.xlsx"

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        if not unmatched_psp.empty:
            unmatched_psp.to_excel(writer, index=False, sheet_name="Unmatched_PSP")
        if not unmatched_crm.empty:
            unmatched_crm.to_excel(writer, index=False, sheet_name="Unmatched_CRM")

    print(f"❌ Unmatched {processor.capitalize()} deposits saved to {out_path} (Processor: {len(unmatched_psp)}, CRM: {len(unmatched_crm)})")
    return unmatched_crm


# Process multiple processors in parallel and return list of unmatched CRM DataFrames
def match_all_processors_in_parallel(processors: list[str], date: str) -> list[pd.DataFrame]:
    # Execute matching for all processors concurrently
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(match_deposits, processor, date) for processor in processors]
        return [f.result() for f in futures if f.result() is not None]


# Combine and save unmatched CRM deposits from all processors
def save_global_crm_unmatched(date: str, unmatched_frames: list[pd.DataFrame]):
    # Combine unmatched CRM DataFrames
    combined = pd.concat(unmatched_frames, ignore_index=True)
    if combined.empty:
        print(f"✅ No unmatched CRM-side deposits to save for {date}")
        return

    # Save combined unmatched CRM deposits to Excel
    out_dir = DATA_DIR / "lists" / "unmatched_deposits" / "crm" / date
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "crm.xlsx"

    combined.to_excel(out_path, index=False)
    print(f"📄 Combined unmatched CRM deposits saved to {out_path} ({len(combined)} rows)")