import pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from src.config import PROCESSED_CRM_DIR, PROCESSED_PROCESSOR_DIR, DATA_DIR


def match_deposits(processor: str, date: str) -> pd.DataFrame:
    crm_path = PROCESSED_CRM_DIR / 'combined' / date / "combined_crm_deposits.xlsx"
    psp_path = PROCESSED_PROCESSOR_DIR / 'combined' / date / "combined_processor_deposits.xlsx"

    if not crm_path.exists() or not psp_path.exists():
        print(f"⚠️ Combined files missing for {processor} deposits on {date}")
        return pd.DataFrame()

    crm_df = pd.read_excel(crm_path, dtype=str)
    psp_df = pd.read_excel(psp_path, dtype=str)

    # Match on transaction_id (add suffixes for column conflicts)
    matched = pd.merge(crm_df, psp_df, on="transaction_id", how="inner", suffixes=('_crm', '_psp'))
    matched['status'] = 'matched'

    # Unmatched
    unmatched_crm = crm_df[~crm_df["transaction_id"].isin(matched["transaction_id"])].copy()
    unmatched_crm['status'] = 'unmatched_crm'

    unmatched_psp = psp_df[~psp_df["transaction_id"].isin(matched["transaction_id"])].copy()
    unmatched_psp['status'] = 'unmatched_psp'

    # Combine all into one DF (align columns)
    all_cols = list(set(matched.columns) | set(unmatched_crm.columns) | set(unmatched_psp.columns))
    combined = pd.concat([matched, unmatched_crm, unmatched_psp], ignore_index=True)
    combined = combined.reindex(columns=all_cols).fillna('')  # Fill missing cols with empty

    if combined.empty:
        print(f"✅ All {processor.capitalize()} deposits for {date} matched both directions.")
        return combined

    out_dir = DATA_DIR / "lists" / "deposits" / processor / date
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{processor}_deposits_combined.xlsx"
    combined.to_excel(out_path, index=False)

    print(f"📄 Combined {processor.capitalize()} deposits saved to {out_path} (Matched: {len(matched)}, Unmatched CRM: {len(unmatched_crm)}, PSP: {len(unmatched_psp)})")
    return combined


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
