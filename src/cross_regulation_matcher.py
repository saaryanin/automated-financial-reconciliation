# src/cross_regulation_matcher.py
"""
Cross-regulation matching for withdrawals.

* Reads row_withdrawals_matching.xlsx  (ROW)
* Reads uk_withdrawals_matching.xlsx   (UK)
* Takes every unmatched CRM row from UK  → tries to match it with every unmatched PROC row from ROW
* Takes every unmatched CRM row from ROW → tries to match it with every unmatched PROC row from UK
* Successful matches are written to separate cross-regulation files: uk_cross_regulation.xlsx and row_cross_regulation.xlsx
* The matched rows are removed from the original *_withdrawals_matching.xlsx files (transfer).
"""
import ast
import pandas as pd
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from src.withdrawals_matcher_test import ReconciliationEngine
from src.config import TEMP_DIR
from src.utils import clean_last4,clean_field


# --------------------------------------------------------------------------- #
# Helper – write cross matches to a new file per regulation                  #
# --------------------------------------------------------------------------- #
def _write_cross_matches(
    matches: List[Dict],
    date_str: str,
    regulation: str,               # "row" or "uk"
) -> None:
    """Write cross-matches to a new regulation-specific cross file."""
    reg_upper = regulation.upper()
    lists_dir = TEMP_DIR / reg_upper / "data" / "lists" / date_str
    file_path = lists_dir / f"{regulation}_cross_regulation.xlsx"

    if not matches:
        return

    # Build a DataFrame exactly like the one produced by the normal matcher
    df_new = pd.DataFrame(matches)

    # Add crm_type for consistency
    df_new['crm_type'] = 'Withdrawal'

    # ------------------------------------------------------------------- #
    # Preserve the exact column order that the rest of the pipeline uses #
    # ------------------------------------------------------------------- #
    desired_order = [
        'crm_type', 'crm_date', 'crm_email', 'crm_firstname', 'crm_lastname',
        'crm_tp', 'crm_last4', 'crm_currency', 'crm_amount', 'payment_method',
        'crm_processor_name', 'regulation', 'proc_date', 'proc_email',
        'proc_tp', 'proc_firstname', 'proc_lastname', 'proc_last4',
        'proc_currency', 'proc_amount', 'proc_amount_crm_currency',
        'proc_processor_name', 'email_similarity_avg', 'last4_match',
        'name_fallback_used', 'exact_match_used', 'match_status',
        'payment_status', 'warning', 'comment', 'matched_proc_indices'
    ]
    df_new = df_new.reindex(columns=[c for c in desired_order if c in df_new.columns or c == 'crm_type'])

    # Clean last-4 the same way the normal matcher does
    if 'proc_last4' in df_new.columns:
        df_new['proc_last4'] = df_new['proc_last4'].astype(str).str.replace(r'\.0$', '', regex=True)

    # ------------------------------------------------------------------- #
    # Write to new file (create if not exists)                          #
    # ------------------------------------------------------------------- #
    file_path.parent.mkdir(parents=True, exist_ok=True)
    df_new.to_excel(file_path, index=False)
    print(f"[Cross-regulation] Wrote {len(df_new)} matches → {file_path}")


# --------------------------------------------------------------------------- #
# Core – cross-match two pools of *unmatched* rows                           #
# --------------------------------------------------------------------------- #
def _cross_match_one_way(
    crm_pool: pd.DataFrame,
    proc_pool: pd.DataFrame,
    exchange_rate_map: dict,
    crm_reg: str,          # "row" or "uk"
    proc_reg: str,         # the opposite
) -> List[Dict]:
    """
    crm_pool  – DataFrame with *unmatched* CRM rows (must contain the normal CRM columns)
    proc_pool – DataFrame with *unmatched* PROC rows (must contain the normal PROC columns)
    Returns a list of match-dicts that can be appended directly.
    """
    if crm_pool.empty or proc_pool.empty:
        return []

    # For ROW CRM -> UK PROC: Exclude UK-only processors like 'barclays' and 'barclaycard'
    if crm_reg == 'row':
        uk_only_processors = ['barclays', 'barclaycard']
        proc_pool = proc_pool[~proc_pool['proc_processor_name'].str.lower().isin(uk_only_processors)]

    if proc_pool.empty:
        return []

    # Preserve original indices before reset
    crm_pool['original_crm_index'] = crm_pool.index
    proc_pool['original_proc_index'] = proc_pool.index

    # Reset indices for engine processing
    crm_pool = crm_pool.reset_index(drop=True)
    proc_pool = proc_pool.reset_index(drop=True)

    # Prepare processor_df
    processor_df = proc_pool.copy()
    processor_df['proc_amount'] = pd.to_numeric(processor_df['proc_amount'], errors='coerce')
    processor_df = processor_df.dropna(subset=['proc_amount'])
    processor_df['proc_last4'] = processor_df['proc_last4'].apply(clean_last4)
    print("After clean, proc_last4 unique: " + str(processor_df['proc_last4'].unique()))
    print("Proc proc_last4 dtypes: " + str(processor_df['proc_last4'].dtype))
    last4_map = processor_df.groupby('proc_last4').indices
    print("Last4 map keys: " + str(list(last4_map.keys())))
    print("Last4 map has '0824': " + str('0824' in last4_map))
    print("Last4 map '0824' indices: " + str(last4_map.get('0824', [])))
    print("Last4 map has '0476': " + str('0476' in last4_map))
    print("Last4 map '0476' indices: " + str(last4_map.get('0476', [])))

    # Prepare crm_df
    crm_df = crm_pool.copy()
    crm_df['crm_amount'] = pd.to_numeric(crm_df['crm_amount'], errors='coerce')
    crm_df = crm_df.dropna(subset=['crm_amount'])
    crm_df = crm_df.drop_duplicates(subset=['crm_email', 'crm_last4', 'crm_amount', 'crm_currency', 'crm_date'])
    print(f"After dedup, CRM rows: {len(crm_df)}")
    crm_df['crm_last4'] = crm_df['crm_last4'].apply(clean_last4)
    print("After clean, crm_last4 unique: " + str(crm_df['crm_last4'].unique()))

    print(f"Cross direction: {crm_reg.upper()} CRM to {proc_reg.upper()} PROC")
    print("Proc pool proc_processor_name unique:", proc_pool['proc_processor_name'].unique())
    print("Proc pool proc_last4 unique:", proc_pool['proc_last4'].unique())
    print("Proc pool proc_email unique:", proc_pool['proc_email'].unique())
    print("CRM pool crm_processor_name unique:", crm_pool['crm_processor_name'].unique())
    print("CRM pool crm_last4 unique:", crm_pool['crm_last4'].unique())
    print("CRM pool crm_email unique:", crm_pool['crm_email'].unique())

    # The engine expects the same column names it works with internally
    engine = ReconciliationEngine(
        exchange_rate_map,
        {
            "enable_cross_processor": True,
            "enable_warning_flag": True,
            "enable_fallback": True,
        },
    )

    # Prepare copies for matching to handle safecharge/safechargeuk mismatches
    crm_df_for_match = crm_df.copy()
    processor_df_for_match = processor_df.copy()

    # For UK CRM -> ROW PROC: Temporarily set regulation to 'row' to bypass safechargeuk-specific routing
    if crm_reg == 'uk':
        crm_df_for_match['regulation'] = 'row'

    # For ROW CRM -> UK PROC: Temporarily rename 'safechargeuk' to 'safecharge' in PROC
    if proc_reg == 'uk':
        processor_df_for_match['proc_processor_name'] = processor_df_for_match['proc_processor_name'].str.replace(
            'safechargeuk', 'safecharge', case=False
        )

    # Run matching on the prepared copies
    raw_matches = engine.match_withdrawals(
        crm_df_for_match,
        processor_df_for_match,
        add_unmatched_proc=False,
        add_unmatched_crm=False,
    )

    print(f"Raw matches count: {len(raw_matches)}")
    print(f"Raw matches with match_status=1: {len([m for m in raw_matches if m['match_status'] == 1])}")
    for m in raw_matches:
        if m['match_status'] == 1:
            print(
                f"Matched CRM email: {m['crm_email']}, PROC email: {m['proc_email']}, last4_match: {m['last4_match']}, email_sim: {m['email_similarity_avg']}")

    # Keep only the *real* matches, restore original proc_processor_name if renamed
    cross_matches = []
    for m in raw_matches:
        if m['match_status'] == 1:
            if proc_reg == 'uk' and m.get('proc_processor_name', '').lower() == 'safecharge':
                m['proc_processor_name'] = 'safechargeuk'
            # Map back to original crm_index using crm_row_index (reset) -> original
            if 'crm_row_index' in m:
                original_crm = crm_df.loc[m['crm_row_index'], 'original_crm_index']
                m['original_crm_index'] = original_crm
            else:
                print(f"Warning: Missing crm_row_index for match with crm_email {m['crm_email']}")
            # Map matched_proc_indices (reset) to original_proc_index
            m['original_proc_indices'] = [processor_df.loc[p, 'original_proc_index'] for p in m['matched_proc_indices']]
            cross_matches.append(m)

    # Add a clear comment so the user knows it came from cross-regulation
    for m in cross_matches:
        crm_proc = m.get("crm_processor_name", "???")
        proc_proc = m.get("proc_processor_name", "???")
        comment = ""
        if m['payment_status'] == 0:
            diff = m['proc_amount_crm_currency'] - abs(m['crm_amount'])
            crm_cur = m['crm_currency']
            if diff > 0:
                over_under = f"Overpaid by {round(diff, 2)} {crm_cur}"
            elif diff < 0:
                over_under = f"Underpaid by {round(-diff, 2)} {crm_cur}"
            else:
                over_under = "Amount mismatch"
            comment = over_under
        cross_part = f"Cross-regulation match – {crm_reg.upper()} CRM ({crm_proc}) ↔ {proc_reg.upper()} PROC ({proc_proc})"
        if comment:
            comment += f". {cross_part}"
        else:
            comment = cross_part
        m["comment"] = comment
        # Force the regulation column to the CRM side (the file we will write to)
        m["regulation"] = crm_reg.upper()

    return cross_matches


# --------------------------------------------------------------------------- #
# Public entry point – called from testing_uk_regulation.py (or anywhere)   #
# --------------------------------------------------------------------------- #
def run_cross_regulation_matching(date_str: str, exchange_rate_map: dict) -> None:
    """
    Main driver – loads the two per-regulation files, extracts unmatched rows,
    runs the two directional cross-matches, writes to new cross files, and removes from originals.
    """
    # ------------------------------------------------------------------- #
    # 1. Load the two per-regulation matching files
    # ------------------------------------------------------------------- #
    row_file = TEMP_DIR / "ROW" / "data" / "lists" / date_str / "row_withdrawals_matching.xlsx"
    uk_file  = TEMP_DIR / "UK"  / "data" / "lists" / date_str / "uk_withdrawals_matching.xlsx"

    if not row_file.exists() or not uk_file.exists():
        print("[Cross-regulation] One of the per-regulation files is missing – nothing to do.")
        return

    row_df = pd.read_excel(row_file)
    uk_df  = pd.read_excel(uk_file)
    for df in (row_df, uk_df):
        list_cols = ['proc_date', 'proc_email', 'proc_tp', 'proc_firstname', 'proc_lastname', 'proc_last4',
                     'proc_currency', 'proc_amount', 'proc_amount_crm_currency']
        for col in list_cols:
            if col in df.columns:
                df[col] = df[col].apply(clean_field)
        if 'proc_last4' in df.columns:
            df['proc_last4'] = df['proc_last4'].apply(clean_last4)
        if 'crm_last4' in df.columns:
            df['crm_last4'] = df['crm_last4'].apply(clean_last4)

    # ------------------------------------------------------------------- #
    # 3. Extract *unmatched* CRM rows  (match_status == 0 AND crm_date NOT null)
    # ------------------------------------------------------------------- #
    unmatched_crm_row_mask = (row_df["match_status"] == 0) & row_df["crm_date"].notna()
    unmatched_crm_row = row_df[unmatched_crm_row_mask].copy()

    unmatched_crm_uk_mask = (uk_df["match_status"] == 0) & uk_df["crm_date"].notna()
    unmatched_crm_uk = uk_df[unmatched_crm_uk_mask].copy()

    # ------------------------------------------------------------------- #
    # 4. Extract *unmatched* PROC rows  (match_status == 0 AND crm_date IS null)
    # ------------------------------------------------------------------- #
    unmatched_proc_row_mask = (row_df["match_status"] == 0) & row_df["crm_date"].isna()
    unmatched_proc_row = row_df[unmatched_proc_row_mask].copy()

    unmatched_proc_uk_mask = (uk_df["match_status"] == 0) & uk_df["crm_date"].isna()
    unmatched_proc_uk = uk_df[unmatched_proc_uk_mask].copy()

    # ------------------------------------------------------------------- #
    # 5. Two directional cross-matches
    # ------------------------------------------------------------------- #
    # UK CRM  → ROW PROC
    uk_to_row = _cross_match_one_way(
        crm_pool=unmatched_crm_uk,
        proc_pool=unmatched_proc_row,
        exchange_rate_map=exchange_rate_map,
        crm_reg="uk",
        proc_reg="row",
    )
    # ROW CRM → UK PROC
    row_to_uk = _cross_match_one_way(
        crm_pool=unmatched_crm_row,
        proc_pool=unmatched_proc_uk,
        exchange_rate_map=exchange_rate_map,
        crm_reg="row",
        proc_reg="uk",
    )

    # ------------------------------------------------------------------- #
    # 6. For each direction: remove matched CRM/PROC from originals
    # ------------------------------------------------------------------- #
    # For UK→ROW
    if uk_to_row:
        matched_original_crm_idxs = [m['original_crm_index'] for m in uk_to_row]
        matched_original_proc_idxs = [p for m in uk_to_row for p in m['original_proc_indices']]

        uk_df = uk_df.drop(matched_original_crm_idxs, errors='ignore')
        row_df = row_df.drop(matched_original_proc_idxs, errors='ignore')

    # For ROW→UK
    if row_to_uk:
        matched_original_crm_idxs = [m['original_crm_index'] for m in row_to_uk]
        matched_original_proc_idxs = [p for m in row_to_uk for p in m['original_proc_indices']]

        row_df = row_df.drop(matched_original_crm_idxs, errors='ignore')
        uk_df = uk_df.drop(matched_original_proc_idxs, errors='ignore')

    # ------------------------------------------------------------------- #
    # 7. Overwrite the original files with removed rows
    # ------------------------------------------------------------------- #
    row_df.to_excel(row_file, index=False)
    uk_df.to_excel(uk_file, index=False)
    print(f"[Cross-regulation] Updated original files by removing cross-matched rows")

    # ------------------------------------------------------------------- #
    # 8. Write cross matches to new files
    # ------------------------------------------------------------------- #
    _write_cross_matches(uk_to_row, date_str, regulation="uk")
    _write_cross_matches(row_to_uk, date_str, regulation="row")

    print(
        f"[Cross-regulation] Finished – "
        f"{len(uk_to_row)} UK→ROW matches, {len(row_to_uk)} ROW→UK matches"
    )