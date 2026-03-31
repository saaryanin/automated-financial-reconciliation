"""
Copyright 2026 Saar Yanin
Licensed under the Apache License 2.0 with Commons Clause
See LICENSE for details.

Script: deposits_matcher.py
Description: This script matches deposit records between combined CRM and processor data for both ROW and UK regulations on a specified date. It conducts local matching based on transaction_id and processor_name, addresses mismatches like safecharge/safechargeuk, performs cross-regulation matching for remaining unmatched CRM records, combines matched and unmatched rows with status flags, and generates regulation-specific deposit matching reports.

Key Features:
- Data loading: Reads combined CRM and processor Excel files for ROW and UK, specifying string dtype for transaction_ids to preserve leading zeros.
- UK matching: Performs general local matching (excluding safecharge) on transaction_id and processor_name; specific matching for safecharge CRM to safechargeuk PROC on transaction_id; cross-matches remaining unmatched UK CRM to ROW processors on transaction_id.
- ROW matching: First excludes ROW processors already matched to UK CRM; then local matching on transaction_id and processor_name; cross-matches remaining unmatched ROW CRM to available UK processors on transaction_id.
- Result combination: Assigns match_status=1 to matched rows, fills missing columns with NaN for unmatched CRM/PROC rows, sets match_status=0; sorts by match_status descending for prioritization.
- Column adjustments: Moves 'crm_type' to the front if present; saves combined results to row/uk_deposits_matching.xlsx in the lists directory for the given date.
- Edge cases: Gracefully handles missing or empty files by skipping and using empty DataFrames; ensures no duplicate matches across regulations.

Dependencies:
- pandas (for DataFrame loading, merging, filtering, and saving)
- numpy (for NaN handling and potential array operations)
- src.config (for setup_dirs_for_reg function to get directory paths)
"""

import pandas as pd
import numpy as np
from src.config import setup_dirs_for_reg


# Function to match deposits for both ROW and UK regulations for a given date
def match_deposits_for_date(date_str: str):
    # Get directories for ROW and UK
    row_dirs = setup_dirs_for_reg("row", create=False)
    uk_dirs = setup_dirs_for_reg("uk", create=False)
    # Load combined CRM and processor files for UK and ROW
    uk_crm_path = (
        uk_dirs["combined_crm_dir"] / date_str / "combined_crm_deposits.xlsx"
    )
    uk_proc_path = (
        uk_dirs["processed_processor_dir"]
        / "combined"
        / date_str
        / "combined_processor_deposits.xlsx"
    )
    row_crm_path = (
        row_dirs["combined_crm_dir"] / date_str / "combined_crm_deposits.xlsx"
    )
    row_proc_path = (
        row_dirs["processed_processor_dir"]
        / "combined"
        / date_str
        / "combined_processor_deposits.xlsx"
    )
    # Load DataFrames if files exist
    uk_crm = (
        pd.read_excel(uk_crm_path, dtype={"crm_transaction_id": str})
        if uk_crm_path.exists()
        else pd.DataFrame()
    )
    uk_proc = (
        pd.read_excel(uk_proc_path, dtype={"proc_transaction_id": str})
        if uk_proc_path.exists()
        else pd.DataFrame()
    )
    row_crm = (
        pd.read_excel(row_crm_path, dtype={"crm_transaction_id": str})
        if row_crm_path.exists()
        else pd.DataFrame()
    )
    row_proc = (
        pd.read_excel(row_proc_path, dtype={"proc_transaction_id": str})
        if row_proc_path.exists()
        else pd.DataFrame()
    )
    if not uk_crm.empty:
        uk_crm["crm_transaction_id"] = (
            uk_crm["crm_transaction_id"].astype(str).str.strip()
        )
    if not uk_proc.empty:
        uk_proc["proc_transaction_id"] = (
            uk_proc["proc_transaction_id"].astype(str).str.strip()
        )
    if not row_crm.empty:
        row_crm["crm_transaction_id"] = (
            row_crm["crm_transaction_id"].astype(str).str.strip()
        )
    if not row_proc.empty:
        row_proc["proc_transaction_id"] = (
            row_proc["proc_transaction_id"].astype(str).str.strip()
        )
    # Initialize so ROW block can safely reference these regardless of whether UK block ran
    matched_cross_uk = pd.DataFrame()
    matched_local_uk = pd.DataFrame()

    if uk_crm.empty or (uk_proc.empty and row_proc.empty):
        print(f"Skipping UK matching: Missing combined files for {date_str}")
        uk_df = pd.DataFrame()
    else:
        # General UK Local Matching: Match on transaction_id and processor_name (excludes SafeCharge due to mismatch)
        matched_local_uk_general = pd.merge(
            uk_crm[uk_crm["crm_processor_name"] != "safecharge"],
            uk_proc[uk_proc["proc_processor_name"] != "safechargeuk"],
            left_on=["crm_transaction_id", "crm_processor_name"],
            right_on=["proc_transaction_id", "proc_processor_name"],
            how="inner",
            suffixes=("", "_y"),
        )
        matched_local_uk_general.drop(
            columns=[
                col
                for col in matched_local_uk_general.columns
                if col.endswith("_y")
            ],
            inplace=True,
        )
        # Specific SafeCharge UK Local Matching: Match UK CRM 'safecharge' with UK Proc 'safechargeuk' on transaction_id only
        uk_crm_safecharge = uk_crm[uk_crm["crm_processor_name"] == "safecharge"]
        uk_proc_safechargeuk = uk_proc[
            uk_proc["proc_processor_name"] == "safechargeuk"
        ]
        matched_local_uk_safecharge = pd.merge(
            uk_crm_safecharge,
            uk_proc_safechargeuk,
            left_on="crm_transaction_id",
            right_on="proc_transaction_id",
            how="inner",
            suffixes=("", "_y"),
        )
        matched_local_uk_safecharge.drop(
            columns=[
                col
                for col in matched_local_uk_safecharge.columns
                if col.endswith("_y")
            ],
            inplace=True,
        )
        # Combine all local matched for UK
        matched_local_uk = pd.concat(
            [matched_local_uk_general, matched_local_uk_safecharge], ignore_index=True
        )
        # Get matched IDs for local
        matched_ids_uk_local = matched_local_uk["crm_transaction_id"].unique()
        # Unmatched UK CRM after local
        unmatched_uk_crm_local = uk_crm[
            ~uk_crm["crm_transaction_id"].isin(matched_ids_uk_local)
        ]
        # UK Cross Matching with ROW processors: Match on transaction_id only
        matched_cross_uk = pd.merge(
            unmatched_uk_crm_local,
            row_proc,
            left_on="crm_transaction_id",
            right_on="proc_transaction_id",
            how="inner",
            suffixes=("", "_y"),
        )
        matched_cross_uk.drop(
            columns=[
                col for col in matched_cross_uk.columns if col.endswith("_y")
            ],
            inplace=True,
        )
        # All matched for UK
        all_matched_uk = pd.concat(
            [matched_local_uk, matched_cross_uk], ignore_index=True
        )
        all_matched_uk["match_status"] = 1
        # Final unmatched UK CRM
        matched_cross_uk_ids = (
            matched_cross_uk["crm_transaction_id"].unique()
            if not matched_cross_uk.empty
            else []
        )
        final_unmatched_uk_crm = unmatched_uk_crm_local[
            ~unmatched_uk_crm_local["crm_transaction_id"].isin(matched_cross_uk_ids)
        ].copy()
        # Add processor columns as NaN to unmatched UK CRM if not empty
        if not final_unmatched_uk_crm.empty:
            proc_cols = [
                col
                for col in uk_proc.columns
                if col not in final_unmatched_uk_crm.columns
            ]  # Use uk_proc for consistency, but could use row_proc too
            for col in proc_cols:
                final_unmatched_uk_crm[col] = np.nan
            final_unmatched_uk_crm["match_status"] = 0
        # Unmatched UK processors: Exclude those matched locally (cross matches are from ROW, so not affecting UK proc)
        matched_uk_proc_ids_local = matched_local_uk[
            "proc_transaction_id"
        ].unique()
        preliminary_unmatched_uk_proc = uk_proc[
            ~uk_proc["proc_transaction_id"].isin(matched_uk_proc_ids_local)
        ].copy()
    if row_crm.empty or (row_proc.empty and uk_proc.empty):
        print(f"Skipping ROW matching: Missing combined files for {date_str}")
        row_df = pd.DataFrame()
    else:
        # Get IDs matched to UK from ROW proc (for exclusion in ROW local)
        matched_cross_uk_proc_ids = (
            matched_cross_uk["proc_transaction_id"].unique()
            if not matched_cross_uk.empty
            else []
        )
        # Available ROW proc (exclude those matched to UK CRM)
        available_row_proc = row_proc[
            ~row_proc["proc_transaction_id"].isin(matched_cross_uk_proc_ids)
        ]
        # ROW Local Matching: Match on transaction_id and processor_name with available ROW PROC
        matched_local_row = pd.merge(
            row_crm,
            available_row_proc,
            left_on=["crm_transaction_id", "crm_processor_name"],
            right_on=["proc_transaction_id", "proc_processor_name"],
            how="inner",
            suffixes=("", "_y"),
        )
        matched_local_row.drop(
            columns=[
                col for col in matched_local_row.columns if col.endswith("_y")
            ],
            inplace=True,
        )
        # Unmatched ROW CRM after local
        matched_ids_row_local = matched_local_row["crm_transaction_id"].unique()
        unmatched_row_crm_local = row_crm[
            ~row_crm["crm_transaction_id"].isin(matched_ids_row_local)
        ]
        # Available UK proc (exclude those matched to UK CRM local)
        matched_uk_proc_ids_local = (
            matched_local_uk["proc_transaction_id"].unique()
            if not matched_local_uk.empty
            else []
        )
        available_uk_proc = uk_proc[
            ~uk_proc["proc_transaction_id"].isin(matched_uk_proc_ids_local)
        ]
        # ROW Cross Matching with available UK processors: Match on transaction_id only
        matched_cross_row = pd.merge(
            unmatched_row_crm_local,
            available_uk_proc,
            left_on="crm_transaction_id",
            right_on="proc_transaction_id",
            how="inner",
            suffixes=("", "_y"),
        )
        matched_cross_row.drop(
            columns=[
                col for col in matched_cross_row.columns if col.endswith("_y")
            ],
            inplace=True,
        )
        # All matched for ROW
        all_matched_row = pd.concat(
            [matched_local_row, matched_cross_row], ignore_index=True
        )
        all_matched_row["match_status"] = 1
        # Final unmatched ROW CRM
        matched_cross_row_ids = (
            matched_cross_row["crm_transaction_id"].unique()
            if not matched_cross_row.empty
            else []
        )
        final_unmatched_row_crm = unmatched_row_crm_local[
            ~unmatched_row_crm_local["crm_transaction_id"].isin(matched_cross_row_ids)
        ].copy()
        # Add processor columns as NaN to unmatched ROW CRM if not empty
        if not final_unmatched_row_crm.empty:
            proc_cols = [
                col
                for col in row_proc.columns
                if col not in final_unmatched_row_crm.columns
            ]
            for col in proc_cols:
                final_unmatched_row_crm[col] = np.nan
            final_unmatched_row_crm["match_status"] = 0
        # Unmatched ROW processors: Exclude those matched locally or to UK CRM cross
        matched_row_proc_ids = matched_local_row["proc_transaction_id"].unique()
        unmatched_row_proc = available_row_proc[
            ~available_row_proc["proc_transaction_id"].isin(matched_row_proc_ids)
        ].copy()
        # Add CRM columns as NaN to unmatched ROW proc if not empty
        if not unmatched_row_proc.empty:
            crm_cols = [
                col
                for col in row_crm.columns
                if col not in unmatched_row_proc.columns
            ]
            for col in crm_cols:
                unmatched_row_proc[col] = np.nan
            unmatched_row_proc["match_status"] = 0
        # Combine for ROW file: All ROW CRM (matched + unmatched) + unmatched ROW proc + matched proc (already in all_matched_row)
        row_df = pd.concat(
            [all_matched_row, final_unmatched_row_crm, unmatched_row_proc],
            ignore_index=True,
        )
        row_df = row_df.sort_values("match_status", ascending=False)
        # Move crm_type to front if exists
        if "crm_type" in row_df.columns:
            cols = ["crm_type"] + [col for col in row_df.columns if col != "crm_type"]
            row_df = row_df[cols]
        # Save ROW deposits matching
        row_report_dir = row_dirs["lists_dir"] / date_str
        row_report_dir.mkdir(parents=True, exist_ok=True)
        row_path = row_report_dir / "row_deposits_matching.xlsx"
        row_df.to_excel(row_path, index=False)
        print(f"ROW deposits matching report saved to {row_path}")
    # Now update unmatched_uk_proc to exclude those matched in ROW cross
    if "preliminary_unmatched_uk_proc" in locals():
        row_cross_matched_uk_proc_ids = (
            matched_cross_row["proc_transaction_id"].unique()
            if not matched_cross_row.empty
            else []
        )
        unmatched_uk_proc = preliminary_unmatched_uk_proc[
            ~preliminary_unmatched_uk_proc["proc_transaction_id"].isin(
                row_cross_matched_uk_proc_ids
            )
        ].copy()
        # Add CRM columns as NaN to updated unmatched UK proc if not empty
        if not unmatched_uk_proc.empty:
            crm_cols = [
                col
                for col in uk_crm.columns
                if col not in unmatched_uk_proc.columns
            ]
            for col in crm_cols:
                unmatched_uk_proc[col] = np.nan
            unmatched_uk_proc["match_status"] = 0
        # Combine for UK file with updated unmatched_uk_proc
        uk_df = pd.concat(
            [all_matched_uk, final_unmatched_uk_crm, unmatched_uk_proc],
            ignore_index=True,
        )
        uk_df = uk_df.sort_values("match_status", ascending=False)
        # Move crm_type to front if exists
        if "crm_type" in uk_df.columns:
            cols = ["crm_type"] + [col for col in uk_df.columns if col != "crm_type"]
            uk_df = uk_df[cols]
        # Save UK deposits matching
        uk_report_dir = uk_dirs["lists_dir"] / date_str
        uk_report_dir.mkdir(parents=True, exist_ok=True)
        uk_path = uk_report_dir / "uk_deposits_matching.xlsx"
        uk_df.to_excel(uk_path, index=False)
        print(
            f"UK deposits matching report saved to {uk_path} (updated after ROW cross)"
        )