"""
Copyright 2026 Saar Yanin
Licensed under the Apache License 2.0 with Commons Clause
See LICENSE for details.

Script: reports_creator.py
Description: This script coordinates the entire reconciliation workflow for deposits and withdrawals across ROW and UK regulations. It executes file renaming, copies shared processors, preprocesses CRM and processor files in parallel, combines processed data, performs matching for deposits and withdrawals, handles shifted deposits, and conducts cross-regulation and cross-processor matching, utilizing exchange rates from a CSV file.

Key Features:
- Workflow orchestration: Calls run_renamer to organize raw files, copies shared processors (e.g., trustpayments, shift4) from ROW to UK.
- Preprocessing: Processes CRM subsets and processor files in parallel for deposits/withdrawals, applies PSP name mapping, filters by regulation, appends previous unmatched shifted deposits.
- Combining: Merges processed files with special logic for zotapay and paymentasia withdrawals (grouping by email/last4).
- Matching: Invokes match_deposits_for_date and match_withdrawals_for_date, followed by handle_shifts for shifted deposits, run_cross_regulation_matching, and run_cross_processor_matching.
- Rates handling: Loads exchange rates from rates CSV into a map for currency conversions.
- Validation: Checks CRM for required fields (e.g., 'Name', 'PSP name'), drops invalid rows, logs warnings.
- Date management: Uses forced_date if provided via sys.argv, otherwise extracts from directories; gets previous business day for shifted deposits.
- Edge cases: Handles missing directories/files, time tracking for performance, regulation-specific paths.
- CRITICAL FIX (March 2026): Fixed "processor_names must match length of file_paths if provided" error. When a processor (e.g. xbo) has no file on a given date, we now ONLY add processors that actually exist to both lists (no more appending None). This guarantees perfect length matching every time you switch dates with different file sets.

Dependencies:
- pandas (for rates loading and data operations)
- shutil (for file copying)
- time (for performance timing)
- src.config (for BASE_DIR, TEMP_DIR, setup_dirs_for_reg)
- src.preprocess (for combine_processed_files, process_files_in_parallel, PSP_NAME_MAP, process_crm_subset)
- src.utils (for categorize_regulation, get_previous_business_day)
- src.deposits_matcher (for match_deposits_for_date)
- src.shifts_handler (for main as handle_shifts)
- src.withdrawals_matcher (for match_withdrawals_for_date, run_cross_processor_matching)
- src.cross_regulation_matcher (for run_cross_regulation_matching)
- src.files_renamer (for run_renamer)
"""
import pandas as pd
import shutil
import src.config as config
from src.preprocess import (
    combine_processed_files,
    process_files_in_parallel,
    PSP_NAME_MAP,
    process_crm_subset,
)
from src.config import BASE_DIR, TEMP_DIR
import time
from src.utils import categorize_regulation, get_previous_business_day
from src.deposits_matcher import match_deposits_for_date
from src.shifts_handler import main as handle_shifts
from src.withdrawals_matcher import match_withdrawals_for_date, run_cross_processor_matching
from src.cross_regulation_matcher import run_cross_regulation_matching
from src.files_renamer import run_renamer
import sys


def setup_regulation_structure(regulation, processors, date_str):
    start_time = time.time()
    dirs = config.setup_dirs_for_reg(regulation, create=True)
    reg_crm_filepath = dirs["crm_dir"] / f"crm_{date_str}.xlsx"

    if not reg_crm_filepath.exists():
        print(f"WARNING: No CRM file found for {regulation.upper()} (normal during XBO-only testing).")
        print("Creating rich dummy CRM so processor processing can continue...")
        reg_crm_filepath.parent.mkdir(parents=True, exist_ok=True)

        # ALL columns that are accessed in preprocess_for_regulation + load_crm_file + process_crm_subset
        dummy_df = pd.DataFrame(columns=[
            "Name", "PSP name", "Site (Account) (Account)", "Method of Payment",
            "TP Account", "Internal Comment", "Approved", "First Name (Account) (Account)",
            "Last Name (Account) (Account)", "Email (Account) (Account)", "Amount",
            "Currency", "CC Last 4 Digits", "Created On"
        ])
        dummy_df.to_excel(reg_crm_filepath, index=False)
    else:
        print(f"CRM file found for {regulation.upper()}")

    end_time = time.time()
    print(f"Setup for {regulation.upper()} took {end_time - start_time:.2f} seconds")
    return {**dirs, "crm_filepath": reg_crm_filepath}


row_processors = [
    "paypal",
    "safecharge",
    "powercash",
    "shift4",
    "skrill",
    "neteller",
    "trustpayments",
    "zotapay",
    "bitpay",
    "ezeebill",
    "paymentasia",
    "bridgerpay",
    "xbo",
]
uk_processors = ["safechargeuk", "barclays", "barclaycard"]


def preprocess_for_regulation(
    regulation, transaction_type="deposit", dirs=None, date_str=None
):
    start_time = time.time()
    processors = row_processors if regulation == "row" else uk_processors
    if dirs is None:
        dirs = setup_regulation_structure(regulation, processors, date_str)

    # === LOAD RAW CRM FIRST (before any append) ===
    crm_df = pd.read_excel(dirs["crm_filepath"], engine="openpyxl")
    crm_df.columns = crm_df.columns.str.strip()

    # Regulation and PSP name normalization (before determining processors)
    crm_df["regulation"] = crm_df["Site (Account) (Account)"].apply(categorize_regulation)
    if regulation == "row":
        row_regs = ["mauritius", "cyprus", "australia", "dubai"]
        crm_df = crm_df[crm_df["regulation"].isin(row_regs)]
        mask_aus = crm_df["regulation"] == "australia"
        mask_psp = crm_df["PSP name"].str.lower().isin(["paypal", "inpendium"])
        crm_df = crm_df[~(mask_aus & mask_psp)]
    elif regulation == "uk":
        crm_df = crm_df[crm_df["regulation"] == "uk"]

    crm_df["PSP name"] = (
        crm_df["PSP name"].astype(str).str.strip().str.lower().replace(PSP_NAME_MAP)
    )
    if regulation == "uk":
        crm_df["PSP name"] = crm_df["PSP name"].replace({"safecharge": "safechargeuk"})

    # Neteller and XBO overrides
    if "Method of Payment" in crm_df.columns:
        neteller_mask = crm_df["Method of Payment"].astype(str).str.strip().str.lower() == "neteller"
        crm_df.loc[neteller_mask, "PSP name"] = "neteller"
        xbo_mask = crm_df["Method of Payment"].astype(str).str.strip().str.upper() == "XBO"
        crm_df.loc[xbo_mask, "PSP name"] = "xbo"
    if "PSP name" in crm_df.columns:
        xbo_name_mask = crm_df["PSP name"].astype(str).str.strip().str.upper() == "XBO"
        crm_df.loc[xbo_name_mask, "PSP name"] = "xbo"

    # === DETERMINE FILTERED PROCESSORS BASED ON CURRENT DAY ONLY ===
    name_mask = crm_df["Name"].str.lower() == transaction_type
    unique_psps = set(crm_df[name_mask]["PSP name"].dropna().unique())

    # Check which raw processor files actually exist for this exact date (strict!)
    potential_processors = processors + ([p for p in row_processors if p not in ["safecharge"]] if regulation == "uk" else [])
    has_raw_file = {}
    for proc in potential_processors:
        has_raw_file[proc] = any(
            (dirs["processor_dir"] / f"{proc}_{date_str}.{ext}").exists()
            for ext in ["xlsx", "csv", "xls"]
        )

    filtered_processors = [p for p in processors if p in unique_psps or has_raw_file.get(p, False)]
    if regulation == "uk":
        additional = [p for p in row_processors if p not in ["safecharge"] and (p in unique_psps or has_raw_file.get(p, False))]
        filtered_processors += additional
    filtered_processors = list(set(filtered_processors))

    print(f"Filtered processors for {regulation.upper()} {date_str}: {filtered_processors}")

    # Now filter crm_df to only active processors for today
    crm_df = crm_df[crm_df["PSP name"].isin(filtered_processors)]

    # Data validation
    invalid_rows = crm_df[crm_df["Name"].isna() | crm_df["PSP name"].isna()]
    if not invalid_rows.empty:
        print(
            f"Warning: {len(invalid_rows)} CRM rows with missing 'Name' or 'PSP name' in {regulation.upper()} - dropping them."
        )
    crm_df = crm_df.dropna(subset=["Name", "PSP name"])

    # === NOW APPEND PREVIOUS UNMATCHED (only for processors active TODAY) ===
    if transaction_type == "deposit":
        previous_date_str = get_previous_business_day(date_str)
        prev_unmatched_path = (
            dirs["lists_dir"] / previous_date_str / f"{regulation}_unmatched_shifted_deposits.xlsx"
        )
        if prev_unmatched_path.exists():
            prev_df = pd.read_excel(prev_unmatched_path, engine="openpyxl")
            prev_df.columns = prev_df.columns.str.strip()

            # Critical: only bring in shifted rows for processors we are processing today
            if 'crm_processor_name' in prev_df.columns:
                prev_df = prev_df[prev_df['crm_processor_name'].isin(filtered_processors)]
            elif 'PSP name' in prev_df.columns:
                prev_df = prev_df[prev_df['PSP name'].isin(filtered_processors)]

            if not prev_df.empty:
                crm_df = pd.concat([crm_df, prev_df], ignore_index=True)
                print(f"Appended {len(prev_df)} rows from previous unmatched shifted deposits for {regulation} (filtered to active processors)")

    # === CRM SUBSET PROCESSING (now only for filtered_processors) ===
    crm_start = time.time()
    processed_crm_dfs = []
    for proc in filtered_processors:
        mask = crm_df["Name"].str.lower() == transaction_type
        psp_mask = crm_df["PSP name"] == proc
        subset = crm_df[mask & psp_mask].copy()
        if regulation == "uk" and proc == "safechargeuk":
            subset["PSP name"] = "safecharge"
        processed_subset = process_crm_subset(
            subset,
            proc,
            regulation,
            transaction_type,
            True,
            dirs["processed_crm_dir"],
            date_str,
        )
        if processed_subset is not None:
            processed_crm_dfs.append(processed_subset)
    crm_end = time.time()
    print(
        f"CRM processing for {regulation.upper()} {transaction_type} took {crm_end - crm_start:.2f} seconds"
    )

    # === Processor file processing (ONLY processors that actually have files - THIS FIXES THE LENGTH MISMATCH) ===
    proc_start = time.time()
    processor_file_paths = []
    processor_names_for_parallel = []
    for proc in filtered_processors:
        for ext in ["xlsx", "csv", "xls"]:
            proc_file = dirs["processor_dir"] / f"{proc}_{date_str}.{ext}"
            if proc_file.exists():
                processor_file_paths.append(proc_file)
                processor_names_for_parallel.append(proc)
                break
    # NO MORE "append None" → lengths are ALWAYS equal. This is the exact fix for your error.
    processed_proc_dfs = process_files_in_parallel(
        processor_file_paths,
        processor_names=processor_names_for_parallel,
        is_crm=False,
        save_clean=True,
        transaction_type=transaction_type,
        regulation=regulation,
        processed_processor_dir=dirs["processed_processor_dir"],
    )
    proc_end = time.time()
    print(
        f"Processor processing for {regulation.upper()} {transaction_type} took {proc_end - proc_start:.2f} seconds"
    )

    # Special combine for zotapay and paymentasia for withdrawals (unchanged)
    if (
        transaction_type == "withdrawal"
        and "zotapay" in filtered_processors
        and "paymentasia" in filtered_processors
    ):
        zotapay_file = (
            dirs["processed_processor_dir"]
            / "zotapay"
            / date_str
            / "zotapay_withdrawals.xlsx"
        )
        paymentasia_file = (
            dirs["processed_processor_dir"]
            / "paymentasia"
            / date_str
            / "paymentasia_withdrawals.xlsx"
        )
        combined_out_dir = (
            dirs["processed_processor_dir"] / "zotapay_paymentasia" / date_str
        )
        combined_out_file = combined_out_dir / "zotapay_paymentasia_withdrawals.xlsx"
        zota_df = (
            pd.read_excel(zotapay_file) if zotapay_file.exists() else pd.DataFrame()
        )
        pa_df = (
            pd.read_excel(paymentasia_file)
            if paymentasia_file.exists()
            else pd.DataFrame()
        )
        combined_df = pd.concat([zota_df, pa_df], ignore_index=True)
        if not combined_df.empty:
            combined_out_dir.mkdir(parents=True, exist_ok=True)
            combined_df.to_excel(combined_out_file, index=False)
            print(
                f"Combined Zotapay + PaymentAsia withdrawals saved to {combined_out_file}"
            )

    combine_start = time.time()
    extra_processors = (
        ["zotapay_paymentasia"]
        if transaction_type == "withdrawal"
        and "zotapay" in filtered_processors
        and "paymentasia" in filtered_processors
        else []
    )
    combine_processed_files(
        date_str,
        filtered_processors,
        processed_crm_dir=dirs["processed_crm_dir"],
        processed_proc_dir=dirs["processed_processor_dir"],
        out_crm_dir=dirs["combined_crm_dir"],
        out_proc_dir=dirs["processed_processor_dir"] / "combined",
        transaction_type=transaction_type,
        regulation=regulation,
        crm_dir=dirs["crm_dir"],
        extra_processors=extra_processors,
    )
    combine_end = time.time()
    print(
        f"Combining for {regulation.upper()} {transaction_type} took {combine_end - combine_start:.2f} seconds"
    )

    end_time = time.time()
    print(
        f"Preprocessed and combined {transaction_type}s for {regulation.upper()} regulation saved successfully. Total time: {end_time - start_time:.2f} seconds."
    )


def main(date_str):
    # Run the renamer first to process raw files into regulation-specific dirs
    run_renamer(forced_date=date_str)  # Optionally pass forced_date if needed for fallback

    # Copy shared processor files to UK processor_reports (after renamer has placed them in ROW)
    shared_processors = [
        "trustpayments",
        "shift4",
        "skrill",
        "powercash",
        "paypal",
        "neteller",
    ]
    row_processor_dir = TEMP_DIR / "ROW" / "data" / "processor_reports"
    uk_processor_dir = TEMP_DIR / "UK" / "data" / "processor_reports"
    uk_processor_dir.mkdir(parents=True, exist_ok=True)
    for shared in shared_processors:
        for ext in [".xlsx", ".csv", ".xls"]:
            src_file = row_processor_dir / f"{shared}_{date_str}{ext}"
            if src_file.exists():
                dst_file = uk_processor_dir / src_file.name
                shutil.copy(src_file, dst_file)
                print(f"Copied shared processor file: {src_file} to {dst_file}")
                break

    overall_start = time.time()
    for reg in ["row", "uk"]:
        processors = row_processors if reg == "row" else uk_processors
        dirs = setup_regulation_structure(reg, processors, date_str)
        preprocess_for_regulation(reg, "deposit", dirs=dirs, date_str=date_str)
        preprocess_for_regulation(reg, "withdrawal", dirs=dirs, date_str=date_str)
    overall_end = time.time()
    print(f"Overall processing time: {overall_end - overall_start:.2f} seconds")
    match_deposits_for_date(date_str)
    matched_sums = handle_shifts(date_str)
    if matched_sums:
        print("Matched Shifted Deposits by Currency:")
        for reg, sums in matched_sums.items():
            print(f"{reg.upper()}:")
            for currency, amount in sums.items():
                print(f" {currency}: {amount}")
    rates_path = BASE_DIR / "data" / "rates" / f"rates_{date_str}.csv"
    if rates_path.exists():
        rates_df = pd.read_csv(rates_path)
        rates_df["from_currency"] = rates_df["from_currency"].str.strip()
        rates_df["to_currency"] = rates_df["to_currency"].str.strip()
        exchange_rate_map = {
            (row["from_currency"], row["to_currency"]): row["rate"]
            for _, row in rates_df.iterrows()
        }
    else:
        exchange_rate_map = {}
        print("No rates file found; using empty exchange rate map.")
    match_withdrawals_for_date(date_str, exchange_rate_map)
    run_cross_regulation_matching(date_str, exchange_rate_map)
    run_cross_processor_matching(date_str, exchange_rate_map)


if __name__ == "__main__":
    date_str = "2026-03-06"
    if len(sys.argv) > 1:
        date_str = sys.argv[1]
    main(date_str)