from src.preprocess import process_files_in_parallel
from src.config import CRM_DIR, PROCESSOR_DIR
from src.deposits_matcher import (
    match_paypal_deposits,
    match_safecharge_deposits,
    save_global_crm_unmatched
)

# --- Step 1: Locate files ---
crm_files = list(CRM_DIR.glob("*.xlsx"))
paypal_files = list(PROCESSOR_DIR.glob("paypal_*.csv"))
safecharge_files = list(PROCESSOR_DIR.glob("safecharge_*.xlsx"))
safecharge_crm_files = list(CRM_DIR.glob("crm_*.xlsx"))

# --- Step 2: Process all raw files ---
process_files_in_parallel(paypal_files, processor_name="paypal", is_crm=False, save_clean=True)
process_files_in_parallel(safecharge_files, processor_name="safecharge", is_crm=False)

process_files_in_parallel(crm_files, processor_name="paypal", is_crm=True, save_clean=True)
process_files_in_parallel(safecharge_crm_files, processor_name="safecharge", is_crm=True, save_clean=True)

# --- Step 3: Match deposits ---
unmatched_crm_paypal = match_paypal_deposits("2025-05-07")
unmatched_crm_safecharge = match_safecharge_deposits("2025-05-07")

# --- Step 4: Save global unmatched CRM list ---
save_global_crm_unmatched("2025-05-07", [unmatched_crm_paypal, unmatched_crm_safecharge])
