from src.preprocess import process_files_in_parallel
from src.config import CRM_DIR, PROCESSOR_DIR
from src.deposits_matcher import (
    match_paypal_deposits,
    match_safecharge_deposits,
    match_powercash_deposits,
    save_global_crm_unmatched,
)

# --- Step 1: Locate all files by processor ---
paypal_files = list(PROCESSOR_DIR.glob("paypal_*.csv"))
safecharge_files = list(PROCESSOR_DIR.glob("safecharge_*.xlsx"))
powercash_files = list(PROCESSOR_DIR.glob("powercash_*.csv"))  # Assuming it's CSV

crm_files = list(CRM_DIR.glob("crm_*.xlsx"))

# --- Step 2: Process raw processor files ---
process_files_in_parallel(paypal_files, processor_name="paypal", is_crm=False, save_clean=True)
process_files_in_parallel(safecharge_files, processor_name="safecharge", is_crm=False, save_clean=True)
process_files_in_parallel(powercash_files, processor_name="powercash", is_crm=False, save_clean=True)

# --- Step 3: Process raw CRM files per processor ---
process_files_in_parallel(crm_files, processor_name="paypal", is_crm=True, save_clean=True)
process_files_in_parallel(crm_files, processor_name="safecharge", is_crm=True, save_clean=True)
process_files_in_parallel(crm_files, processor_name="powercash", is_crm=True, save_clean=True)

# --- Step 4: Match processor <> CRM deposits ---
unmatched_crm_paypal = match_paypal_deposits("2025-05-07")
unmatched_crm_safecharge = match_safecharge_deposits("2025-05-07")
unmatched_crm_powercash = match_powercash_deposits("2025-05-07")
# You'll add `match_powercash_deposits()` soon

# --- Step 5: Save final global unmatched CRM view ---
save_global_crm_unmatched("2025-05-07", [
    unmatched_crm_paypal,
    unmatched_crm_safecharge,
    unmatched_crm_powercash
])
