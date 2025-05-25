import time
from src.preprocess import process_files_in_parallel
from src.config import CRM_DIR, PROCESSOR_DIR
from src.deposits_matcher import (
    match_all_processors_in_parallel,
    save_global_crm_unmatched
)

start_time = time.time()
# --- Step 1: Locate raw files ---
paypal_files = list(PROCESSOR_DIR.glob("paypal_*.csv"))
safecharge_files = list(PROCESSOR_DIR.glob("safecharge_*.xlsx"))
powercash_files = list(PROCESSOR_DIR.glob("powercash_*.csv"))
shift4_files = list(PROCESSOR_DIR.glob("shift4_*.csv"))
crm_files = list(CRM_DIR.glob("crm_*.xlsx"))

# --- Step 2: Process all processor files ---
process_files_in_parallel(paypal_files, processor_name="paypal", is_crm=False, save_clean=True)
process_files_in_parallel(safecharge_files, processor_name="safecharge", is_crm=False, save_clean=True)
process_files_in_parallel(powercash_files, processor_name="powercash", is_crm=False, save_clean=True)
process_files_in_parallel(shift4_files, processor_name="shift4", is_crm=False, save_clean=True)

# --- Step 3: Process all CRM files ---
process_files_in_parallel(crm_files, processor_name="paypal", is_crm=True, save_clean=True)
process_files_in_parallel(crm_files, processor_name="safecharge", is_crm=True, save_clean=True)
process_files_in_parallel(crm_files, processor_name="powercash", is_crm=True, save_clean=True)
process_files_in_parallel(crm_files, processor_name="shift4", is_crm=True, save_clean=True)

# --- Step 4: Match all processors in parallel ---
unmatched_crm_frames = match_all_processors_in_parallel(
    ["paypal", "safecharge", "powercash", "shift4"], "2025-05-07"
)

# --- Step 5: Save final unmatched CRM results ---
save_global_crm_unmatched("2025-05-07", unmatched_crm_frames)

end_time = time.time()
print(f"\n⏱️ Total time: {end_time - start_time:.2f} seconds")
