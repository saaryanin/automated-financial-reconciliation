import time
from src.preprocess import process_files_in_parallel
from src.config import CRM_DIR, PROCESSOR_DIR
from src.deposits_matcher import (
    match_all_processors_in_parallel,
    save_global_crm_unmatched
)

start_time = time.time()
# --- Configuration ---
DATE = "2025-05-07"
PROCESSORS = ["paypal", "safecharge", "powercash", "shift4", "skrill"]

# --- Step 1: Gather files ---
crm_files = list(CRM_DIR.glob("crm_*.xlsx"))
paypal_files = list(PROCESSOR_DIR.glob("paypal_*.csv"))
safecharge_files = list(PROCESSOR_DIR.glob("safecharge_*.xlsx"))
powercash_files = list(PROCESSOR_DIR.glob("powercash_*.csv"))
shift4_files = list(PROCESSOR_DIR.glob("shift4_*.csv"))
skrill_files = list(PROCESSOR_DIR.glob("skrill_*.csv"))

# --- Step 2: Preprocess processor files ---
process_files_in_parallel(paypal_files, processor_name="paypal", is_crm=False, save_clean=True)
process_files_in_parallel(safecharge_files, processor_name="safecharge", is_crm=False, save_clean=True)
process_files_in_parallel(powercash_files, processor_name="powercash", is_crm=False, save_clean=True)
process_files_in_parallel(shift4_files, processor_name="shift4", is_crm=False, save_clean=True)
process_files_in_parallel(skrill_files, processor_name="skrill", is_crm=False, save_clean=True)

# --- Step 3: Preprocess CRM files per processor ---
for processor in PROCESSORS:
    process_files_in_parallel(crm_files, processor_name=processor, is_crm=True, save_clean=True)

# --- Step 4: Match deposits in parallel ---
unmatched_crm_frames = match_all_processors_in_parallel(PROCESSORS, DATE)

# --- Step 5: Save global unmatched CRM deposits ---
save_global_crm_unmatched(DATE, unmatched_crm_frames)

end_time = time.time()
print(f"\n⏱️ Total time: {end_time - start_time:.2f} seconds")
