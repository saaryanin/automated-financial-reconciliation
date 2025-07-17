import time
from src.preprocess_test import process_files_in_parallel,combine_processed_files
from src.config import CRM_DIR, PROCESSOR_DIR, DATA_DIR, PROCESSED_CRM_DIR, PROCESSED_PROCESSOR_DIR
from src.deposits_matcher_test import (
    match_all_processors_in_parallel,
    save_global_crm_unmatched,
    match_deposits
)

start_time = time.time()

# --- Configuration ---
DATE = "2025-05-07"
PROCESSORS = ["paypal", "safecharge", "powercash", "shift4", "skrill", "trustpayments","neteller", "zotapay", "bitpay", "ezeebill", "paymentasia"]

# --- Step 1: Gather files (use DATE for all) ---
crm_files = list(CRM_DIR.glob(f"crm_{DATE}.xlsx"))
paypal_files = list(PROCESSOR_DIR.glob(f"paypal_*{DATE}*.csv"))
safecharge_files = list(PROCESSOR_DIR.glob(f"safecharge_*{DATE}*.xlsx"))
powercash_files = list(PROCESSOR_DIR.glob(f"powercash_*{DATE}*.csv"))
shift4_files = list(PROCESSOR_DIR.glob(f"shift4_*{DATE}*.csv"))
skrill_files = list(PROCESSOR_DIR.glob(f"skrill_*{DATE}*.csv"))
trustpayments_files = list(PROCESSOR_DIR.glob(f"trustpayments_*{DATE}*.csv"))
neteller_files = list(PROCESSOR_DIR.glob(f"neteller_{DATE}.csv"))
neteller_crm_files = list(CRM_DIR.glob(f"crm_{DATE}.xlsx"))
zotapay_crm_files = list(CRM_DIR.glob(f"crm_{DATE}.xlsx"))
zotapay_files = list(PROCESSOR_DIR.glob(f"zotapay_{DATE}.csv*"))
bitpay_files = list(PROCESSOR_DIR.glob(f"bitpay_{DATE}.csv"))
bitpay_crm_files = list(CRM_DIR.glob(f"crm_{DATE}.xlsx"))
ezeebill_files = list(PROCESSOR_DIR.glob(f"ezeebill_{DATE}.csv"))
ezeebill_crm_files = list(CRM_DIR.glob(f"crm_{DATE}.xlsx"))
paymentasia_files = list(PROCESSOR_DIR.glob(f"paymentasia_{DATE}.csv"))
paymentasia_crm_files = list(CRM_DIR.glob(f"crm_{DATE}.xlsx"))

# --- Step 2: Preprocess processor files for deposits ---
process_files_in_parallel(paypal_files, processor_name="paypal", is_crm=False, transaction_type="deposit")
process_files_in_parallel(safecharge_files, processor_name="safecharge", is_crm=False, transaction_type="deposit")
process_files_in_parallel(powercash_files, processor_name="powercash", is_crm=False, transaction_type="deposit")
process_files_in_parallel(shift4_files, processor_name="shift4", is_crm=False, transaction_type="deposit")
process_files_in_parallel(skrill_files, processor_name="skrill", is_crm=False, transaction_type="deposit")
process_files_in_parallel(trustpayments_files, processor_name="trustpayments", is_crm=False, transaction_type="deposit")
process_files_in_parallel(neteller_files, processor_name="neteller", is_crm=False, transaction_type="deposit")
process_files_in_parallel(zotapay_files, processor_name="zotapay", is_crm=False, transaction_type="deposit")
process_files_in_parallel(bitpay_files, processor_name="bitpay", is_crm=False, transaction_type="deposit")
process_files_in_parallel(ezeebill_files, processor_name="ezeebill", is_crm=False, transaction_type="deposit")
process_files_in_parallel(paymentasia_files, processor_name="paymentasia", is_crm=False, transaction_type="deposit")

# --- Step 3: Preprocess CRM files for deposits ---
for processor in PROCESSORS:
    process_files_in_parallel(crm_files, processor_name=processor, is_crm=True, transaction_type="deposit")

# --- Step 3.5: Combine processed files for deposits (no grouping) ---
combine_processed_files(
    date=DATE,
    processors=PROCESSORS,
    transaction_type="deposit",
    exchange_rate_map={},  # Load rates if needed, else empty dict
)

# --- Step 4: Match deposits ---
unmatched_crm_frames = match_all_processors_in_parallel(PROCESSORS, DATE)

# --- Step 5: Save unmatched CRM deposits ---
save_global_crm_unmatched(DATE, unmatched_crm_frames)

end_time = time.time()
print(f"\n⏱️ Total time: {end_time - start_time:.2f} seconds")