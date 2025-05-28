import time
from src.preprocess import process_files_in_parallel
from src.config import CRM_DIR, PROCESSOR_DIR
from src.deposits_matcher import (
    match_all_processors_in_parallel,
    save_global_crm_unmatched,
    match_deposits
)

start_time = time.time()

# --- Configuration ---
DATE = "2025-05-07"
NETELLER_DATE = "2025-05-19"
ZOTAPAY_DATE = "2025-03-28"
BITPAY_DATE = "2025-05-16"
PROCESSORS = ["paypal", "safecharge", "powercash", "shift4", "skrill", "trustpayments"]

# --- Step 1: Gather files ---
crm_files = list(CRM_DIR.glob("crm_2025-05-07.xlsx"))  # Only process relevant date
paypal_files = list(PROCESSOR_DIR.glob("paypal_*.csv"))
safecharge_files = list(PROCESSOR_DIR.glob("safecharge_*.xlsx"))
powercash_files = list(PROCESSOR_DIR.glob("powercash_*.csv"))
shift4_files = list(PROCESSOR_DIR.glob("shift4_*.csv"))
skrill_files = list(PROCESSOR_DIR.glob("skrill_*.csv"))
trustpayments_files = list(PROCESSOR_DIR.glob("trustpayments_*.csv"))
neteller_files = list(PROCESSOR_DIR.glob("neteller_2025-05-19.csv"))
neteller_crm_files = list(CRM_DIR.glob("crm_2025-05-19.xlsx"))
zotapay_crm_files = list(CRM_DIR.glob("crm_2025-03-28.xlsx"))
zotapay_files = list(PROCESSOR_DIR.glob("zotapay_2025-03-28.csv*"))
bitpay_files = list(PROCESSOR_DIR.glob("bitpay_2025-05-16.csv"))
bitpay_crm_files = list(CRM_DIR.glob("crm_2025-05-16.xlsx"))

# --- Step 2: Preprocess processor files ---
process_files_in_parallel(paypal_files, processor_name="paypal", is_crm=False)
process_files_in_parallel(safecharge_files, processor_name="safecharge", is_crm=False)
process_files_in_parallel(powercash_files, processor_name="powercash", is_crm=False)
process_files_in_parallel(shift4_files, processor_name="shift4", is_crm=False)
process_files_in_parallel(skrill_files, processor_name="skrill", is_crm=False)
process_files_in_parallel(trustpayments_files, processor_name="trustpayments", is_crm=False)
process_files_in_parallel(neteller_files, processor_name="neteller", is_crm=False)
process_files_in_parallel(zotapay_files, processor_name="zotapay", is_crm=False)
process_files_in_parallel(bitpay_files, processor_name="bitpay", is_crm=False)

# --- Step 3: Preprocess CRM files ---
for processor in PROCESSORS:
    process_files_in_parallel(crm_files, processor_name=processor, is_crm=True)
process_files_in_parallel(neteller_crm_files, processor_name="neteller", is_crm=True)
process_files_in_parallel(zotapay_crm_files, processor_name="zotapay", is_crm=True)
process_files_in_parallel(bitpay_crm_files, processor_name="bitpay", is_crm=True)

# --- Step 4: Match deposits ---
unmatched_crm_frames = match_all_processors_in_parallel(PROCESSORS, DATE)
unmatched_crm_neteller = match_deposits("neteller", NETELLER_DATE)
unmatched_crm_zotapay = match_deposits("zotapay", ZOTAPAY_DATE)
unmatched_crm_bitpay = match_deposits("bitpay", BITPAY_DATE)

# --- Step 5: Save unmatched CRM deposits ---
save_global_crm_unmatched(DATE, unmatched_crm_frames + [unmatched_crm_neteller, unmatched_crm_zotapay])
save_global_crm_unmatched(DATE, unmatched_crm_frames + [unmatched_crm_neteller, unmatched_crm_zotapay, unmatched_crm_bitpay])

end_time = time.time()
print(f"\n⏱️ Total time: {end_time - start_time:.2f} seconds")
