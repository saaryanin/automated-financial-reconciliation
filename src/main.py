from src.preprocess import process_files_in_parallel
from src.config import CRM_DIR, PROCESSOR_DIR
from src.deposits_matcher import match_paypal_deposits

# Example CRM and PayPal file lists
crm_files = list(CRM_DIR.glob("*.xlsx"))
paypal_files = list(PROCESSOR_DIR.glob("paypal_*.csv"))
match_paypal_deposits("2025-05-07")

# Process all PayPal processor files in parallel
process_files_in_parallel(paypal_files, processor_name="paypal", is_crm=False, save_clean=True)

# Process all CRM files in parallel
process_files_in_parallel(crm_files, is_crm=True, save_clean=True)
