import pandas as pd
from pathlib import Path
from src.config import PROCESSED_CRM_DIR, LISTS_DIR,COMBINED_CRM_DIR,PROCESSED_UNMATCHED_SHIFTED_DEPOSITS_DIR  # Adjust if config.py has different paths

# Set the date (use the date from your reports_creator or command-line)
DATE = "2025-09-02"  # Replace with actual date if needed

# Paths to the files
combined_crm_path = COMBINED_CRM_DIR / DATE / "combined_crm_deposits.xlsx"
unmatched_shifted_path = PROCESSED_UNMATCHED_SHIFTED_DEPOSITS_DIR / DATE / "unmatched_shifted_deposits.xlsx"  # Assuming this is where handle_shifts saves it; adjust if different

# Or if it's in PROCESSED_CRM_DIR / "unmatched_shifted_deposits" / DATE / "unmatched_shifted_deposits.xlsx"
# unmatched_shifted_path = PROCESSED_CRM_DIR / "unmatched_shifted_deposits" / DATE / "unmatched_shifted_deposits.xlsx"

def append_unmatched_to_combined(combined_path: Path, unmatched_path: Path, output_path: Path = None):
    if not combined_path.exists():
        print(f"Combined CRM file not found: {combined_path}")
        return
    if not unmatched_path.exists():
        print(f"Unmatched shifted deposits file not found: {unmatched_path}")
        return

    # Load combined_crm_deposits
    df_combined = pd.read_excel(combined_path, dtype={'crm_transaction_id': str})
    print(f"Original combined_crm_deposits shape: {df_combined.shape}")

    # Load unmatched_shifted_deposits (headers are read, but we append the data as-is)
    df_unmatched = pd.read_excel(unmatched_path, dtype={'crm_transaction_id': str})
    print(f"Unmatched shifted deposits shape: {df_unmatched.shape}")

    # Check if columns match (approximately, allowing for minor differences)
    combined_cols = set(df_combined.columns)
    unmatched_cols = set(df_unmatched.columns)
    if combined_cols != unmatched_cols:
        print(f"Warning: Column mismatch between combined ({combined_cols}) and unmatched ({unmatched_cols}). Appending anyway.")

    # Get set of existing crm_transaction_id in combined
    existing_ids = set(df_combined['crm_transaction_id'].dropna().unique())

    # Filter unmatched rows where crm_transaction_id not in existing_ids
    if 'crm_transaction_id' in df_unmatched.columns:
        df_unmatched_to_append = df_unmatched[~df_unmatched['crm_transaction_id'].isin(existing_ids)]
    else:
        df_unmatched_to_append = df_unmatched  # If no transaction_id column, append all

    print(f"Rows to append after filtering: {df_unmatched_to_append.shape[0]}")

    # Append the filtered unmatched to combined
    if not df_unmatched_to_append.empty:
        df_updated = pd.concat([df_combined, df_unmatched_to_append], ignore_index=True)

        # Drop duplicates if any (based on key columns like crm_transaction_id, crm_tp, etc.)
        key_cols = ['crm_transaction_id', 'crm_tp', 'crm_email', 'crm_amount']  # Adjust keys as needed
        df_updated = df_updated.drop_duplicates(subset=[col for col in key_cols if col in df_updated.columns])

        print(f"Updated combined_crm_deposits shape after append and dedup: {df_updated.shape}")
    else:
        df_updated = df_combined
        print("No new rows to append.")

    # Save back to the original path or a new one
    if output_path is None:
        output_path = combined_path
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        df_updated.to_excel(writer, index=False, sheet_name='Sheet1')
        if 'crm_transaction_id' in df_updated.columns:
            worksheet = writer.sheets['Sheet1']
            trans_col = df_updated.columns.get_loc('crm_transaction_id') + 1
            for row in range(2, len(df_updated) + 2):
                worksheet.cell(row=row, column=trans_col).number_format = '@'
    print(f"Appended unmatched shifted deposits to {output_path}")

# Run the append
append_unmatched_to_combined(combined_crm_path, unmatched_shifted_path)

# Optional: To save to a new file for checking without overwriting
# append_unmatched_to_combined(combined_crm_path, unmatched_shifted_path, output_path=combined_crm_path.parent / "combined_crm_deposits_updated.xlsx")