"""
Copyright 2026 Saar Yanin
Licensed under the Apache License 2.0 with Commons Clause
See LICENSE for details.

Script: config.py
Description: This script establishes the base and temporary directory structures for the reconciliation application, accommodating both development and frozen (e.g., PyInstaller) environments. It defines global read-only directories and includes a function to set up and return regulation-specific directory paths for data, output, and processing, ensuring organized file management across ROW and UK regulations.

Key Features:
- Environment detection: Sets BASE_DIR to the executable's directory in frozen mode (using sys._MEIPASS) or the script's parent in development; configures TEMP_DIR as a system temporary subdirectory in frozen mode or a local 'temp' folder in dev.
- Global directories: Defines paths like RAW_ATTACHED_FILES (creates if missing), CRM_DIR, PROCESSOR_DIR, and RATES_DIR for shared resources.
- setup_dirs_for_reg function: Takes a regulation ('ROW' or 'UK'), optionally creates directories, and returns a dictionary of paths including root, data, output, crm, processors, processed subdirs for crm and processors, rates, lists (with date subdir), combined_crm, unmatched_shifted_deposits, and training datasets.
- Regulation handling: Converts regulation input to uppercase for consistent folder naming (e.g., 'row' becomes 'ROW').
- Edge cases: Ensures all directories are created only if create=True, handles path resolution for consistency across environments.

Dependencies:
- sys (for frozen environment detection via _MEIPASS)
- pathlib (for robust path creation and manipulation)
- tempfile (for creating temporary directories in frozen mode)
"""

import sys
from pathlib import Path
import tempfile

if getattr(sys, "frozen", False):
    BASE_DIR = Path(sys._MEIPASS)
    TEMP_DIR = Path(tempfile.gettempdir()) / "ReconciliationSystem_temp"
    TEMP_DIR.mkdir(exist_ok=True)
else:
    BASE_DIR = Path(__file__).resolve().parent.parent
    TEMP_DIR = BASE_DIR / "temp"

# Read-only dirs stay under BASE_DIR
MODEL_DIR = BASE_DIR / "model"
FRONTEND_DIR = BASE_DIR / "frontend"
RAW_ATTACHED_FILES = TEMP_DIR / "raw_attached_files"  # ← Global shared folder
RAW_ATTACHED_FILES.mkdir(parents=True, exist_ok=True)
CRM_DIR = BASE_DIR / "data" / "crm_reports"
PROCESSOR_DIR = BASE_DIR / "data" / "processor_reports"
RATES_DIR = BASE_DIR / "data" / "rates"
RATES_DIR.mkdir(parents=True, exist_ok=True)


def setup_dirs_for_reg(regulation, create=True):
    """Return a dict of directories for a given regulation and optionally create them."""
    reg_upper = regulation.upper()
    reg_root = TEMP_DIR / reg_upper
    reg_data_dir = reg_root / "data"
    reg_output_dir = reg_root / "output"
    reg_raw_attached_files = reg_root / "raw_attached_files"

    # Subdirs
    reg_crm_dir = reg_data_dir / "crm_reports"
    reg_processor_dir = reg_data_dir / "processor_reports"
    reg_raw_tracking_dir = reg_data_dir / "raw_tracking_lists"
    reg_processed_dir = reg_data_dir / "processed"
    reg_processed_crm_dir = reg_processed_dir / "crm"
    reg_processed_processor_dir = reg_processed_dir / "processors"
    reg_rates_dir = reg_data_dir / "rates"
    reg_lists_dir = reg_data_dir / "lists"
    reg_combined_crm_dir = reg_processed_crm_dir / "combined"
    reg_processed_unmatched_shifted_deposits_dir = (
        reg_processed_crm_dir / "unmatched_shifted_deposits"
    )
    reg_training_dataset_dir = reg_data_dir / "training_dataset"
    reg_true_training_dir = reg_training_dataset_dir / "check_newversion_datasets"
    reg_false_training_dir = reg_training_dataset_dir / "false_training_datasets"
    reg_test_model_dir = reg_training_dataset_dir / "model_testing"

    dirs = {
        "root": reg_root,
        "data_dir": reg_data_dir,
        "output_dir": reg_output_dir,
        "crm_dir": reg_crm_dir,
        "processor_dir": reg_processor_dir,
        "raw_tracking_dir": reg_raw_tracking_dir,
        "processed_dir": reg_processed_dir,
        "processed_crm_dir": reg_processed_crm_dir,
        "processed_processor_dir": reg_processed_processor_dir,
        "rates_dir": reg_rates_dir,
        "lists_dir": reg_lists_dir,
        "combined_crm_dir": reg_combined_crm_dir,
        "processed_unmatched_shifted_deposits_dir": reg_processed_unmatched_shifted_deposits_dir,
        "training_dataset_dir": reg_training_dataset_dir,
        "true_training_dir": reg_true_training_dir,
        "false_training_dir": reg_false_training_dir,
        "test_model_dir": reg_test_model_dir,
    }

    if create:
        for d in dirs.values():
            d.mkdir(parents=True, exist_ok=True)

    return dirs