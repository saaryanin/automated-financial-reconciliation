# config.py (Updated to remove automatic dir creation at top-level; dirs are created per regulation in scripts)
import sys
from pathlib import Path
import tempfile

if getattr(sys, 'frozen', False):
    BASE_DIR = Path(sys._MEIPASS)
    TEMP_DIR = BASE_DIR / "temp"
    TEMP_DIR.mkdir(exist_ok=True)
else:
    BASE_DIR = Path(__file__).resolve().parent.parent
    TEMP_DIR = BASE_DIR / "temp"

# Read-only dirs stay under BASE_DIR
MODEL_DIR = BASE_DIR / "model"
FRONTEND_DIR = BASE_DIR / "frontend"

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
    reg_processed_unmatched_shifted_deposits_dir = reg_processed_crm_dir / "unmatched_shifted_deposits"
    reg_training_dataset_dir = reg_data_dir / "training_dataset"
    reg_true_training_dir = reg_training_dataset_dir / "check_newversion_datasets"
    reg_false_training_dir = reg_training_dataset_dir / "false_training_datasets"
    reg_test_model_dir = reg_training_dataset_dir / "model_testing"

    dirs = {
        'root': reg_root,
        'data_dir': reg_data_dir,
        'output_dir': reg_output_dir,
        'raw_attached_files': reg_raw_attached_files,
        'crm_dir': reg_crm_dir,
        'processor_dir': reg_processor_dir,
        'raw_tracking_dir': reg_raw_tracking_dir,
        'processed_dir': reg_processed_dir,
        'processed_crm_dir': reg_processed_crm_dir,
        'processed_processor_dir': reg_processed_processor_dir,
        'rates_dir': reg_rates_dir,
        'lists_dir': reg_lists_dir,
        'combined_crm_dir': reg_combined_crm_dir,
        'processed_unmatched_shifted_deposits_dir': reg_processed_unmatched_shifted_deposits_dir,
        'training_dataset_dir': reg_training_dataset_dir,
        'true_training_dir': reg_true_training_dir,
        'false_training_dir': reg_false_training_dir,
        'test_model_dir': reg_test_model_dir,
    }

    if create:
        for d in dirs.values():
            d.mkdir(parents=True, exist_ok=True)

    return dirs