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

# Writable dirs under TEMP_DIR (definitions only; creation handled in scripts)
DATA_DIR = TEMP_DIR / "data"
OUTPUT_DIR = TEMP_DIR / "output"
CRM_DIR = DATA_DIR / "crm_reports"
PROCESSOR_DIR = DATA_DIR / "processor_reports"
RAW_TRACKING_DIR = DATA_DIR / "raw_tracking_lists"
RAW_ATTACHED_FILES = TEMP_DIR / "raw_attached_files"
PROCESSED_DIR = DATA_DIR / "processed"
PROCESSED_CRM_DIR = PROCESSED_DIR / "crm"
PROCESSED_PROCESSOR_DIR = PROCESSED_DIR / "processors"
RATES_DIR = DATA_DIR / "rates"
LISTS_DIR = DATA_DIR / "lists"
COMBINED_CRM_DIR = PROCESSED_CRM_DIR / "combined"
PROCESSED_UNMATCHED_SHIFTED_DEPOSITS_DIR = PROCESSED_CRM_DIR / "unmatched_shifted_deposits"
TRAINING_DATASET_DIR = DATA_DIR / "training_dataset"
TRUE_TRAINING_DIR = TRAINING_DATASET_DIR / "check_newversion_datasets"
FALSE_TRAINING_DIR = TRAINING_DATASET_DIR / "false_training_datasets"
TEST_MODEL_DIR = TRAINING_DATASET_DIR / "model_testing"

# Remove the for loop that creates dirs automatically
# for dir_path in [...]:
#     dir_path.mkdir(parents=True, exist_ok=True)