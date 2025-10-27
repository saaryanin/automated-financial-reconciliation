# config.py (Updated to handle base TEMP_DIR without regulation-specific nesting)
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

# Writable dirs under TEMP_DIR (no regulation here; handled in scripts)
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

# Ensure all dirs exist
for dir_path in [TEMP_DIR, DATA_DIR, OUTPUT_DIR, CRM_DIR, PROCESSOR_DIR, RAW_TRACKING_DIR, RAW_ATTACHED_FILES,
                 PROCESSED_DIR, PROCESSED_CRM_DIR, PROCESSED_PROCESSOR_DIR, RATES_DIR, LISTS_DIR, COMBINED_CRM_DIR,
                 PROCESSED_UNMATCHED_SHIFTED_DEPOSITS_DIR, TRAINING_DATASET_DIR, TRUE_TRAINING_DIR, FALSE_TRAINING_DIR, TEST_MODEL_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)