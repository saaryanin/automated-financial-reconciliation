from pathlib import Path

# Base directories
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
MODEL_DIR = BASE_DIR / "model"

# Raw input directories
CRM_DIR = DATA_DIR / "crm_reports"
PROCESSOR_DIR = DATA_DIR / "processor_reports"
RAW_TRACKING_DIR = DATA_DIR / "raw_tracking_lists"
RAW_ATTACHED_FILES = DATA_DIR / "raw_attached_files"

# Preprocessed data directories
PROCESSED_DIR = DATA_DIR / "processed"
PROCESSED_CRM_DIR = PROCESSED_DIR / "crm"
PROCESSED_PROCESSOR_DIR = PROCESSED_DIR / "processors"
RATES_DIR = DATA_DIR / "rates"
LISTS_DIR =  DATA_DIR /"lists"

# Training dataset directories
TRAINING_DATASET_DIR = DATA_DIR / "training_dataset"
TRUE_TRAINING_DIR = TRAINING_DATASET_DIR / "check_newversion_datasets"
FALSE_TRAINING_DIR = TRAINING_DATASET_DIR / "false_training_datasets"
TEST_MODEL_DIR =  TRAINING_DATASET_DIR /"model_testing"



# Ensure model directory exists
MODEL_DIR.mkdir(parents=True, exist_ok=True)
