import sys
import os
from pathlib import Path

if getattr(sys, 'frozen', False):
    BASE_DIR = Path(sys._MEIPASS)
else:
    BASE_DIR = Path(__file__).resolve().parent.parent

# Base directories
DATA_DIR = BASE_DIR / "data"
MODEL_DIR = BASE_DIR / "model"
FRONTEND_DIR = BASE_DIR / "frontend"
OUTPUT_DIR = BASE_DIR / "output"

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
COMBINED_CRM_DIR = PROCESSED_CRM_DIR / "combined"
PROCESSED_UNMATCHED_SHIFTED_DEPOSITS_DIR = PROCESSED_CRM_DIR / "unmatched_shifted_deposits"

# Training dataset directories
TRAINING_DATASET_DIR = DATA_DIR / "training_dataset"
TRUE_TRAINING_DIR = TRAINING_DATASET_DIR / "check_newversion_datasets"
FALSE_TRAINING_DIR = TRAINING_DATASET_DIR / "false_training_datasets"
TEST_MODEL_DIR =  TRAINING_DATASET_DIR /"model_testing"

# Ensure all directories exist (create at runtime; optimizes for unmatched tracking updates)
for dir_path in [DATA_DIR, MODEL_DIR, FRONTEND_DIR, OUTPUT_DIR, CRM_DIR, PROCESSOR_DIR, RAW_TRACKING_DIR, RAW_ATTACHED_FILES,
                 PROCESSED_DIR, PROCESSED_CRM_DIR, PROCESSED_PROCESSOR_DIR, RATES_DIR, LISTS_DIR, COMBINED_CRM_DIR,
                 PROCESSED_UNMATCHED_SHIFTED_DEPOSITS_DIR, TRAINING_DATASET_DIR, TRUE_TRAINING_DIR, FALSE_TRAINING_DIR, TEST_MODEL_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)