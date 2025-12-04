"""Configuration module for financial reconciliation system."""
import sys
import os
from pathlib import Path
import tempfile
from typing import List

# Determine base directory
def _get_base_dir() -> Path:
    """Get base directory, supporting both dev and frozen (PyInstaller) modes."""
    if getattr(sys, 'frozen', False):
        return Path(sys._MEIPASS)
    return Path(__file__).resolve().parent.parent

def _get_temp_dir() -> Path:
    """Get temp directory with environment variable support."""
    if getattr(sys, 'frozen', False):
        temp_root = Path(tempfile.gettempdir())
        return temp_root / 'reconciliation_temp'

    # Support custom temp dir via environment variable
    custom_temp = os.getenv('RECONCILIATION_TEMP_DIR')
    if custom_temp:
        return Path(custom_temp)

    return _get_base_dir() / "temp"

# Base directories
BASE_DIR = _get_base_dir()
TEMP_DIR = _get_temp_dir()

# Read-only directories (under BASE_DIR)
MODEL_DIR = BASE_DIR / "model"
FRONTEND_DIR = BASE_DIR / "frontend"

# Writable directories (under TEMP_DIR)
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

# All writable directories that need to be created
_WRITABLE_DIRS: List[Path] = [
    TEMP_DIR, DATA_DIR, OUTPUT_DIR, CRM_DIR, PROCESSOR_DIR, RAW_TRACKING_DIR,
    RAW_ATTACHED_FILES, PROCESSED_DIR, PROCESSED_CRM_DIR, PROCESSED_PROCESSOR_DIR,
    RATES_DIR, LISTS_DIR, COMBINED_CRM_DIR, PROCESSED_UNMATCHED_SHIFTED_DEPOSITS_DIR,
    TRAINING_DATASET_DIR, TRUE_TRAINING_DIR, FALSE_TRAINING_DIR, TEST_MODEL_DIR
]

def ensure_directories() -> None:
    """Create all required directories if they don't exist."""
    for dir_path in _WRITABLE_DIRS:
        dir_path.mkdir(parents=True, exist_ok=True)

# Auto-create directories on import
ensure_directories()