from pathlib import Path

# Project root = src/.. (parent of current file)
BASE_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = BASE_DIR / "data"
CRM_DIR = DATA_DIR / "crm_reports"
PROCESSOR_DIR = DATA_DIR / "processor_reports"
PROCESSED_DIR = DATA_DIR / "processed"
PROCESSED_CRM_DIR = PROCESSED_DIR / "crm"
PROCESSED_PROCESSOR_DIR = PROCESSED_DIR / "processors"

