import sys
from pathlib import Path
import shutil  # For clearing temp directories

# Add project root and src to path for script running (ignored in EXE)
root_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(root_dir))
sys.path.insert(0, str(root_dir / 'src'))
sys.path.insert(0, str(root_dir / 'frontend'))

import os  # Optional: For debugging

# Optional debug print (remove after testing)
print("sys.path:", sys.path)
print("Current dir:", os.getcwd())

# NEW: Import config early to access DATA_DIR (handles frozen/EXE vs dev paths)
from src.config import DATA_DIR, LISTS_DIR, RATES_DIR, OUTPUT_DIR  # Assumes config.py is in src/ + add OUTPUT_DIR

# NEW: Clear contents of temp/data subdirs at app launch (except lists/ and rates/ to preserve unmatched_shifted_deposits and rates_{date}.csv)
DATA_DIR.mkdir(parents=True, exist_ok=True)  # Ensure exists
lists_dir = LISTS_DIR  # Full path from config
rates_dir = RATES_DIR
for item in list(DATA_DIR.iterdir()):  # Use list() to avoid modification-during-iteration
    if item.is_dir():
        if item == lists_dir or item == rates_dir:
            continue  # Skip entirely (preserve contents + dir)
        # Clear contents only (unlink files, rmtree child dirs), leave empty dir intact for later mkdir-free moves
        for child in list(item.iterdir()):
            if child.is_dir():
                shutil.rmtree(child)
                print(f"Cleared child dir {child} in {item}")
            else:
                child.unlink()
                print(f"Removed child file {child} in {item}")
        print(f"Cleared contents of {item} to prevent stale file bleed")
    elif item.is_file():
        item.unlink()
        print(f"Removed stray file {item} in DATA_DIR")

# NEW: Also clear contents of temp/output (leave empty dir for fresh exports in output.py/fourth_window.py)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
for child in list(OUTPUT_DIR.iterdir()):
    if child.is_dir():
        shutil.rmtree(child)
        print(f"Cleared child dir {child} in OUTPUT_DIR")
    else:
        child.unlink()
        print(f"Removed child file {child} in OUTPUT_DIR")
print("Cleared contents of OUTPUT_DIR to prevent stale exports")

from PyQt5.QtWidgets import QApplication
from frontend.first_window import ReconciliationWindow  # Adjusted import from frontend folder

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = ReconciliationWindow()
    window.show()
    sys.exit(app.exec_())