import sys
from pathlib import Path
import shutil

# Add project root and src to path for script running (ignored in EXE)
root_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(root_dir))
sys.path.insert(0, str(root_dir / 'src'))
sys.path.insert(0, str(root_dir / 'frontend'))

# NEW: Global clear of old date contents at startup (EXE-only; preserves folder structure)
if getattr(sys, 'frozen', False):
    from src.config import OUTPUT_DIR, LISTS_DIR
    print("EXE mode: Clearing old date contents in OUTPUT_DIR and LISTS_DIR...")
    for dir_path in [OUTPUT_DIR, LISTS_DIR]:
        for item in list(dir_path.iterdir()):
            try:
                if item.is_dir():
                    shutil.rmtree(item)
                    print(f"Cleared old subdir {item} in {dir_path}")
                elif item.is_file():
                    item.unlink()
                    print(f"Removed old file {item} in {dir_path}")
            except Exception as e:
                print(f"Failed to clear {item}: {e} (continuing...)")

from PyQt5.QtWidgets import QApplication
from frontend.first_window import ReconciliationWindow  # Adjusted import from frontend folder

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = ReconciliationWindow()
    window.show()
    sys.exit(app.exec_())