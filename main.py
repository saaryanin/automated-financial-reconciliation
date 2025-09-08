import sys
from pathlib import Path

# Add project root and src to path for script running (ignored in EXE)
root_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(root_dir))
sys.path.insert(0, str(root_dir / 'src'))
sys.path.insert(0, str(root_dir / 'frontend'))

import os  # Optional: For debugging

# Optional debug print (remove after testing)
print("sys.path:", sys.path)
print("Current dir:", os.getcwd())

from PyQt5.QtWidgets import QApplication
from frontend.first_window import ReconciliationWindow  # Adjusted import from frontend folder

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = ReconciliationWindow()
    window.show()
    sys.exit(app.exec_())