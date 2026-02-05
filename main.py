"""
Script: main.py
Description: This script serves as the entry point for the CRM-Processor Reconciliation System application. It sets up the Python path to include the project root, src, and frontend directories for module imports (useful in script mode, ignored in executable), initializes the PyQt5 QApplication, creates and shows the initial ReconciliationWindow (first_window.py), and executes the application event loop.

Key Features:
- Path setup: Resolves the script's parent directory as root, inserts root, src, and frontend paths to sys.path for relative imports, ensuring compatibility when running as script or frozen executable.
- Application launch: Creates QApplication with sys.argv, instantiates ReconciliationWindow, shows it, and runs app.exec_() with sys.exit for proper termination.
- Edge cases: Handles frozen executable (via sys._MEIPASS potentially, but paths are relative); no additional logic beyond startup.

Dependencies:
- sys (for path manipulation and app exit)
- pathlib (for Path resolution)
- PyQt5 (QtWidgets for QApplication)
- frontend.first_window (for ReconciliationWindow class)
"""

import sys
from pathlib import Path

# Add project root and src to path for script running (ignored in EXE)
root_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(root_dir))
sys.path.insert(0, str(root_dir / 'src'))
sys.path.insert(0, str(root_dir / 'frontend'))

from PyQt5.QtWidgets import QApplication
from frontend.first_window import ReconciliationWindow  # Adjusted import from frontend folder

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = ReconciliationWindow()
    window.show()
    sys.exit(app.exec_())