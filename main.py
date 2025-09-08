import sys
import os

# Optional: Ensure frontend is on path (can remove if absolute import works)
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'frontend'))

from PyQt5.QtWidgets import QApplication
from frontend.first_window import ReconciliationWindow  # Adjusted import from frontend folder

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = ReconciliationWindow()
    window.show()
    sys.exit(app.exec_())