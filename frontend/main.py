import sys
from PyQt5.QtWidgets import QApplication
from first_window import ReconciliationWindow  # Import your class (adjust if class name differs)

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = ReconciliationWindow()
    window.show()
    sys.exit(app.exec_())