from PyQt5.QtWidgets import QWidget, QVBoxLayout, QPushButton, QMessageBox, QDesktopWidget, QApplication, QProgressBar
from PyQt5.QtCore import QTimer
import sys
import re
from src.reports_creator import main
from src.output import generate_warning_withdrawals
from third_window_test import ThirdWindow
from fourth_window import FourthWindow
from src.config import OUTPUT_DIR

class StdoutRedirector(object):
    def __init__(self, progress_bar):
        self.progress_bar = progress_bar
        self.combined_count = 0
    def write(self, message):
        cleaned_message = message.strip()
        # Update progress based on milestones
        if re.search(r"Debug: combined_crm type: <class 'pandas\.core\.frame\.DataFrame'>, shape: \(\d+, \d+\), columns: \[.*\]", cleaned_message):
            self.combined_count += 1
            if self.combined_count == 1:
                self.progress_bar.setValue(20)
            elif self.combined_count == 2:
                self.progress_bar.setValue(70)
        elif re.search(r"Deposits matching report saved to .+\\deposits_matching\.xlsx", cleaned_message):
            self.progress_bar.setValue(35)
        elif "Matched Shifted Deposits by Currency:" in cleaned_message:
            self.progress_bar.setValue(45)
        elif re.search(r"(No Zotapay or PaymentAsia|Combined Zotapay \+ PaymentAsia)", cleaned_message):
            self.progress_bar.setValue(55)
        elif re.search(r"Withdrawals matching report saved to .+\\withdrawals_matching\.xlsx", cleaned_message):
            self.progress_bar.setValue(85)
        elif re.search(r"Saved \d+ rows for withdrawals", cleaned_message):
            self.progress_bar.setValue(95)
        elif re.search(r"Total time: \d+\.\d+ seconds", cleaned_message):
            self.progress_bar.setValue(100)
        QApplication.processEvents() # Update UI immediately
    def flush(self):
        pass # Needed for compatibility with sys.stdout
class SecondWindow(QWidget):
    def __init__(self, date_str):
        super().__init__()
        print("Debug: SecondWindow __init__ started")
        self.date_str = date_str
        self.initUI()
        print("Debug: initUI completed")
        # Delay the run to after the window is shown
        QTimer.singleShot(0, self.run_reports_creator_script)
        print("Debug: QTimer set for run_reports_creator_script")
    def initUI(self):
        print("Debug: initUI started")
        self.setWindowTitle('Reports Creator Processing')
        self.resize(600, 150) # Set smaller size
        qr = self.frameGeometry()
        cp = QDesktopWidget().availableGeometry().center()
        qr.moveCenter(cp)
        self.move(qr.topLeft())
        layout = QVBoxLayout()
        # Continue button (initially disabled; enabled when processing done)
        self.continue_btn = QPushButton('Next')
        self.continue_btn.setEnabled(False)
        self.continue_btn.clicked.connect(self.open_third_window)  # CHANGED: open_third_window
        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setMinimum(0)
        self.progress_bar.setMaximum(100)
        self.progress_bar.setValue(0)
        self.progress_bar.setTextVisible(True)
        self.progress_bar.setFormat("%p%")
        # Add to layout: progress and button (no console)
        layout.addStretch(1) # Center vertically
        layout.addWidget(self.progress_bar)
        layout.addWidget(self.continue_btn)
        layout.addStretch(1)
        self.setLayout(layout)
        self.setStyleSheet("""
            QWidget {
                font-family: 'Segoe UI', Arial, sans-serif;
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #4a90e2, stop:1 #d3d8e8);
                border-radius: 10px;
                padding: 10px;
            }
            QPushButton {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #4a90e2, stop:1 #357abd);
                color: #ffffff;
                border: none;
                padding: 12px 25px;
                border-radius: 6px;
                font-size: 16px;
                font-weight: 600;
            }
            QPushButton:hover {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #357abd, stop:1 #2a609d);
                box-shadow: 0 4px 10px rgba(74, 144, 226, 0.4);
            }
            QPushButton:disabled {
                background: #b0b7c3;
                color: #ffffff;
                cursor: not-allowed;
                box-shadow: none;
            }
            QProgressBar {
                background: #f0f0f0;
                border: 1px solid #dfe6e9;
                border-radius: 4px;
                text-align: center;
                font-size: 12px;
                color: #2c3e50;
            }
            QProgressBar::chunk {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #4a90e2, stop:1 #357abd);
                border-radius: 4px;
            }
        """)
        print("Debug: initUI finished")
    def run_reports_creator_script(self):
        print("Debug: run_reports_creator_script started")
        # Redirect stdout to the console
        old_stdout = sys.stdout
        redirector = StdoutRedirector(self.progress_bar)
        sys.stdout = redirector
        try:
            main(self.date_str) # Direct call to reports_creator.main
            # NEW: Generate warnings file after reports_creator (ensures it exists for check)
            print("Debug: Generating warnings_withdrawals.xlsx...")
            generate_warning_withdrawals(self.date_str)
            print("Debug: Warnings generation complete")
            # No append since no console
            self.continue_btn.setEnabled(True)
        except Exception as e:
            print(f"Error executing reports_creator or output: {e}")
            QMessageBox.critical(self, "Error", f"Failed to run processing: {e}")
        finally:
            sys.stdout = old_stdout # Restore stdout
        print("Debug: run_reports_creator_script finished")
    def open_third_window(self):  # CHANGED: Conditional open—third if warnings exist, else direct to fourth with alert
        print("Debug: Checking for warnings file before opening next window")
        output_dir = OUTPUT_DIR / self.date_str
        warnings_path = output_dir / "warnings_withdrawals.xlsx"
        print(f"Debug: warnings_path = {warnings_path}")  # NEW: Log path for debug
        print(f"Debug: warnings_path.exists() = {warnings_path.exists()}")  # NEW: Log existence
        if not warnings_path.exists():
            print(f"Debug: No warnings_withdrawals.xlsx found for {self.date_str}—skipping to fourth_window export")
            # NEW: Show alert (ported from third_window.py) before direct export
            QMessageBox.information(self, "Info", "No warnings file found. Skipping review and proceeding to export.")
            self.fourth_window = FourthWindow(self.date_str)
            self.fourth_window.show()
        else:
            print(f"Debug: Warnings file exists—opening ThirdWindow")
            self.third_window = ThirdWindow(self.date_str)
            self.third_window.show()
        self.close() # Close second window regardless