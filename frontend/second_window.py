from PyQt5.QtWidgets import QWidget, QVBoxLayout, QPushButton, QMessageBox, QDesktopWidget, QApplication, QProgressBar
from PyQt5.QtCore import QTimer
import sys
import re
from src.reports_creator import main as reports_main
from src.output import main as output_main
from third_window import ThirdWindow
from fourth_window import FourthWindow
from src.config import setup_dirs_for_reg

class StdoutRedirector(object):
    def __init__(self, progress_bar):
        self.progress_bar = progress_bar
        self.preprocess_count = 0
        self.deposits_match_count = 0
        self.withdrawals_match_count = 0
        self.cross_count = 0
        self.output_count = 0
        self.current_progress = 0

    def write(self, message):
        cleaned_message = message.strip()
        new_progress = self.current_progress
        if "Preprocessed and combined" in cleaned_message:
            self.preprocess_count += 1
            new_progress = min(2 + 4 * self.preprocess_count, 18)  # Slower: 6,10,14,18 for 4 steps
        elif "Deposits matching report saved to" in cleaned_message:
            self.deposits_match_count += 1
            if self.deposits_match_count == 1:
                new_progress = 22
            elif self.deposits_match_count == 2:
                new_progress = 26
        elif "Matched Shifted Deposits by Currency:" in cleaned_message:
            new_progress = 30
        elif re.search(r"(No Zotapay or PaymentAsia|Combined Zotapay \+ PaymentAsia)", cleaned_message):
            new_progress = 34
        elif "Withdrawals matching report saved to" in cleaned_message:
            self.withdrawals_match_count += 1
            if self.withdrawals_match_count == 1:
                new_progress = 38
            elif self.withdrawals_match_count == 2:
                new_progress = 42
        elif "Cross-regulation matching" in cleaned_message:
            self.cross_count += 1
            new_progress = 46
        elif "Cross-processor matching" in cleaned_message:
            self.cross_count += 1
            new_progress = 50
        elif "Overall processing time" in cleaned_message:
            new_progress = 54
        elif re.search(r"(DataFrame prepared for|saved to|No unmatched|Removed \d+ compensated)", cleaned_message):
            self.output_count += 1
            new_progress = min(54 + 3 * self.output_count, 100)  # Spread output to 46%, increment 3% each
        if new_progress > self.current_progress:
            self.current_progress = new_progress
            self.progress_bar.setValue(self.current_progress)
        QApplication.processEvents()  # Update UI immediately

    def flush(self):
        pass  # Needed for compatibility with sys.stdout

class SecondWindow(QWidget):
    def __init__(self, date_str):
        super().__init__()
        print("Debug: SecondWindow __init__ started")
        self.date_str = date_str
        self.initUI()
        print("Debug: initUI completed")
        # Delay the run to after the window is shown
        QTimer.singleShot(0, self.run_processing_scripts)
        print("Debug: QTimer set for run_processing_scripts")

    def initUI(self):
        print("Debug: initUI started")
        self.setWindowTitle('Reports Creator Processing')
        self.resize(600, 150)  # Set smaller size
        qr = self.frameGeometry()
        cp = QDesktopWidget().availableGeometry().center()
        qr.moveCenter(cp)
        self.move(qr.topLeft())
        layout = QVBoxLayout()
        # Continue button (initially disabled; enabled when processing done)
        self.continue_btn = QPushButton('Next')
        self.continue_btn.setEnabled(False)
        self.continue_btn.clicked.connect(self.open_next_window)
        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setMinimum(0)
        self.progress_bar.setMaximum(100)
        self.progress_bar.setValue(0)
        self.progress_bar.setTextVisible(True)
        self.progress_bar.setFormat("%p%")
        # Add to layout: progress and button (no console)
        layout.addStretch(1)  # Center vertically
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

    def run_processing_scripts(self):
        print("Debug: run_processing_scripts started")
        # Redirect stdout to update progress
        old_stdout = sys.stdout
        redirector = StdoutRedirector(self.progress_bar)
        sys.stdout = redirector
        try:
            reports_main(self.date_str)  # Run reports_creator.main
            output_main(self.date_str)   # Run output.main
            self.continue_btn.setEnabled(True)
        except Exception as e:
            print(f"Error executing processing: {e}")
            QMessageBox.critical(self, "Error", f"Failed to run processing: {e}")
        finally:
            sys.stdout = old_stdout  # Restore stdout
        print("Debug: run_processing_scripts finished")

    def open_next_window(self):
        print("Debug: Checking for warnings files before opening next window")
        has_warnings = False
        for reg in ['row', 'uk']:
            dirs = setup_dirs_for_reg(reg)
            output_dir = dirs['output_dir'] / self.date_str
            warnings_path = output_dir / f"{reg.upper()} warnings_withdrawals.xlsx"
            print(f"Debug: Checking {warnings_path}")
            if warnings_path.exists():
                has_warnings = True
                print(f"Debug: Warnings file found for {reg.upper()}")
                break
        if not has_warnings:
            print(f"Debug: No warnings files found—skipping to fourth_window export")
            QMessageBox.information(self, "Info", "No warnings file found for any regulation. Skipping review and proceeding to export.")
            self.fourth_window = FourthWindow(self.date_str)
            self.fourth_window.show()
        else:
            print(f"Debug: Warnings file(s) exist—opening ThirdWindow for 'uk'")
            self.third_window = ThirdWindow(self.date_str, 'uk')
            self.third_window.show()
        self.close()  # Close second window regardless