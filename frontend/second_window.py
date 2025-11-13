from PyQt5.QtWidgets import QWidget, QVBoxLayout, QPushButton, QMessageBox, QDesktopWidget, QApplication, QProgressBar
from PyQt5.QtCore import QTimer
import sys
import re
from src.reports_creator import main as reports_main
from src.output import main as output_main
# from third_window import ThirdWindow
# from fourth_window import FourthWindow
from src.config import setup_dirs_for_reg

class StdoutRedirector(object):
    def __init__(self, progress_bar):
        self.progress_bar = progress_bar
        self.preprocess_count = 0
        self.deposits_match_count = 0
        self.withdrawals_match_count = 0
        self.output_prepared_count = 0
        self.cross_count = 0

    def write(self, message):
        cleaned_message = message.strip()
        # Updated patterns for new output
        if "Preprocessed and combined" in cleaned_message:
            self.preprocess_count += 1
            progress = min(5 * self.preprocess_count, 20)  # Up to 20% for 4 preprocess steps
            self.progress_bar.setValue(progress)
        elif "Deposits matching report saved to" in cleaned_message:
            self.deposits_match_count += 1
            if self.deposits_match_count == 1:
                self.progress_bar.setValue(30)
            elif self.deposits_match_count == 2:
                self.progress_bar.setValue(40)
        elif "Matched Shifted Deposits by Currency:" in cleaned_message:
            self.progress_bar.setValue(45)
        elif re.search(r"(No Zotapay or PaymentAsia|Combined Zotapay \+ PaymentAsia)", cleaned_message):
            self.progress_bar.setValue(50)
        elif "Withdrawals matching report saved to" in cleaned_message:
            self.withdrawals_match_count += 1
            if self.withdrawals_match_count == 1:
                self.progress_bar.setValue(60)
            elif self.withdrawals_match_count == 2:
                self.progress_bar.setValue(70)
        elif "Cross-regulation matching" in cleaned_message:  # Adjust if exact print is different
            self.cross_count += 1
            self.progress_bar.setValue(75)
        elif "Cross-processor matching" in cleaned_message:  # Adjust if exact print is different
            self.cross_count += 1
            self.progress_bar.setValue(80)
        elif "Overall processing time" in cleaned_message:
            self.progress_bar.setValue(85)
        elif "DataFrame prepared for" in cleaned_message or "No unmatched" in cleaned_message:
            self.output_prepared_count += 1
            progress = 85 + min(2 * self.output_prepared_count, 15)  # Up to 100% for ~8-10 messages
            self.progress_bar.setValue(progress)
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
            print(f"Debug: Warnings file(s) exist—opening ThirdWindow")
            self.third_window = ThirdWindow(self.date_str)
            self.third_window.show()
        self.close()  # Close second window regardless

