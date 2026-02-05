"""
Script: second_window.py
Description: This script creates a secondary GUI window using PyQt5 to display a progress bar during the execution of backend processing scripts (reports_creator and output). It redirects stdout to parse log messages and update the progress bar incrementally based on specific processing milestones. Upon successful completion, it enables a "Next" button to transition to the third window (ThirdWindow) for the 'uk' regulation, passing the date string.

Key Features:
- Window setup: Resizes to 600x150, centers on screen using QDesktopWidget, sets title 'Reports Creator Processing'.
- Stylesheet: Applies gradients, borders, and hover effects for widgets like QPushButton and QProgressBar.
- Layout: Vertical layout with progress bar (0-100, text visible as %p%) and initially disabled "Next" button, stretched for centering.
- Stdout redirection: Uses StdoutRedirector class to capture and parse cleaned messages; increments progress based on counts for preprocessing (up to 18%), deposits matching (22-26%), shifts (30%), zotapay/paymentasia (34%), withdrawals matching (38-42%), cross-regulation/processor (46-50%), overall time (54%), and output steps (54-100% in 3% increments).
- Processing delay: Uses QTimer.singleShot(0) to run processing after window is shown, preventing UI freeze.
- Execution: Runs reports_main(date_str) and output_main(date_str) in try-except; restores stdout afterward; shows critical QMessageBox on errors.
- Button logic: "Next" button connects to open_next_window, which instantiates ThirdWindow(date_str, 'uk'), shows it, and closes the current window.
- UI updates: Calls QApplication.processEvents() after progress updates for immediate UI refresh.
- Edge cases: Handles exceptions with error messages; flush method for stdout compatibility; caps progress at 100; regex for specific log patterns (e.g., zotapay/paymentasia, output saves/removals).

Dependencies:
- PyQt5 (QtWidgets for QWidget, layouts, buttons, progress bar, message box, desktop widget; QtCore for QTimer)
- sys (for stdout redirection)
- re (for regex in message parsing)
- src.reports_creator (for main function as reports_main)
- src.output (for main function as output_main)
- third_window (for ThirdWindow class)
"""

from PyQt5.QtWidgets import QWidget, QVBoxLayout, QPushButton, QMessageBox, QDesktopWidget, QApplication, QProgressBar
from PyQt5.QtCore import QTimer
import sys
import re
from src.reports_creator import main as reports_main
from src.output import main as output_main
from third_window import ThirdWindow

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
        self._setup_window()
        self._setup_stylesheet()
        self._setup_layout()
        print("Debug: initUI finished")

    def _setup_window(self):
        self.setWindowTitle('Reports Creator Processing')
        self.resize(600, 150)  # Set smaller size
        qr = self.frameGeometry()
        cp = QDesktopWidget().availableGeometry().center()
        qr.moveCenter(cp)
        self.move(qr.topLeft())

    def _setup_stylesheet(self):
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

    def _setup_layout(self):
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

    def run_processing_scripts(self):
        print("Debug: run_processing_scripts started")
        # Redirect stdout to update progress
        old_stdout = sys.stdout
        redirector = StdoutRedirector(self.progress_bar)
        sys.stdout = redirector
        try:
            reports_main(self.date_str)  # Run reports_creator.main without reg (handles both internally)
            output_main(self.date_str)   # Run output.main without reg (assuming it handles both internally)
            self.continue_btn.setEnabled(True)
        except Exception as e:
            print(f"Error executing processing: {e}")
            QMessageBox.critical(self, "Error", f"Failed to run processing: {e}")
        finally:
            sys.stdout = old_stdout  # Restore stdout
        print("Debug: run_processing_scripts finished")

    def open_next_window(self):
        self.third_window = ThirdWindow(self.date_str, 'uk')
        self.third_window.show()
        self.close()