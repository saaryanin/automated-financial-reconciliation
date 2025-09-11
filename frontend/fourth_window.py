# Modified fourth_window.py (changes: made standalone runnable with hardcoded date_str="2025-08-05", skipped output generation functions for testing display only, added QApplication for direct execution)
from PyQt5.QtWidgets import QWidget, QVBoxLayout, QPushButton, QLabel, QTableWidget, QTableWidgetItem, QFileDialog, \
    QMessageBox, QDesktopWidget, QHeaderView, QApplication
from PyQt5.QtCore import QProcess, Qt
from PyQt5.QtGui import QLinearGradient, QBrush, QPalette
import os
import shutil
import pandas as pd
from src.config import OUTPUT_DIR  # Import OUTPUT_DIR from config
import sys
from src.output import generate_unmatched_crm_deposits, generate_unapproved_crm_deposits, \
    generate_unmatched_proc_deposits, generate_unmatched_proc_withdrawals, remove_compensated_entries, \
    generate_unmatched_crm_withdrawals  # Import modular functions (not used in standalone mode)


class FourthWindow(QWidget):
    def __init__(self, date_str):
        super().__init__()
        print("Debug: FourthWindow __init__ started")
        self.date_str = date_str
        self.initUI()
        print("Debug: initUI completed")
        self.run_output_script()
        print("Debug: run_output_script called")

    def initUI(self):
        print("Debug: initUI started")
        self.setWindowTitle('Processing Output')
        self.setGeometry(300, 300, 800, 600)  # Initial size, will be adjusted dynamically
        qr = self.frameGeometry()
        cp = QDesktopWidget().availableGeometry().center()
        qr.moveCenter(cp)
        self.move(qr.topLeft())
        layout = QVBoxLayout()
        layout.setSpacing(15)  # Add spacing between widgets for better layout
        layout.setContentsMargins(20, 20, 20, 20)  # Margins for the window content
        # Export button (initially disabled)
        self.export_btn = QPushButton('Export Daily Reconciliation Reports')
        self.export_btn.setEnabled(False)
        self.export_btn.clicked.connect(self.export_files)
        layout.addWidget(self.export_btn)
        # Shifts label (initially hidden)
        self.shifts_label = QLabel()
        self.shifts_label.setAlignment(Qt.AlignCenter)
        self.shifts_label.setStyleSheet("""
            QLabel {
                font-family: 'Segoe UI', Arial, sans-serif;
                font-size: 16px;
                font-weight: 600;
                color: #2c3e50;
                padding: 10px;
                background: #ffffff;
                border: 1px solid #dfe6e9;
                border-radius: 4px;
            }
        """)
        self.shifts_label.hide()
        layout.addWidget(self.shifts_label)
        # Shifts table (initially hidden)
        self.shifts_table = QTableWidget()
        self.shifts_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.shifts_table.horizontalHeader().setVisible(True)
        self.shifts_table.verticalHeader().setVisible(False)
        self.shifts_table.horizontalHeader().setStretchLastSection(False)
        self.shifts_table.setAlternatingRowColors(True)
        self.shifts_table.setStyleSheet("""
            QTableWidget {
                font-family: 'Segoe UI', Arial, sans-serif;
                font-size: 16px;
                background: #ffffff;
                border: 1px solid #dfe6e9;
                border-radius: 4px;
                gridline-color: #dfe6e9;
                alternate-background-color: #f8f9fa;
            }
            QHeaderView::section {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #4a90e2, stop:1 #357abd);
                color: #ffffff;
                font-weight: 600;
                padding: 20px;  /* Increased vertical padding for full header visibility */
                border: none;
                font-size: 16px;
            }
            QTableWidget::item {
                padding: 20px 15px;  /* Increased padding for full content visibility */
                color: #2c3e50;
            }
        """)
        self.shifts_table.hide()
        layout.addWidget(self.shifts_table)
        self.setLayout(layout)
        self.setStyleSheet("""
            QWidget {
                font-family: 'Segoe UI', Arial, sans-serif;
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #4a90e2, stop:1 #d3d8e8);
                border-radius: 6px;
                padding: 6px;
            }
            QPushButton {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #4a90e2, stop:1 #357abd);
                color: #ffffff;
                border: 2px solid #2a609d;  /* Added border for clear button outline */
                padding: 12px 25px;
                border-radius: 6px;
                font-size: 16px;
                font-weight: 600;
            }
            QPushButton:hover {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #357abd, stop:1 #2a609d);
                box-shadow: 0 4px 10px rgba(74, 144, 226, 0.4);
                border: 2px solid #1e4b7a;  /* Darker border on hover */
            }
            QPushButton:disabled {
                background: #b0b7c3;
                color: #ffffff;
                border: 2px solid #8a9aa6;  /* Border for disabled state */
                cursor: not-allowed;
                box-shadow: none;
            }
        """)
        print("Debug: initUI finished")

    def populate_shifts_table(self):
        shifts_path = OUTPUT_DIR / self.date_str / "total_shifts_by_currency.csv"
        if not shifts_path.exists():
            print("Debug: Shifts file not found")
            return
        try:
            df = pd.read_csv(shifts_path)
            if df.empty:
                print("Debug: Shifts file is empty")
                return
            num_currencies = len(df.columns)
            self.shifts_label.setText(
                "Total Shifts by Currencies" if num_currencies >= 2 else "Total Shifts by Currency")
            self.shifts_table.setRowCount(len(df))
            self.shifts_table.setColumnCount(len(df.columns))
            self.shifts_table.setHorizontalHeaderLabels(df.columns.tolist())
            for i in range(len(df)):
                for j in range(len(df.columns)):
                    item = QTableWidgetItem(str(df.iloc[i, j]))
                    item.setTextAlignment(Qt.AlignCenter)
                    self.shifts_table.setItem(i, j, item)
            # Adjust column widths based on content length (increased cap for better visibility)
            for j in range(len(df.columns)):
                value_str = str(df.iloc[0, j]) if len(df) > 0 else ""
                base_width = 80
                content_width = len(value_str) * 8 + 20
                col_width = min(base_width + content_width, 150)  # Increased cap to 150px per column
                self.shifts_table.setColumnWidth(j, col_width)
            self.shifts_table.resizeColumnsToContents()  # Ensure columns fit content
            # Set increased row height for better visibility
            for i in range(len(df)):
                self.shifts_table.setRowHeight(i, 80)
        except Exception as e:
            print(f"Debug: Error populating shifts table: {e}")

    def adjust_window_size(self):
        # Calculate required width: max of button width or table width
        button_width = self.export_btn.sizeHint().width()
        table_width = self.shifts_table.horizontalHeader().length() + self.style().pixelMetric(
            self.style().PM_ScrollBarExtent) + 20  # Include scrollbar space if needed
        window_width = max(button_width,
                           table_width) + self.layout().contentsMargins().left() + self.layout().contentsMargins().right() + 40  # Extra for borders

        # Calculate required height: button + label + table (header + row + padding) + margins
        button_height = self.export_btn.sizeHint().height()
        label_height = self.shifts_label.sizeHint().height()
        header_height = self.shifts_table.horizontalHeader().height()
        row_height = self.shifts_table.rowHeight(0) if self.shifts_table.rowCount() > 0 else 0
        table_height = header_height + row_height + 40  # Extra padding for table layer height
        window_height = button_height + label_height + table_height + self.layout().spacing() * 2 + self.layout().contentsMargins().top() + self.layout().contentsMargins().bottom() + 40  # Fit tightly

        self.resize(window_width, window_height)

        # Recenter the window
        qr = self.frameGeometry()
        cp = QDesktopWidget().availableGeometry().center()
        qr.moveCenter(cp)
        self.move(qr.topLeft())

        print(f"Debug: Window resized to {window_width}x{window_height}")

    def run_output_script(self):
        print("Debug: run_output_script started")
        try:
            # Skip generation functions for standalone testing (files already exist in temp/output/2025-08-05)
            # generate_unmatched_crm_deposits(self.date_str)
            # generate_unapproved_crm_deposits(self.date_str)
            # generate_unmatched_proc_deposits(self.date_str)
            # generate_unmatched_proc_withdrawals(self.date_str)
            # remove_compensated_entries(self.date_str)
            # generate_unmatched_crm_withdrawals(self.date_str)
            self.populate_shifts_table()
            self.shifts_label.show()
            self.shifts_table.show()
            self.export_btn.setEnabled(True)
            self.adjust_window_size()  # Dynamically fit window to content
        except Exception as e:
            print(f"Error executing output phase 2: {e}")
            QMessageBox.critical(self, "Error", f"Failed to run output phase 2: {e}")
        print("Debug: run_output_script finished")

    def export_files(self):
        print("Debug: export_files started")
        dest_folder = QFileDialog.getExistingDirectory(self, "Select Folder to Export To")
        if dest_folder:
            source_folder = OUTPUT_DIR / self.date_str
            if source_folder.exists():
                exported_count = 0
                for file in source_folder.iterdir():
                    if file.is_file() and file.name != "warnings_withdrawals.xlsx":
                        shutil.copy(str(file), dest_folder)
                        exported_count += 1
                if exported_count > 0:
                    QMessageBox.information(self, "Success", f"{exported_count} files exported to {dest_folder}")
                else:
                    QMessageBox.warning(self, "No Files", "No files to export (excluding warnings_withdrawals.xlsx).")
            else:
                QMessageBox.warning(self, "Error", f"No files found in output/{self.date_str}")
        print("Debug: export_files finished")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = FourthWindow("2025-08-05")
    window.show()
    sys.exit(app.exec_())