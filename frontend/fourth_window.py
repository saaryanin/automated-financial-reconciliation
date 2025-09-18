from PyQt5.QtWidgets import QWidget, QVBoxLayout, QPushButton, QLabel, QTableWidget, QTableWidgetItem, QFileDialog, \
    QMessageBox, QDesktopWidget, QHeaderView, QApplication, QHBoxLayout, QSizePolicy, QStyle
from PyQt5.QtCore import QProcess, Qt
import shutil
import pandas as pd
from src.config import OUTPUT_DIR, LISTS_DIR # Import LISTS_DIR from config
import sys

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
        self.setGeometry(300, 300, 800, 600) # Initial size, will be adjusted dynamically
        qr = self.frameGeometry()
        cp = QDesktopWidget().availableGeometry().center()
        qr.moveCenter(cp)
        self.move(qr.topLeft())
        layout = QVBoxLayout()
        layout.setSpacing(15) # Add spacing between widgets for better layout
        layout.setContentsMargins(20, 20, 20, 20) # Margins for the window content
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
                background: transparent;
                border: none;
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
        self.shifts_table.horizontalHeader().setSectionResizeMode(QHeaderView.Fixed)
        self.shifts_table.horizontalHeader().setSectionsClickable(False)
        self.shifts_table.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.shifts_table.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.shifts_table.setAlternatingRowColors(True)
        self.shifts_table.setStyleSheet("""
            QTableWidget {
                font-family: 'Segoe UI', Arial, sans-serif;
                font-size: 16px;
                background: transparent;
                border: none;
                border-radius: 4px;
                gridline-color: #dfe6e9;
                alternate-background-color: transparent;
            }
            QHeaderView::section {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #4a90e2, stop:1 #357abd);
                color: #ffffff;
                font-weight: 600;
                padding: 12px 15px; /* Reduced vertical padding for tighter fit */
                border: none;
                font-size: 16px;
            }
            QTableWidget::item {
                background-color: transparent;
                padding: 12px 15px; /* Reduced padding for tighter fit */
                color: #2c3e50;
            }
        """)
        # Container for centering the table horizontally
        self.table_container = QWidget()
        self.table_layout = QHBoxLayout(self.table_container)
        self.table_layout.addStretch(1)
        self.table_layout.addWidget(self.shifts_table)
        self.table_layout.addStretch(1)
        self.table_layout.setContentsMargins(0, 0, 0, 0)
        self.table_layout.setSpacing(0)
        self.table_container.hide()
        layout.addWidget(self.table_container)
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
                border: 2px solid #2a609d; /* Added border for clear button outline */
                padding: 12px 25px;
                border-radius: 6px;
                font-size: 16px;
                font-weight: 600;
            }
            QPushButton:hover {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #357abd, stop:1 #2a609d);
                box-shadow: 0 4px 10px rgba(74, 144, 226, 0.4);
                border: 2px solid #1e4b7a; /* Darker border on hover */
            }
            QPushButton:disabled {
                background: #b0b7c3;
                color: #ffffff;
                border: 2px solid #8a9aa6; /* Border for disabled state */
                cursor: not-allowed;
                box-shadow: none;
            }
        """)
        print("Debug: initUI finished")
    def populate_shifts_table(self):
        shifts_path = OUTPUT_DIR / self.date_str / "total_shifts_by_currency.csv"
        if not shifts_path.exists():
            print("Debug: Shifts file not found")
            self.shifts_label.setText("No Shifts Detected")
            self.shifts_label.show()
            self.table_container.hide()
            return
        try:
            df = pd.read_csv(shifts_path)
            if df.empty:
                print("Debug: Shifts file is empty")
                self.shifts_label.setText("No Shifts Detected")
                self.shifts_label.show()
                self.table_container.hide()
                return
            num_currencies = len(df.columns)
            self.shifts_label.setText(
                "Total Shifts by Currencies" if num_currencies >= 2 else "Total Shifts by Currency")
            self.shifts_table.setRowCount(len(df))
            self.shifts_table.setColumnCount(len(df.columns))
            self.shifts_table.setHorizontalHeaderLabels(df.columns.tolist())
            for i in range(len(df)):
                for j in range(len(df.columns)):
                    value = df.iloc[i, j]
                    formatted_value = f"{value:g}"
                    item = QTableWidgetItem(formatted_value)
                    item.setTextAlignment(Qt.AlignCenter)
                    self.shifts_table.setItem(i, j, item)
            # Resize columns to contents first to get natural widths
            self.shifts_table.resizeColumnsToContents()
            # Find the maximum column width to ensure no clipping and equal widths for centering
            max_width = 0
            for j in range(self.shifts_table.columnCount()):
                max_width = max(max_width, self.shifts_table.columnWidth(j))
            # Set a minimum column width
            min_col_width = 120
            col_width = max(max_width, min_col_width)
            # Set all columns to the same width for symmetric centering
            for j in range(self.shifts_table.columnCount()):
                self.shifts_table.setColumnWidth(j, col_width)
            # Set row height for better visibility but tighter
            for i in range(len(df)):
                self.shifts_table.setRowHeight(i, 60)
            # Calculate and set fixed width for the table to prevent expansion and ensure centering
            col_sum = sum(self.shifts_table.columnWidth(j) for j in range(self.shifts_table.columnCount()))
            vheader_w = self.shifts_table.verticalHeader().width()
            frame_w = self.shifts_table.style().pixelMetric(QStyle.PM_DefaultFrameWidth) * 2
            desired_w = col_sum + vheader_w + frame_w
            self.shifts_table.setFixedWidth(desired_w)
            self.table_container.show()
        except Exception as e:
            print(f"Debug: Error populating shifts table: {e}")
            self.shifts_label.setText("Error Loading Shifts")
            self.table_container.hide()
    def adjust_window_size(self):
        # Calculate required width based on visible widgets
        margins = self.layout().contentsMargins()
        margin_width = margins.left() + margins.right()
        button_width = self.export_btn.sizeHint().width()
        if self.table_container.isVisible():
            table_width = self.shifts_table.width()
            window_width = max(button_width, table_width) + margin_width
        else:
            label_width = self.shifts_label.sizeHint().width() if self.shifts_label.isVisible() else 0
            window_width = max(button_width, label_width) + margin_width
        # Calculate required height based on visible widgets
        button_height = self.export_btn.sizeHint().height()
        label_height = self.shifts_label.sizeHint().height() if self.shifts_label.isVisible() else 0
        if self.table_container.isVisible():
            header_height = self.shifts_table.horizontalHeader().height()
            total_row_height = sum(self.shifts_table.rowHeight(i) for i in range(self.shifts_table.rowCount()))
            table_height = header_height + total_row_height
        else:
            table_height = 0
        # Number of widgets: button always, +label if visible, +table if visible
        num_widgets = 1 + (1 if label_height > 0 else 0) + (1 if table_height > 0 else 0)
        num_spacings = max(0, num_widgets - 1)
        margin_height = margins.top() + margins.bottom()
        window_height = button_height + label_height + table_height + self.layout().spacing() * num_spacings + margin_height
        self.resize(window_width, window_height)
        # Recenter the window
        qr = self.frameGeometry()
        cp = QDesktopWidget().availableGeometry().center()
        qr.moveCenter(cp)
        self.move(qr.topLeft())
        print(f"Debug: Window resized to {window_width}x{window_height}")

    def is_perfect_match(self):
        """Check if deposits and withdrawals matching files indicate perfect reconciliation (all match_status=1) and no shifts."""
        perfect = True
        lists_dir = LISTS_DIR / self.date_str
        deposits_path = lists_dir / "deposits_matching.xlsx"
        withdrawals_path = lists_dir / "withdrawals_matching.xlsx"
        shifts_path = OUTPUT_DIR / self.date_str / "total_shifts_by_currency.csv"

        # Check deposits
        if deposits_path.exists():
            try:
                df_d = pd.read_excel(deposits_path)
                if 'match_status' in df_d.columns:
                    unmatched_d = (df_d['match_status'] == 0).sum()
                    if unmatched_d > 0:
                        perfect = False
            except Exception as e:
                print(f"Debug: Error checking deposits: {e}")
                perfect = False

        # Check withdrawals
        if withdrawals_path.exists():
            try:
                df_w = pd.read_excel(withdrawals_path)
                if 'match_status' in df_w.columns:
                    unmatched_w = (df_w['match_status'] == 0).sum()
                    if unmatched_w > 0:
                        perfect = False
            except Exception as e:
                print(f"Debug: Error checking withdrawals: {e}")
                perfect = False

        # Check shifts
        if shifts_path.exists():
            try:
                df_s = pd.read_csv(shifts_path)
                if not df_s.empty:
                    perfect = False
            except Exception as e:
                print(f"Debug: Error checking shifts: {e}")

        return perfect

    def run_output_script(self):
        print("Debug: run_output_script started")
        try:
            from src.output import (
                generate_unmatched_crm_deposits, generate_unapproved_crm_deposits,
                generate_unmatched_proc_deposits, generate_unmatched_proc_withdrawals,
                remove_compensated_entries, generate_unmatched_crm_withdrawals
            )
            from src.shifts_handler import main as handle_shifts  # Import for CSV save
            from src.config import OUTPUT_DIR

            output_dir = OUTPUT_DIR / self.date_str
            output_dir.mkdir(parents=True,
                             exist_ok=True)  # Ensure dir exists (no rmtree—avoids overwriting warnings.xlsx)

            # Save shifts CSV (idempotent; re-runs handle_shifts safely)
            matched_sums = handle_shifts(self.date_str)
            if matched_sums:
                output_path = output_dir / "total_shifts_by_currency.csv"
                df = pd.DataFrame([matched_sums])
                if not df.empty:
                    df.to_csv(output_path, index=False)
                    print(f"Debug: Shifts CSV saved: {output_path}")
                else:
                    print("Debug: No shifts data—skipping CSV")
            else:
                print("Debug: handle_shifts returned None/empty—skipping CSV")

            # Check for perfect match before phase 2 to avoid .str errors on empty DFs
            if self.is_perfect_match():
                print("Debug: Perfect match detected - skipping phase 2 to avoid errors")
                self.shifts_label.setText("Perfect Reconciliation - No Shifts or Unmatched Rows")
                self.shifts_label.show()
                self.table_container.hide()
            else:
                # Phase 2: Generate all output files (unmatched/unapproved/etc.)
                print("Debug: Running phase 2")
                generate_unmatched_crm_deposits(self.date_str)
                generate_unapproved_crm_deposits(self.date_str)
                generate_unmatched_proc_deposits(self.date_str)
                generate_unmatched_proc_withdrawals(self.date_str)
                remove_compensated_entries(self.date_str)
                generate_unmatched_crm_withdrawals(self.date_str)
                print("Debug: Phase 2 complete—all files generated")

            # Now populate UI (table will show if CSV exists)
            self.populate_shifts_table()
            self.export_btn.setEnabled(True)
            self.adjust_window_size()
        except Exception as e:
            print(f"Error executing output phase 2: {e}")
            import traceback
            traceback.print_exc()  # For EXE console debug
            QMessageBox.critical(self, "Error", f"Failed to run output phase 2: {e}")
        print("Debug: run_output_script finished")
    def export_files(self):
        print("Debug: export_files started")
        dest_folder = QFileDialog.getExistingDirectory(self, "Select Folder to Export To")
        if dest_folder:
            source_folder = OUTPUT_DIR / self.date_str
            if source_folder.exists():
                exported_count = 0
                excluded_files = ["warnings_withdrawals.xlsx", "withdrawals_matching_updated.xlsx"]
                for file in source_folder.iterdir():
                    if file.is_file() and file.name not in excluded_files:
                        shutil.copy(str(file), dest_folder)
                        exported_count += 1
                if exported_count > 0:
                    QMessageBox.information(self, "Success", f"{exported_count} files exported to {dest_folder}")
                else:
                    # Check for perfect match for custom alert
                    if self.is_perfect_match():
                        alert_msg = "Congratulations! Every row has matched perfectly with no discrepancies or shifts detected. No additional output reports are required for this date."
                        QMessageBox.information(self, "Perfect Reconciliation", alert_msg)
                    else:
                        QMessageBox.warning(self, "No Files", "No files to export (excluding warnings_withdrawals.xlsx and withdrawals_matching_updated.xlsx).")
            else:
                QMessageBox.warning(self, "Error", f"No files found in output/{self.date_str}")
        print("Debug: export_files finished")
if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = FourthWindow("2025-08-06")
    window.show()
    sys.exit(app.exec_())