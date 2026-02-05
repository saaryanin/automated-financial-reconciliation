"""
Script: fourth_window.py
Description: This script creates the final GUI window using PyQt5 for exporting daily reconciliation reports after processing. It runs the second phase of output generation to create unmatched/matched Excel files for deposits and withdrawals (handling compensated entries and UK Barclays declined), displays shifts by currency in tables or labels for ROW and UK, allows selecting regulations via checkboxes, and exports selected files (excluding warnings and updated matching) to a user-chosen directory with regulation prefixes. It checks for perfect matches (no unmatched rows or shifts) and shows appropriate messages.

Key Features:
- UI setup: Centers window, adds ROW/UK checkboxes (default checked), disabled export button until ready, dynamic shifts layout with labels/tables styled for gradients, borders, and hover effects.
- Output generation: Calls handle_shifts for matched sums, clears output dir (keeping warnings/updated), saves regulation-specific shifts, generates unmatched CRM/proc deposits/withdrawals DFs with compensation removal, appends UK Barclays declined withdrawals (loading/processing/standardizing/sorting), creates matched deposits/withdrawals, saves to multi-sheet Excels using save_unmatched_to_excel and save_matched_to_excel.
- Shifts display: Loads total_shifts_by_currency.xlsx per regulation, shows centered labels/tables with currencies as headers and amounts in single row (formatted without decimals if integer), resizes columns uniformly (min 120px), handles no shifts with labels.
- Window adjustment: Calculates max width/height from buttons, checkboxes, shifts content (headers/rows/margins/scrollbars), caps to screen size, resizes/fixes table heights (60px rows), re-centers window.
- Perfect match check: Verifies no match_status=0 in deposits/withdrawals_matching.xlsx and empty/no shifts file, shows congrats message if true during export if no files.
- Export logic: Validates at least one regulation selected, chooses folder via dialog, copies files from output/date dir (prefixing with ROW/UK_ if missing), counts exported, shows success/info/warning messages (e.g., no files for perfect match).
- Edge cases: Handles missing paths/empty DFs with skips/empty displays; UK-specific Barclays declined from multiple folders (barclays/barclaycard/barclay card), concatenating/renaming/sorting/appending to proc_wds_df; numeric cleaning/formatting with format_date/pad_last4; no shifts/regs selected warnings; error handling with tracebacks and critical QMessageBox.
- Standalone run: If __name__=='__main__', creates app and shows window with sample date.

Dependencies:
- PyQt5 (QtWidgets for QWidget, layouts, buttons, labels, tables, dialogs, message boxes, desktop widget, headers, app, size policy, style, checkboxes; QtCore for Qt)
- shutil (for file copying and directory removal)
- pandas (for Excel reading/writing, DataFrame operations, concatenation, sorting, numeric coercion)
- sys (for app execution)
- src.output (for generate_unmatched_crm_deposits, generate_unapproved_crm_deposits, generate_unmatched_proc_deposits, generate_unmatched_proc_withdrawals, remove_compensated_entries, generate_unmatched_crm_withdrawals, generate_matched_deposits, generate_matched_withdrawals, save_unmatched_to_excel, save_matched_to_excel, format_date, pad_last4)
- src.shifts_handler (for main as handle_shifts)
- src.config (for setup_dirs_for_reg)
- pathlib (for Path operations)
- numpy (for np.where in declined processing)
"""

from PyQt5.QtWidgets import QWidget, QVBoxLayout, QPushButton, QLabel, QTableWidget, QTableWidgetItem, QFileDialog, \
    QMessageBox, QDesktopWidget, QHeaderView, QApplication, QHBoxLayout, QSizePolicy, QStyle, QCheckBox
from PyQt5.QtCore import Qt
import shutil
import pandas as pd
import sys
from src.output import (generate_unmatched_crm_deposits, generate_unapproved_crm_deposits,
                generate_unmatched_proc_deposits, generate_unmatched_proc_withdrawals,
                remove_compensated_entries, generate_unmatched_crm_withdrawals,generate_matched_deposits, generate_matched_withdrawals,
                save_unmatched_to_excel,save_matched_to_excel, format_date, pad_last4)
from src.shifts_handler import main as handle_shifts
from src.config import setup_dirs_for_reg
from pathlib import Path
import numpy as np
import traceback

class FourthWindow(QWidget):
    def __init__(self, date_str):
        super().__init__()
        print("Debug: FourthWindow __init__ started")
        self.date_str = date_str
        self.regulations = ['row', 'uk']
        self.initUI()
        print("Debug: initUI completed")
        self.run_output_script()
        print("Debug: run_output_script called")

    def initUI(self):
        """Initialize the user interface components."""
        print("Debug: initUI started")

        self.setWindowTitle('Export Daily Reconciliation Reports')
        self.setGeometry(300, 300, 800, 600)  # Initial size, will be adjusted dynamically

        # Center the window on the screen
        qr = self.frameGeometry()
        cp = QDesktopWidget().availableGeometry().center()
        qr.moveCenter(cp)
        self.move(qr.topLeft())

        # Main layout setup
        layout = QVBoxLayout()
        layout.setSpacing(15)  # Add spacing between widgets for better layout
        layout.setContentsMargins(20, 20, 20, 20)  # Margins for the window content

        # Regulations selection (centered)
        reg_layout = QHBoxLayout()
        reg_layout.addStretch(1)
        self.row_checkbox = QCheckBox('ROW')
        self.row_checkbox.setChecked(True)
        reg_layout.addWidget(self.row_checkbox)
        self.uk_checkbox = QCheckBox('UK')
        self.uk_checkbox.setChecked(True)
        reg_layout.addWidget(self.uk_checkbox)
        reg_layout.addStretch(1)
        layout.addLayout(reg_layout)

        # Export button (initially disabled)
        self.export_btn = QPushButton('Export Daily Reconciliation Reports')
        self.export_btn.setEnabled(False)
        self.export_btn.clicked.connect(self.export_files)
        layout.addWidget(self.export_btn)

        # Shifts area (dynamic)
        self.shifts_layout = QVBoxLayout()
        layout.addLayout(self.shifts_layout)

        self.setLayout(layout)

        # Apply window styles
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

        # Styles for dynamic labels/tables (added here for reuse)
        self.label_style = """
            QLabel {
                font-family: 'Segoe UI', Arial, sans-serif;
                font-size: 16px;
                font-weight: 600;
                color: #2c3e50;
                padding: 10px;
                background: transparent;
                border: none;
            }
        """
        self.table_style = """
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
        """
        print("Debug: initUI finished")

    def display_shifts(self):
        """Display shifts data for each regulation in tables or labels."""
        # Clear existing shifts display
        while self.shifts_layout.count():
            child = self.shifts_layout.takeAt(0)
            if child.widget():
                child.widget().deleteLater()

        shifts_data = {}
        has_any_shifts = False

        # Load shifts data for each regulation
        for regulation in self.regulations:
            dirs = setup_dirs_for_reg(regulation)
            shifts_path = dirs['output_dir'] / self.date_str / f"{regulation.upper()} total_shifts_by_currency.xlsx"
            if shifts_path.exists():
                df = pd.read_excel(shifts_path)
                if not df.empty:
                    shifts_data[regulation] = df.iloc[0].to_dict()
                    has_any_shifts = True

        # Handle case with no shifts
        if not has_any_shifts:
            no_shifts_label = QLabel("No Shifts In The Regulations")
            no_shifts_label.setAlignment(Qt.AlignCenter)
            no_shifts_label.setStyleSheet(self.label_style)
            self.shifts_layout.addWidget(no_shifts_label)
            return

        # Display shifts for each regulation
        for regulation in self.regulations:
            reg_upper = regulation.upper()
            if regulation in shifts_data:
                # Add regulation label
                label_text = f"{reg_upper} Shifts By Currency"
                reg_label = QLabel(label_text)
                reg_label.setAlignment(Qt.AlignCenter)
                reg_label.setStyleSheet(self.label_style)
                self.shifts_layout.addWidget(reg_label)

                # Prepare data for table
                data = shifts_data[regulation]
                currencies = list(data.keys())
                amounts = list(data.values())

                # Table setup
                reg_table = QTableWidget()
                reg_table.setEditTriggers(QTableWidget.NoEditTriggers)
                reg_table.horizontalHeader().setVisible(True)
                reg_table.verticalHeader().setVisible(False)
                reg_table.horizontalHeader().setStretchLastSection(False)
                reg_table.horizontalHeader().setSectionResizeMode(QHeaderView.Fixed)
                reg_table.horizontalHeader().setSectionsClickable(False)
                reg_table.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
                reg_table.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
                reg_table.setAlternatingRowColors(True)
                reg_table.setStyleSheet(self.table_style)
                reg_table.setRowCount(1)
                reg_table.setColumnCount(len(currencies))
                reg_table.setHorizontalHeaderLabels(currencies)

                # Populate table with amounts
                for j, amt in enumerate(amounts):
                    formatted_amt = f"{amt:g}"
                    item = QTableWidgetItem(formatted_amt)
                    item.setTextAlignment(Qt.AlignCenter)
                    reg_table.setItem(0, j, item)

                # Resize columns to contents
                reg_table.resizeColumnsToContents()
                max_width = 0
                for j in range(reg_table.columnCount()):
                    max_width = max(max_width, reg_table.columnWidth(j))
                min_col_width = 120
                col_width = max(max_width, min_col_width)
                for j in range(reg_table.columnCount()):
                    reg_table.setColumnWidth(j, col_width)
                reg_table.setRowHeight(0, 60)

                # Center table in container
                table_container = QWidget()
                table_layout = QHBoxLayout(table_container)
                table_layout.addStretch(1)
                table_layout.addWidget(reg_table)
                table_layout.addStretch(1)
                table_layout.setContentsMargins(0, 0, 0, 0)
                table_layout.setSpacing(0)
                self.shifts_layout.addWidget(table_container)
            else:
                # Add no shifts label for this regulation
                no_shift_label = QLabel(f"No Shifts in {reg_upper}")
                no_shift_label.setAlignment(Qt.AlignCenter)
                no_shift_label.setStyleSheet(self.label_style)
                self.shifts_layout.addWidget(no_shift_label)

    def adjust_window_size(self):
        """Adjust the window size based on content."""
        margins = self.layout().contentsMargins()
        margin_width = margins.left() + margins.right()
        margin_height = margins.top() + margins.bottom()

        # Width: max of button, reg layout, shifts widgets
        button_width = self.export_btn.sizeHint().width()
        reg_layout_width = self.layout().itemAt(0).layout().sizeHint().width()  # reg checkboxes
        max_width = max(button_width, reg_layout_width)

        # Collect shifts widgets for height and width
        total_shifts_height = 0
        max_shifts_width = 0
        tables = []
        labels = []
        for i in range(self.shifts_layout.count()):
            widget = self.shifts_layout.itemAt(i).widget()
            if isinstance(widget, QLabel):
                labels.append(widget)
                if widget.isVisible():
                    total_shifts_height += widget.sizeHint().height()
                    max_shifts_width = max(max_shifts_width, widget.sizeHint().width())
            elif isinstance(widget, QWidget):  # table container
                table = widget.layout().itemAt(1).widget()
                if table:
                    tables.append(table)
                    # Resize table rows
                    table.resizeRowsToContents()
                    header_height = table.horizontalHeader().height()
                    row_height = table.rowHeight(0) if table.rowCount() > 0 else 0
                    table_height = header_height + row_height + 20  # buffer
                    table.setFixedHeight(table_height)
                    total_shifts_height += table_height
                    # Table width from columns
                    col_sum = sum(table.columnWidth(j) for j in range(table.columnCount()))
                    vheader_w = table.verticalHeader().width()
                    frame_w = table.style().pixelMetric(QStyle.PM_DefaultFrameWidth) * 2
                    table_width = col_sum + vheader_w + frame_w
                    max_shifts_width = max(max_shifts_width, table_width)

        max_width = max(max_width, max_shifts_width) + margin_width

        # Height: button + reg_layout + shifts + spacings
        button_height = self.export_btn.sizeHint().height()
        reg_height = self.layout().itemAt(0).layout().sizeHint().height()
        num_widgets = len(labels) + len(tables)  # containers count as one per table
        num_spacings = max(0, 1 + num_widgets)  # between reg, button, shifts
        total_height = button_height + reg_height + total_shifts_height + self.layout().spacing() * num_spacings + margin_height

        # Cap to screen
        available_height = QApplication.desktop().availableGeometry().height()
        frame_overhead = self.frameGeometry().height() - self.height() if self.isVisible() else 40
        final_height = min(total_height + frame_overhead, available_height - 30)

        self.resize(max_width, final_height)

        # Re-center after resize
        qr = self.frameGeometry()
        cp = QDesktopWidget().availableGeometry().center()
        qr.moveCenter(cp)
        self.move(qr.topLeft())

        print(f"Debug: Window resized to {max_width}x{final_height}")

    def is_perfect_match(self):
        """Check if there is a perfect match with no unmatched rows or shifts."""
        for regulation in self.regulations:
            dirs = setup_dirs_for_reg(regulation)
            lists_dir = dirs['lists_dir'] / self.date_str
            deposits_path = lists_dir / f"{regulation}_deposits_matching.xlsx"
            withdrawals_path = lists_dir / f"{regulation}_withdrawals_matching.xlsx"
            shifts_path = dirs['output_dir'] / self.date_str / f"{regulation.upper()} total_shifts_by_currency.xlsx"

            # Check deposits
            if deposits_path.exists():
                try:
                    df_d = pd.read_excel(deposits_path)
                    if 'match_status' in df_d.columns and (df_d['match_status'] == 0).any():
                        return False
                except Exception as e:
                    print(f"Debug: Error checking deposits for {regulation}: {e}")
                    return False

            # Check withdrawals
            if withdrawals_path.exists():
                try:
                    df_w = pd.read_excel(withdrawals_path)
                    if 'match_status' in df_w.columns and (df_w['match_status'] == 0).any():
                        return False
                except Exception as e:
                    print(f"Debug: Error checking withdrawals for {regulation}: {e}")
                    return False

            # Check shifts
            if shifts_path.exists():
                try:
                    df_s = pd.read_excel(shifts_path)
                    if not df_s.empty:
                        return False
                except Exception as e:
                    print(f"Debug: Error checking shifts for {regulation}: {e}")

        return True

    def run_output_script(self):
        """Run the output generation script for reports."""
        print("Debug: run_output_script started")
        try:
            matched_sums = handle_shifts(self.date_str)

            for regulation in self.regulations:
                dirs = setup_dirs_for_reg(regulation)
                output_dir = dirs['output_dir'] / self.date_str
                output_dir.mkdir(parents=True, exist_ok=True)

                # Clear output_dir except for specific files
                files_to_keep = [f"{regulation}_warnings_withdrawals.xlsx", "withdrawals_matching_updated.xlsx"]
                for item in list(output_dir.iterdir()):
                    if item.name not in files_to_keep:
                        if item.is_file():
                            item.unlink()
                        else:
                            shutil.rmtree(item)

                # Save shifts for this regulation if data exists
                if matched_sums and regulation in matched_sums:
                    df = pd.DataFrame([matched_sums[regulation]])
                    if not df.empty:
                        shifts_path = output_dir / f"{regulation.upper()} total_shifts_by_currency.xlsx"
                        df.to_excel(shifts_path, index=False)

                # Generate output files for this regulation
                lists_dir = dirs['lists_dir']
                crm_deps_df = generate_unmatched_crm_deposits(self.date_str, lists_dir, regulation)
                generate_unapproved_crm_deposits(self.date_str, lists_dir, output_dir, regulation)
                proc_deps_df = generate_unmatched_proc_deposits(self.date_str, lists_dir, regulation)
                proc_wds_df = generate_unmatched_proc_withdrawals(self.date_str, lists_dir, output_dir, regulation)
                proc_deps_df, proc_wds_df, compensated_deps, compensated_wds = remove_compensated_entries(proc_deps_df, proc_wds_df)
                crm_wds_df = generate_unmatched_crm_withdrawals(self.date_str, lists_dir, output_dir, regulation)

                # Add UK declined processing here (copied from output.py)
                declined_df = None
                if regulation == 'uk':
                    print("Starting Barclays declined processing for UK")
                    processed_processor_dir = dirs['processed_processor_dir']
                    declined_dfs = []
                    for folder in ['barclays', 'barclaycard', 'barclay card']:
                        declined_path = processed_processor_dir / folder / self.date_str / f"{folder}_declined_withdrawals.xlsx"
                        print(f"Checking declined file: {declined_path}")
                        if declined_path.exists():
                            print(f"Found {declined_path} - loading...")
                            declined_raw = pd.read_excel(declined_path)
                            print(f"Loaded raw declined rows: {len(declined_raw)}")
                            declined_temp = declined_raw.copy()
                            declined_temp['amount'] = pd.to_numeric(declined_temp['amount'], errors='coerce')
                            declined_temp['amount'] = declined_temp['amount'].apply(lambda x: -abs(x) if pd.notna(x) else x)
                            declined_temp['date'] = declined_temp['date'].apply(lambda x: format_date(x, is_proc=True))
                            pad_last4(declined_temp, 'last_4cc')
                            declined_temp = declined_temp.rename(columns={
                                'date': 'Date',
                                'first_name': 'First Name',
                                'last_name': 'Last Name',
                                'email': 'Email',
                                'amount': 'Amount',
                                'currency': 'Currency',
                                'tp': 'TP',
                                'processor_name': 'Processor Name',
                                'last_4cc': 'Last 4 Digits'
                            })
                            declined_temp['Type'] = 'Withdrawal'
                            declined_temp['Processor Name'] = 'Barclays'  # Standardize name
                            declined_temp['Comment'] = 'Withdrawal Declined'
                            declined_temp = declined_temp[[
                                'Type', 'Date', 'First Name', 'Last Name', 'Email',
                                'Amount', 'Currency', 'TP', 'Processor Name', 'Last 4 Digits', 'Comment'
                            ]]
                            declined_dfs.append(declined_temp)
                            print(f"Appended processed declined for {folder}: {len(declined_temp)} rows")
                        else:
                            print(f"No file at {declined_path}")
                    if declined_dfs:
                        declined_df = pd.concat(declined_dfs, ignore_index=True)
                        declined_df['Date'] = pd.to_datetime(declined_df['Date'], errors='coerce')
                        declined_df = declined_df.sort_values(by='Date', ascending=False)
                        print(f"Loaded and prepared Barclays Declined WDs: {len(declined_df)} rows")
                        # Append to proc_wds_df
                        if proc_wds_df is not None and not proc_wds_df.empty:
                            proc_wds_df['Date'] = pd.to_datetime(proc_wds_df['Date'], errors='coerce')
                            proc_wds_df = proc_wds_df.sort_values(by='Date', ascending=False)
                            print(f"Pre-append: proc_wds_df rows = {len(proc_wds_df)}")
                            print(proc_wds_df.tail(3))
                        else:
                            print("proc_wds_df is empty or None before append")
                            proc_wds_df = pd.DataFrame()
                        proc_wds_df = pd.concat([proc_wds_df, declined_df], ignore_index=True)
                        print(f"Post-append: proc_wds_df rows = {len(proc_wds_df)}")
                        print(proc_wds_df.tail(3))
                        # Force declined last
                        proc_wds_df['Date'] = pd.to_datetime(proc_wds_df['Date'], errors='coerce')
                        proc_wds_df['is_declined'] = np.where(proc_wds_df['Comment'] == 'Withdrawal Declined', 1, 0)
                        proc_wds_df = proc_wds_df.sort_values(by=['is_declined', 'Date'], ascending=[True, False])
                        proc_wds_df = proc_wds_df.drop(columns=['is_declined'])
                        print(f"Final proc_wds_df after sort: {len(proc_wds_df)} rows")
                        print(proc_wds_df.tail(3))
                    else:
                        print("No Barclays declined withdrawals file found; skipping tab.")

                deps_df = generate_matched_deposits(self.date_str, lists_dir, regulation, compensated_deps)
                wds_df = generate_matched_withdrawals(self.date_str, regulation, lists_dir, output_dir, compensated_wds)
                save_matched_to_excel(self.date_str, regulation, deps_df, wds_df, output_dir)
                save_unmatched_to_excel(self.date_str, regulation, crm_deps_df, proc_deps_df, crm_wds_df, proc_wds_df, output_dir)

            # Populate UI
            self.display_shifts()
            self.export_btn.setEnabled(True)
            self.adjust_window_size()

            if self.is_perfect_match():
                self.shifts_label.setText("Perfect Reconciliation - No Shifts or Unmatched Rows")
                self.shifts_label.show()
                self.table_container.hide()
        except Exception as e:
            print(f"Error executing output phase 2: {e}")
            traceback.print_exc()
            QMessageBox.critical(self, "Error", f"Failed to run output phase 2: {e}")
        print("Debug: run_output_script finished")

    def export_files(self):
        """Export selected files to a chosen directory."""
        print("Debug: export_files started")

        if not self.row_checkbox.isChecked() and not self.uk_checkbox.isChecked():
            QMessageBox.warning(self, "Error", "Regulation must be picked before Exporting")
            return

        dest_folder = QFileDialog.getExistingDirectory(self, "Select Folder to Export To")
        if dest_folder:
            exported_count = 0
            selected_regs = []
            if self.row_checkbox.isChecked():
                selected_regs.append('row')
            if self.uk_checkbox.isChecked():
                selected_regs.append('uk')

            for reg in selected_regs:
                dirs = setup_dirs_for_reg(reg)
                source_folder = dirs['output_dir'] / self.date_str
                if source_folder.exists():
                    for file in source_folder.iterdir():
                        if file.is_file() and "warnings_withdrawals" not in file.name and "withdrawals_matching_updated" not in file.name:
                            reg_upper = reg.upper()
                            if file.name.startswith(reg_upper + ' ') or file.name.startswith(reg_upper + '_'):
                                dest_file = Path(dest_folder) / file.name
                            else:
                                dest_file = Path(dest_folder) / f"{reg_upper}_{file.name}"
                            shutil.copy(str(file), str(dest_file))
                            exported_count += 1

            if exported_count > 0:
                QMessageBox.information(self, "Success", f"{exported_count} files exported to {dest_folder}")
            else:
                if self.is_perfect_match():
                    alert_msg = "Congratulations! Every row has matched perfectly with no discrepancies or shifts detected. No additional output reports are required for this date."
                    QMessageBox.information(self, "Perfect Reconciliation", alert_msg)
                else:
                    QMessageBox.warning(self, "No Files",
                                        "No files to export (excluding warnings and updated matching).")

        print("Debug: export_files finished")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = FourthWindow("2025-08-06")
    window.show()
    sys.exit(app.exec_())