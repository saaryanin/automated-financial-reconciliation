# src/third_window.py
from PyQt5.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QTextEdit, QLabel, QTableWidget, QTableWidgetItem, QMessageBox, QDesktopWidget, QApplication, QHeaderView
from PyQt5.QtCore import Qt, QItemSelectionModel, QItemSelection
import pandas as pd
import numpy as np
import sys
import os
from pathlib import Path
from src.config import LISTS_DIR, OUTPUT_DIR
from src.output import clean_value, format_date, process_comment, save_excel
from src.shifts_handler import main as handle_shifts
import shutil
from fourth_window import FourthWindow  # Import to open next window

class ThirdWindow(QWidget):
    def __init__(self, date_str):
        super().__init__()
        self.date_str = date_str
        self.initUI()
        self.run_initial_phase()

    def initUI(self):
        self.setWindowTitle('Review Shifts and Warnings')
        screen_width = QApplication.desktop().screenGeometry().width()
        self.resize(screen_width, 800)
        qr = self.frameGeometry()
        cp = QDesktopWidget().availableGeometry().center()
        qr.moveCenter(cp)
        self.move(qr.topLeft())
        layout = QVBoxLayout()
        # Shifts summary (read-only, made shorter and smaller)
        shifts_label_hbox = QHBoxLayout()
        shifts_label = QLabel('Total Shifts by Currency (Read-Only):')
        shifts_label.setFixedWidth(220)  # 10% wider than text (200 + 20)
        shifts_label_hbox.addStretch(1)
        shifts_label_hbox.addWidget(shifts_label)
        shifts_label_hbox.addStretch(1)
        layout.addLayout(shifts_label_hbox)
        shifts_hbox = QHBoxLayout()
        self.shifts_text = QTextEdit()
        self.shifts_text.setReadOnly(True)
        self.shifts_text.setFixedHeight(80)  # Fixed shorter height
        self.shifts_text.setFixedWidth(200)  # Narrow width
        shifts_hbox.addStretch(1)
        shifts_hbox.addWidget(self.shifts_text)
        shifts_hbox.addStretch(1)
        layout.addLayout(shifts_hbox)
        # Placeholder for warnings tables
        self.layout = layout
        # Buttons
        button_layout = QHBoxLayout()
        self.remove_btn = QPushButton('Remove Selected (Accept Match)')
        self.remove_btn.clicked.connect(self.remove_selected)
        button_layout.addWidget(self.remove_btn)
        next_btn = QPushButton('Next')
        next_btn.clicked.connect(self.on_next)
        button_layout.addWidget(next_btn)
        layout.addLayout(button_layout)
        self.setLayout(layout)
        self.setStyleSheet("""
            QWidget {
                font-family: 'Segoe UI', Arial, sans-serif;
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #4a90e2, stop:1 #d3d8e8);
                border-radius: 10px;
                padding: 10px;
            }
            QPushButton#row_button {
                font-size: 18px;
                min-width: 30px;
                min-height: 30px;
                max-width: 30px;
                max-height: 30px;
                padding: 0px;
                text-align: center;
                border: none;
                background: transparent;
            }
            QPushButton:hover {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #357abd, stop:1 #2a609d);
                box-shadow: 0 4px 10px rgba(74, 144, 226, 0.4);
            }
            QTextEdit {
                background: #ffffff;
                border: 1px solid #dfe6e9;
                border-radius: 4px;
                padding: 10px;
                font-family: 'Consolas', 'Courier New', monospace;
                font-size: 12px;
                color: #2c3e50;
            }
            QTableWidget {
                background: #ffffff;
                border: 1px solid #dfe6e9;
                border-radius: 4px;
                padding: 10px;
                font-size: 12px;
                color: #2c3e50;
            }
            QTableWidget::item:selected {
                background: transparent;
                color: #2c3e50;
            }
            QHeaderView::section {
                background-color: #f0f0f0;
                padding: 4px 8px 4px 4px;
                border: 1px solid #dfe6e9;
                font-size: 12px;
                height:38px;
            }
            QHeaderView::down-arrow, QHeaderView::up-arrow {
                image: none;
            }
            QTableView::item {
                padding-left: 4px;
            }
            QPushButton#row_button {
                border-radius: 0;
                font-size: 20px;
                min-width: 30px;
                min-height: 30px;
                max-width: 30px;
                max-height: 30px;
                padding: 0px;
                text-align: center;
            }
        """)

    def run_initial_phase(self):
        try:
            output_dir = OUTPUT_DIR / self.date_str
            if output_dir.exists():
                shutil.rmtree(output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            matched_sums = handle_shifts(self.date_str)
            if matched_sums:
                output_path = output_dir / "total_shifts_by_currency.csv"
                df = pd.DataFrame([matched_sums])
                df.to_csv(output_path, index=False)
                shifts_str = "\n".join([f"{curr}: {amt}" for curr, amt in matched_sums.items()])
                self.shifts_text.setText(shifts_str)
            # Load and prepare warnings
            withdrawals_matching_path = LISTS_DIR / self.date_str / "withdrawals_matching.xlsx"
            if not withdrawals_matching_path.exists():
                raise FileNotFoundError(f"Withdrawals matching file not found: {withdrawals_matching_path}")
            full_df = pd.read_excel(withdrawals_matching_path)
            self.warnings_df = full_df[full_df['warning'] == True].copy()  # Keep full for later
            print("Warnings DF shape:", self.warnings_df.shape)
            print("Warnings DF columns:", list(self.warnings_df.columns))
            if self.warnings_df.empty:
                return
            # Clean and process as before
            columns_to_clean = [
                'proc_date', 'proc_email', 'proc_tp', 'proc_firstname', 'proc_lastname',
                'proc_last4', 'proc_currency', 'proc_amount', 'proc_amount_crm_currency'
            ]
            for col in columns_to_clean:
                if col in self.warnings_df.columns:
                    self.warnings_df[col] = self.warnings_df[col].apply(clean_value)
            self.warnings_df['proc_date'] = self.warnings_df['proc_date'].apply(format_date)
            self.warnings_df['crm_amount'] = self.warnings_df['crm_amount'].apply(
                lambda x: -abs(x) if pd.notna(x) else x)
            self.warnings_df['proc_amount'] = self.warnings_df['proc_amount'].apply(
                lambda x: -abs(x) if pd.notna(x) else x)
            self.warnings_df['comment'] = self.warnings_df['comment'].apply(process_comment)
            # Select display columns
            display_columns = [
                'crm_email', 'crm_amount', 'crm_currency', 'crm_tp', 'crm_processor_name', 'crm_last4',
                'proc_email', 'proc_amount', 'proc_currency', 'proc_tp', 'proc_processor_name', 'proc_last4', 'comment'
            ]
            display_df = self.warnings_df[display_columns]
            # Split into differ and other
            differ_mask = self.warnings_df['comment'].str.contains("Processor names differ", na=False)
            differ_df = display_df[differ_mask]
            other_df = display_df[~differ_mask]
            self.differ_indices = self.warnings_df.index[differ_mask].tolist()
            self.other_indices = self.warnings_df.index[~differ_mask].tolist()
            print("Differ indices:", self.differ_indices)
            print("Other indices:", self.other_indices)
            self.accepted_rows = {}
            # Add differ table if not empty
            if not differ_df.empty:
                if len(differ_df) == 1:
                    differ_label_text = 'Warnings - Cross Processor Withdrawal Detected'
                else:
                    differ_label_text = 'Warnings - Cross Processors Withdrawals Detected'
                differ_label = QLabel(differ_label_text)
                self.layout.addWidget(differ_label)
                self.differ_table = QTableWidget()
                self.differ_table.setSelectionMode(QTableWidget.NoSelection)
                self.differ_table.setEditTriggers(QTableWidget.NoEditTriggers)
                visible_columns = [''] + display_columns  # Empty for button column
                self.differ_table.setColumnCount(len(visible_columns) + 1)  # +1 for hidden orig_index
                self.differ_table.setHorizontalHeaderLabels(['orig_index'] + visible_columns)
                self.differ_table.horizontalHeader().setVisible(True)
                self.differ_table.verticalHeader().setVisible(False)
                self.differ_table.setRowCount(len(differ_df))
                self.accepted_rows[self.differ_table] = set()
                for i, row_idx in enumerate(self.differ_indices):
                    self.differ_table.setItem(i, 0, QTableWidgetItem(str(row_idx)))
                    # Button column
                    button = QPushButton('✅')
                    button.setObjectName('row_button')
                    button.setStyleSheet("color: green; background: transparent; border: none;")
                    button.clicked.connect(lambda checked, tbl=self.differ_table, rw=i: self.toggle_accept(tbl, rw))
                    container = QWidget()
                    container_layout = QHBoxLayout()
                    container_layout.addStretch(1)
                    container_layout.addWidget(button)
                    container_layout.addStretch(1)
                    container_layout.setAlignment(Qt.AlignCenter)
                    container_layout.setContentsMargins(0, 0, 0, 0)
                    container.setLayout(container_layout)
                    self.differ_table.setCellWidget(i, 1, container)
                    # Data columns
                    for j, col in enumerate(display_columns):
                        val = differ_df.iloc[i][col]
                        item_text = '' if pd.isna(val) else str(val)
                        item = QTableWidgetItem(item_text)
                        item.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
                        self.differ_table.setItem(i, j + 2, item)
                self.differ_table.hideColumn(0)
                self.differ_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
                self.differ_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Fixed)
                self.differ_table.setWordWrap(True)
                self.differ_table.resizeRowsToContents()
                self.differ_table.setColumnWidth(1, 40)  # Narrow button column with space
                self.layout.addWidget(self.differ_table)
            # Add other CRM and Proc tables if not empty
            if not other_df.empty:
                crm_columns = ['crm_email', 'crm_amount', 'crm_currency', 'crm_tp', 'crm_processor_name', 'crm_last4',
                               'comment']
                proc_columns = ['proc_email', 'proc_amount', 'proc_currency', 'proc_tp', 'proc_processor_name',
                                'proc_last4', 'comment']
                # Filter rows for CRM table: where crm_email is not nan
                crm_mask = other_df['crm_email'].notna()
                crm_display = other_df[crm_mask]
                crm_indices = [self.other_indices[i] for i in range(len(other_df)) if crm_mask.iloc[i]]
                # Filter rows for Proc table: where proc_email is not na
                proc_mask = other_df['proc_email'].notna()
                proc_display = other_df[proc_mask]
                proc_indices = [self.other_indices[i] for i in range(len(other_df)) if proc_mask.iloc[i]]
                print("CRM display shape:", crm_display.shape)
                print("Proc display shape:", proc_display.shape)
                # CRM table
                if not crm_display.empty:
                    crm_label = QLabel('Warnings - CRM Side')
                    self.layout.addWidget(crm_label)
                    self.crm_table = QTableWidget()
                    self.crm_table.setSelectionMode(QTableWidget.NoSelection)
                    self.crm_table.setEditTriggers(QTableWidget.NoEditTriggers)
                    visible_crm_columns = [''] + crm_columns
                    self.crm_table.setColumnCount(len(visible_crm_columns) + 1)
                    self.crm_table.setHorizontalHeaderLabels(['orig_index'] + visible_crm_columns)
                    self.crm_table.horizontalHeader().setVisible(True)
                    self.crm_table.verticalHeader().setVisible(False)
                    self.crm_table.setRowCount(len(crm_display))
                    self.accepted_rows[self.crm_table] = set()
                    for i in range(len(crm_display)):
                        row_idx = crm_indices[i]
                        self.crm_table.setItem(i, 0, QTableWidgetItem(str(row_idx)))
                        # Button column
                        button = QPushButton('✅')
                        button.setObjectName('row_button')
                        button.setStyleSheet("color: green; background: transparent; border: none;")
                        button.clicked.connect(lambda checked, tbl=self.crm_table, rw=i: self.toggle_accept(tbl, rw))
                        container = QWidget()
                        container_layout = QHBoxLayout()
                        container_layout.addStretch(1)
                        container_layout.addWidget(button)
                        container_layout.addStretch(1)
                        container_layout.setAlignment(Qt.AlignCenter)
                        container_layout.setContentsMargins(0, 0, 0, 0)
                        container.setLayout(container_layout)
                        self.crm_table.setCellWidget(i, 1, container)
                        # Data columns
                        for j, col in enumerate(crm_columns):
                            val = crm_display.iloc[i][col]
                            item_text = '' if pd.isna(val) else str(val)
                            item = QTableWidgetItem(item_text)
                            item.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
                            self.crm_table.setItem(i, j + 2, item)
                    self.crm_table.hideColumn(0)
                    self.crm_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
                    self.crm_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Fixed)
                    self.crm_table.setWordWrap(True)
                    self.crm_table.resizeRowsToContents()
                    self.crm_table.setColumnWidth(1, 40)  # Narrow button column with space
                    self.layout.addWidget(self.crm_table)
                # Proc table
                if not proc_display.empty:
                    proc_label = QLabel('Warnings - Processor Side')
                    self.layout.addWidget(proc_label)
                    self.proc_table = QTableWidget()
                    self.proc_table.setSelectionMode(QTableWidget.NoSelection)
                    self.proc_table.setEditTriggers(QTableWidget.NoEditTriggers)
                    visible_proc_columns = [''] + proc_columns
                    self.proc_table.setColumnCount(len(visible_proc_columns) + 1)
                    self.proc_table.setHorizontalHeaderLabels(['orig_index'] + visible_proc_columns)
                    self.proc_table.horizontalHeader().setVisible(True)
                    self.proc_table.verticalHeader().setVisible(False)
                    self.proc_table.setRowCount(len(proc_display))
                    self.accepted_rows[self.proc_table] = set()
                    for i in range(len(proc_display)):
                        row_idx = proc_indices[i]
                        self.proc_table.setItem(i, 0, QTableWidgetItem(str(row_idx)))
                        # Button column
                        button = QPushButton('✅')
                        button.setObjectName('row_button')
                        button.setStyleSheet("color: green; background: transparent; border: none;")
                        button.clicked.connect(lambda checked, tbl=self.proc_table, rw=i: self.toggle_accept(tbl, rw))
                        container = QWidget()
                        container_layout = QHBoxLayout()
                        container_layout.addStretch(1)
                        container_layout.addWidget(button)
                        container_layout.addStretch(1)
                        container_layout.setAlignment(Qt.AlignCenter)
                        container_layout.setContentsMargins(0, 0, 0, 0)
                        container.setLayout(container_layout)
                        self.proc_table.setCellWidget(i, 1, container)
                        # Data columns
                        for j, col in enumerate(proc_columns):
                            val = proc_display.iloc[i][col]
                            item_text = '' if pd.isna(val) else str(val)
                            item = QTableWidgetItem(item_text)
                            item.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
                            self.proc_table.setItem(i, j + 2, item)
                    self.proc_table.hideColumn(0)
                    self.proc_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
                    self.proc_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Fixed)
                    self.proc_table.setWordWrap(True)
                    self.proc_table.resizeRowsToContents()
                    self.proc_table.setColumnWidth(1, 40)  # Narrow button column with space
                    self.layout.addWidget(self.proc_table)
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to load data: {e}")

    def toggle_accept(self, table, row):
        if row in self.accepted_rows[table]:
            self.accepted_rows[table].remove(row)
            button = table.cellWidget(row, 1).layout().itemAt(1).widget() if table.cellWidget(row,1).layout() else table.cellWidget(row, 1)
            button.setText('✅')
            button.setStyleSheet("color: green; background: transparent; border: none;")
        else:
            self.accepted_rows[table].add(row)
            button = table.cellWidget(row, 1).layout().itemAt(1).widget() if table.cellWidget(row,1).layout() else table.cellWidget(row, 1)
            button.setText('X')
            button.setStyleSheet("color: white; background: red; border: none; font-size: 16px;")

    def remove_selected(self):
        for table in self.accepted_rows:
            rows = sorted(list(self.accepted_rows[table]), reverse=True)
            for r in rows:
                idx_item = table.item(r, 0)
                if idx_item:
                    orig_idx = int(idx_item.text())
                    self.remove_rows_by_index(orig_idx)
            self.accepted_rows[table].clear()

    def remove_rows_by_index(self, orig_idx):
        for t in [getattr(self, attr, None) for attr in ['differ_table', 'crm_table', 'proc_table']]:
            if t:
                rows_to_remove = []
                for r in range(t.rowCount()):
                    idx_item = t.item(r, 0)
                    if idx_item and int(idx_item.text()) == orig_idx:
                        rows_to_remove.append(r)
                for r in sorted(rows_to_remove, reverse=True):
                    t.removeRow(r)

    def on_next(self):
        tables = [getattr(self, attr, None) for attr in ['differ_table', 'crm_table', 'proc_table'] if getattr(self, attr, None)]
        remaining_indices = set()
        for t in tables:
            for r in range(t.rowCount()):
                idx_item = t.item(r, 0)
                if idx_item:
                    remaining_indices.add(int(idx_item.text()))
        removed_indices = set(self.warnings_df.index) - remaining_indices
        # Update matching_df
        withdrawals_matching_path = LISTS_DIR / self.date_str / "withdrawals_matching.xlsx"
        matching_df = pd.read_excel(withdrawals_matching_path)
        for idx in removed_indices:
            matching_df.at[idx, 'warning'] = False
        for idx in sorted(list(remaining_indices), reverse=True):
            row = matching_df.loc[idx]
            crm_row = row.copy()
            proc_cols = [c for c in matching_df.columns if c.startswith('proc_')]
            crm_row[proc_cols] = np.nan
            crm_row['match_status'] = 0
            crm_row['payment_status'] = 0
            crm_row['warning'] = False
            crm_row['comment'] = f"Unmatched due to warning: {row['comment']}"
            proc_row = row.copy()
            crm_cols = [c for c in matching_df.columns if c.startswith('crm_') and c != 'crm_type']
            proc_row[crm_cols] = np.nan
            proc_row['match_status'] = 0
            proc_row['payment_status'] = 0
            proc_row['warning'] = False
            proc_row['comment'] = "No matching CRM row found (due to warning)"
            matching_df = matching_df.drop(idx)
            matching_df = pd.concat([matching_df, pd.DataFrame([crm_row]), pd.DataFrame([proc_row])], ignore_index=True)
        matching_df.to_excel(withdrawals_matching_path, index=False)
        # Save kept warnings, splitting other
        display_columns = [
            'crm_email', 'crm_amount', 'crm_currency', 'crm_tp', 'crm_processor_name', 'crm_last4',
            'proc_email', 'proc_amount', 'proc_currency', 'proc_tp', 'proc_processor_name', 'proc_last4', 'comment'
        ]
        kept_df = pd.DataFrame(columns=display_columns)
        differ_mask = self.warnings_df['comment'].str.contains("Processor names differ", na=False)
        differ_remaining = [idx for idx in remaining_indices if differ_mask.loc[idx]]
        other_remaining = [idx for idx in remaining_indices if not differ_mask.loc[idx]]
        for idx in differ_remaining:
            row = self.warnings_df.loc[idx][display_columns]
            kept_df = pd.concat([kept_df, pd.DataFrame([row])], ignore_index=True)
        crm_cols = ['crm_email', 'crm_amount', 'crm_currency', 'crm_tp', 'crm_processor_name', 'crm_last4', 'comment']
        proc_cols = ['proc_email', 'proc_amount', 'proc_currency', 'proc_tp', 'proc_processor_name', 'proc_last4', 'comment']
        for idx in other_remaining:
            full_row = self.warnings_df.loc[idx][display_columns]
            crm_row = pd.Series(np.nan, index=display_columns)
            crm_row[crm_cols] = full_row[crm_cols]
            proc_row = pd.Series(np.nan, index=display_columns)
            proc_row[proc_cols] = full_row[proc_cols]
            kept_df = pd.concat([kept_df, pd.DataFrame([crm_row]), pd.DataFrame([proc_row])], ignore_index=True)
        output_dir = OUTPUT_DIR / self.date_str
        output_path = output_dir / "warnings_withdrawals.xlsx"
        if not kept_df.empty:
            save_excel(kept_df, output_path, text_columns=['crm_last4', 'proc_last4'])
        # Proceed to fourth window
        self.fourth_window = FourthWindow(self.date_str)
        self.fourth_window.show()
        self.close()