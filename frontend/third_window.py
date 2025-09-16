from PyQt5.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QTextEdit, QLabel, QTableWidget, QTableWidgetItem, QMessageBox, QDesktopWidget, QApplication, QHeaderView
from PyQt5.QtCore import Qt, QItemSelectionModel, QItemSelection
import pandas as pd
import numpy as np
import re
from src.config import LISTS_DIR, OUTPUT_DIR
import shutil
from src.output import clean_value, format_date, process_comment, save_excel, generate_unmatched_crm_withdrawals, generate_unmatched_proc_withdrawals, generate_warning_withdrawals,process_unmatched_comment
from fourth_window import FourthWindow # Import to open next window
class ThirdWindow(QWidget):

    def __init__(self, date_str):
        output_dir = OUTPUT_DIR / date_str
        if output_dir.exists():
            shutil.rmtree(output_dir)
            print(f"Cleared stale output dir for date {date_str} to prevent file bleed")
        output_dir.mkdir(parents=True, exist_ok=True)
        super().__init__()
        self.date_str = date_str
        self.screen_width = QApplication.desktop().screenGeometry().width()
        self.available_height = QApplication.desktop().availableGeometry().height()
        self.initUI()
        self.run_initial_phase()
        self.adjust_tables_and_window()
    def initUI(self):
        self.setWindowTitle('Review Warning Withdrawals')
        layout = QVBoxLayout()
        layout.setSpacing(10)
        layout.setContentsMargins(10, 10, 10, 10)
        # Placeholder for warnings tables
        self.layout = layout
        # Buttons
        button_layout = QHBoxLayout()
        self.remove_btn = QPushButton('Remove Selected (Accept Match)')
        self.remove_btn.clicked.connect(self.remove_selected)
        self.remove_btn.setEnabled(False)
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
            QPushButton:disabled {
                background-color: #cccccc;
                color: #666666;
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
                padding: 4px;
                border: 1px solid #dfe6e9;
                font-size: 12px;
                height:38px;
            }
            QHeaderView::down-arrow, QHeaderView::up-arrow {
                image: none;
            }
            QTableView::item {
                padding: 4px;
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
            QLabel {
                max-height: 30px;
            }
        """)
    def format_cell_value(self, val, col):
        if pd.isna(val):
            return ''
        if col in ['CRM Amount', 'PSP Amount']:
            if isinstance(val, (int, float)):
                if isinstance(val, float) and val.is_integer():
                    return str(int(val))
                else:
                    return f"{val:.1f}"
            return str(val)
        elif col in ['CRM TP', 'PSP TP', 'CRM Last 4 Digits', 'PSP Last 4 Digits']:
            if isinstance(val, (int, float)):
                return str(int(val))
            return str(val)
        else:
            return str(val)
    def extract_match_key(self, comment):
        if pd.isna(comment) or str(comment).strip() == '':
            return '', ''
        comment_str = str(comment)
        # Prefer last occurrence
        last4_match = re.search(r'last4\s*:\s*([^\s, .]+)', comment_str)
        if last4_match:
            val = last4_match.group(1).strip()
            return 'last4', val
        email_match = re.search(r'email\s*:\s*(.+?)(?:\s+in\s|(?:\s*\.|$))', comment_str)
        if email_match:
            val = email_match.group(1).strip()
            return 'email', val.lower()
        return '', comment_str.lower() or ''
    def run_initial_phase(self):
        try:
            output_dir = OUTPUT_DIR / self.date_str
            output_dir.mkdir(parents=True, exist_ok=True)
            # NEW: Remove any stale warnings file before regenerating
            warnings_withdrawals_path = output_dir / "warnings_withdrawals.xlsx"
            if warnings_withdrawals_path.exists():
                warnings_withdrawals_path.unlink()
                print(f"Removed stale warnings_withdrawals.xlsx for {self.date_str}")
            # Generate warnings if not exists
            generate_warning_withdrawals(self.date_str)
            # Load warnings directly from warnings_withdrawals file
            warnings_withdrawals_path = output_dir / "warnings_withdrawals.xlsx"
            if not warnings_withdrawals_path.exists():
                print(f"Warnings withdrawals file not found: {warnings_withdrawals_path}. Proceeding with empty warnings.")
                self.warnings_df = pd.DataFrame(columns=['crm_email', 'crm_amount', 'crm_currency', 'crm_tp', 'crm_processor_name', 'crm_last4', 'proc_email', 'proc_amount', 'proc_currency', 'proc_tp', 'proc_processor_name', 'proc_last4', 'comment', 'crm_date', 'proc_date'])
                self.orig_indices = np.array([])
                self.orig_to_local = {}
                QMessageBox.information(self, "Info", "No warnings file found. Skipping review and proceeding to export.")
            else:
                self.warnings_df = pd.read_excel(warnings_withdrawals_path)
                self.orig_indices = self.warnings_df['orig_index'].values
                self.warnings_df = self.warnings_df.drop('orig_index', axis=1)
                self.orig_to_local = {self.orig_indices[i]: i for i in range(len(self.orig_indices))}
            print("Warnings DF shape:", self.warnings_df.shape)
            print("Warnings DF columns:", list(self.warnings_df.columns))
            if self.warnings_df.empty:
                return
            # Clean processor columns
            columns_to_clean = [
                'proc_date', 'proc_email', 'proc_tp', 'proc_firstname', 'proc_lastname',
                'proc_last4', 'proc_currency', 'proc_amount', 'proc_amount_crm_currency',
                'crm_amount', 'crm_tp', 'crm_last4'
            ]
            for col in columns_to_clean:
                if col in self.warnings_df.columns:
                    self.warnings_df[col] = self.warnings_df[col].apply(clean_value)
            self.warnings_df['proc_date'] = self.warnings_df['proc_date'].apply(lambda x: format_date(x, is_proc=True))
            self.warnings_df['crm_date'] = self.warnings_df['crm_date'].apply(lambda x: format_date(x, is_proc=False))  # Explicit format for dates
            self.warnings_df['crm_amount'] = self.warnings_df['crm_amount'].apply(
                lambda x: -abs(x) if pd.notna(x) else x)
            self.warnings_df['proc_amount'] = self.warnings_df['proc_amount'].apply(
                lambda x: -abs(x) if pd.notna(x) else x)
            self.warnings_df['comment'] = self.warnings_df['comment'].apply(process_comment)
            # Rename columns for display
            rename_dict = {
                'crm_email': 'CRM Email',
                'crm_amount': 'CRM Amount',
                'crm_currency': 'CRM Currency',
                'crm_tp': 'CRM TP',
                'crm_processor_name': 'CRM Processor Name',
                'crm_last4': 'CRM Last 4 Digits',
                'proc_email': 'PSP Email',
                'proc_amount': 'PSP Amount',
                'proc_currency': 'PSP Currency',
                'proc_tp': 'PSP TP',
                'proc_processor_name': 'PSP Processor Name',
                'proc_last4': 'PSP Last 4 Digits',
            }
            self.warnings_df.rename(columns=rename_dict, inplace=True)
            # Define display columns with new names
            display_columns = [
                'CRM Email', 'CRM Amount', 'CRM Currency', 'CRM TP', 'CRM Processor Name', 'CRM Last 4 Digits',
                'PSP Email', 'PSP Amount', 'PSP Currency', 'PSP TP', 'PSP Processor Name',
                'PSP Last 4 Digits', 'comment'
            ]
            # Select display columns
            self.display_df = self.warnings_df[display_columns].copy()
            # Split into differ and other
            differ_mask = self.warnings_df['comment'].str.contains("Cross-processor fallback match", na=False)
            differ_local_indices = list(self.display_df[differ_mask].index)
            self.differ_orig_indices = [self.orig_indices[l] for l in differ_local_indices]
            differ_df = self.display_df.loc[differ_local_indices].copy()
            other_df = self.display_df[~differ_mask].copy()
            print("Differ local indices:", differ_local_indices)
            print("Differ orig indices:", self.differ_orig_indices)
            self.accepted_rows = {}
            # Define CRM and Proc columns with new names
            crm_columns = ['CRM Email', 'CRM Amount', 'CRM Currency', 'CRM TP', 'CRM Processor Name', 'CRM Last 4 Digits',
                           'comment']
            proc_columns = ['PSP Email', 'PSP Amount', 'PSP Currency', 'PSP TP', 'PSP Processor Name',
                            'PSP Last 4 Digits', 'comment']
            # Extract match keys for sorting other_df subsets
            other_df[['match_type', 'match_value']] = pd.DataFrame(
                other_df['comment'].apply(self.extract_match_key).tolist(), index=other_df.index
            )
            # Filter and sort CRM table rows
            crm_mask = other_df['CRM Email'].notna()
            crm_df = other_df[crm_mask].copy()
            if not crm_df.empty:
                crm_df['secondary_sort'] = crm_df.index
                crm_sorted = crm_df.sort_values(['match_type', 'match_value', 'secondary_sort'])
                self.crm_display = crm_sorted.drop(['match_type', 'match_value', 'secondary_sort'], axis=1)
                self.crm_display_local_indices = list(crm_sorted.index)
                self.crm_orig_indices = [self.orig_indices[l] for l in self.crm_display_local_indices]
            else:
                self.crm_display = pd.DataFrame()
                self.crm_orig_indices = []
            # Filter and sort Proc table rows
            proc_mask = other_df['PSP Email'].notna()
            proc_df = other_df[proc_mask].copy()
            if not proc_df.empty:
                proc_df['secondary_sort'] = proc_df.index
                proc_sorted = proc_df.sort_values(['match_type', 'match_value', 'secondary_sort'])
                self.proc_display = proc_sorted.drop(['match_type', 'match_value', 'secondary_sort'], axis=1)
                self.proc_display_local_indices = list(proc_sorted.index)
                self.proc_orig_indices = [self.orig_indices[l] for l in self.proc_display_local_indices]
            else:
                self.proc_display = pd.DataFrame()
                self.proc_orig_indices = []
            print("CRM display shape:", self.crm_display.shape)
            print("Proc display shape:", self.proc_display.shape)
            print("Sample CRM match keys (first 3):", list(zip(self.crm_orig_indices[:3], self.crm_display['comment'][:3])))
            print("Sample Proc match keys (first 3):", list(zip(self.proc_orig_indices[:3], self.proc_display['comment'][:3])))
            # Add differ table if not empty
            if not differ_df.empty:
                if len(differ_df) == 1:
                    differ_label_text = 'Warnings - Cross Processor Withdrawal Detected'
                else:
                    differ_label_text = 'Warnings - Cross Processors Withdrawals Detected'
                self.differ_label = QLabel(differ_label_text)
                self.differ_label.setFixedHeight(30)
                self.differ_sub_layout = QVBoxLayout()
                self.differ_sub_layout.setSpacing(0)
                self.differ_sub_layout.addWidget(self.differ_label)
                self.differ_table = QTableWidget()
                self.differ_table.setSelectionMode(QTableWidget.NoSelection)
                self.differ_table.setEditTriggers(QTableWidget.NoEditTriggers)
                visible_columns = [''] + display_columns # Empty for button column
                self.differ_table.setColumnCount(len(visible_columns) + 1) # +1 for hidden orig_index
                self.differ_table.setHorizontalHeaderLabels(['orig_index'] + visible_columns)
                self.differ_table.horizontalHeader().setVisible(True)
                self.differ_table.verticalHeader().setVisible(False)
                self.differ_table.setRowCount(len(differ_df))
                self.accepted_rows[self.differ_table] = set()
                center_cols = ['CRM Email', 'PSP Email', 'CRM Amount', 'PSP Amount', 'CRM TP', 'PSP TP', 'CRM Last 4 Digits', 'PSP Last 4 Digits', 'CRM Currency', 'PSP Currency', 'CRM Processor Name', 'PSP Processor Name']
                for i, orig_idx in enumerate(self.differ_orig_indices):
                    self.differ_table.setItem(i, 0, QTableWidgetItem(str(orig_idx)))
                    # Button column
                    button = QPushButton('✅')
                    button.setObjectName('row_button')
                    button.setStyleSheet("color: green; background: transparent; border: none;")
                    button.clicked.connect(self.make_toggle_accept(self.differ_table))
                    container = QWidget()
                    container_layout = QHBoxLayout()
                    container_layout.addStretch(1)
                    container_layout.addWidget(button)
                    container_layout.addStretch(1)
                    container_layout.setAlignment(Qt.AlignCenter)
                    container_layout.setContentsMargins(0, 0, 0, 0)
                    container.setLayout(container_layout)
                    container.setStyleSheet("background-color: #ffffff;")
                    self.differ_table.setCellWidget(i, 1, container)
                    # Data columns
                    for j, col in enumerate(display_columns):
                        val = differ_df.iloc[i][col]
                        item_text = self.format_cell_value(val, col)
                        item = QTableWidgetItem(item_text)
                        item.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
                        if col in center_cols:
                            item.setTextAlignment(Qt.AlignVCenter | Qt.AlignHCenter)
                        self.differ_table.setItem(i, j + 2, item)
                self.differ_table.hideColumn(0)
                self.differ_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
                self.differ_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Fixed)
                self.differ_table.setWordWrap(True)
                self.differ_table.resizeRowsToContents()
                self.differ_table.setColumnWidth(1, 40) # Narrow button column with space
                self.differ_sub_layout.addWidget(self.differ_table)
                self.layout.addLayout(self.differ_sub_layout)
            # Add other CRM and Proc tables if not empty
            if not other_df.empty:
                # CRM table
                if not self.crm_display.empty:
                    self.crm_label = QLabel('Warnings - CRM Side')
                    self.crm_label.setFixedHeight(30)
                    self.crm_sub_layout = QVBoxLayout()
                    self.crm_sub_layout.setSpacing(0)
                    self.crm_sub_layout.addWidget(self.crm_label)
                    self.crm_table = QTableWidget()
                    self.crm_table.setSelectionMode(QTableWidget.NoSelection)
                    self.crm_table.setEditTriggers(QTableWidget.NoEditTriggers)
                    visible_crm_columns = [''] + crm_columns
                    self.crm_table.setColumnCount(len(visible_crm_columns) + 1)
                    self.crm_table.setHorizontalHeaderLabels(['orig_index'] + visible_crm_columns)
                    self.crm_table.horizontalHeader().setVisible(True)
                    self.crm_table.verticalHeader().setVisible(False)
                    self.crm_table.setRowCount(len(self.crm_display))
                    self.accepted_rows[self.crm_table] = set()
                    center_cols = ['CRM Email', 'CRM Amount', 'CRM TP', 'CRM Last 4 Digits', 'CRM Currency', 'CRM Processor Name']
                    for i in range(len(self.crm_display)):
                        row_idx = self.crm_orig_indices[i]
                        self.crm_table.setItem(i, 0, QTableWidgetItem(str(row_idx)))
                        # Button column
                        button = QPushButton('✅')
                        button.setObjectName('row_button')
                        button.setStyleSheet("color: green; background: transparent; border: none;")
                        button.clicked.connect(self.make_toggle_accept(self.crm_table))
                        container = QWidget()
                        container_layout = QHBoxLayout()
                        container_layout.addStretch(1)
                        container_layout.addWidget(button)
                        container_layout.addStretch(1)
                        container_layout.setAlignment(Qt.AlignCenter)
                        container_layout.setContentsMargins(0, 0, 0, 0)
                        container.setLayout(container_layout)
                        container.setStyleSheet("background-color: #ffffff;")
                        self.crm_table.setCellWidget(i, 1, container)
                        # Data columns
                        for j, col in enumerate(crm_columns):
                            val = self.crm_display.iloc[i][col]
                            item_text = self.format_cell_value(val, col)
                            item = QTableWidgetItem(item_text)
                            item.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
                            if col in center_cols:
                                item.setTextAlignment(Qt.AlignVCenter | Qt.AlignHCenter)
                            self.crm_table.setItem(i, j + 2, item)
                    self.crm_table.hideColumn(0)
                    self.crm_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
                    self.crm_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Fixed)
                    self.crm_table.setWordWrap(True)
                    self.crm_table.resizeRowsToContents()
                    self.crm_table.setColumnWidth(1, 40) # Narrow button column with space
                    self.crm_sub_layout.addWidget(self.crm_table)
                    self.layout.addLayout(self.crm_sub_layout)
                # PSP table
                if not self.proc_display.empty:
                    self.proc_label = QLabel('Warnings - Processors Side')
                    self.proc_label.setFixedHeight(30)
                    self.proc_sub_layout = QVBoxLayout()
                    self.proc_sub_layout.setSpacing(0)
                    self.proc_sub_layout.addWidget(self.proc_label)
                    self.proc_table = QTableWidget()
                    self.proc_table.setSelectionMode(QTableWidget.NoSelection)
                    self.proc_table.setEditTriggers(QTableWidget.NoEditTriggers)
                    visible_proc_columns = [''] + proc_columns
                    self.proc_table.setColumnCount(len(visible_proc_columns) + 1)
                    self.proc_table.setHorizontalHeaderLabels(['orig_index'] + visible_proc_columns)
                    self.proc_table.horizontalHeader().setVisible(True)
                    self.proc_table.verticalHeader().setVisible(False)
                    self.proc_table.setRowCount(len(self.proc_display))
                    self.accepted_rows[self.proc_table] = set()
                    center_cols = ['PSP Email', 'PSP Amount', 'PSP TP', 'PSP Last 4 Digits', 'PSP Currency', 'PSP Processor Name']
                    for i in range(len(self.proc_display)):
                        row_idx = self.proc_orig_indices[i]
                        self.proc_table.setItem(i, 0, QTableWidgetItem(str(row_idx)))
                        # Button column
                        button = QPushButton('✅')
                        button.setObjectName('row_button')
                        button.setStyleSheet("color: green; background: transparent; border: none;")
                        button.clicked.connect(self.make_toggle_accept(self.proc_table))
                        container = QWidget()
                        container_layout = QHBoxLayout()
                        container_layout.addStretch(1)
                        container_layout.addWidget(button)
                        container_layout.addStretch(1)
                        container_layout.setAlignment(Qt.AlignCenter)
                        container_layout.setContentsMargins(0, 0, 0, 0)
                        container.setLayout(container_layout)
                        container.setStyleSheet("background-color: #ffffff;")
                        self.proc_table.setCellWidget(i, 1, container)
                        # Data columns
                        for j, col in enumerate(proc_columns):
                            val = self.proc_display.iloc[i][col]
                            item_text = self.format_cell_value(val, col)
                            item = QTableWidgetItem(item_text)
                            item.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
                            if col in center_cols:
                                item.setTextAlignment(Qt.AlignVCenter | Qt.AlignHCenter)
                            self.proc_table.setItem(i, j + 2, item)
                    self.proc_table.hideColumn(0)
                    self.proc_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
                    self.proc_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Fixed)
                    self.proc_table.setWordWrap(True)
                    self.proc_table.resizeRowsToContents()
                    self.proc_table.setColumnWidth(1, 40) # Narrow button column with space
                    self.proc_sub_layout.addWidget(self.proc_table)
                    self.layout.addLayout(self.proc_sub_layout)
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to load data: {e}")
    def make_toggle_accept(self, table):
        def handler():
            button = self.sender()
            row = self.get_row_from_button(table, button)
            if row != -1:
                self.toggle_accept(table, row)
        return handler
    def get_row_from_button(self, table, button):
        for r in range(table.rowCount()):
            if table.cellWidget(r, 1).layout().itemAt(1).widget() == button:
                return r
        return -1
    def adjust_tables_and_window(self):
        tables = []
        labels = []
        sub_layouts = []
        if hasattr(self, 'differ_table') and self.differ_table:
            tables.append(self.differ_table)
            labels.append(self.differ_label if hasattr(self, 'differ_label') else None)
            sub_layouts.append(self.differ_sub_layout if hasattr(self, 'differ_sub_layout') else None)
        if hasattr(self, 'crm_table') and self.crm_table:
            tables.append(self.crm_table)
            labels.append(self.crm_label if hasattr(self, 'crm_label') else None)
            sub_layouts.append(self.crm_sub_layout if hasattr(self, 'crm_sub_layout') else None)
        if hasattr(self, 'proc_table') and self.proc_table:
            tables.append(self.proc_table)
            labels.append(self.proc_label if hasattr(self, 'proc_label') else None)
            sub_layouts.append(self.proc_sub_layout if hasattr(self, 'proc_sub_layout') else None)
        # First, set base heights without extra
        for idx, table in enumerate(tables):
            label = labels[idx]
            sub_layout = sub_layouts[idx]
            if table.rowCount() == 0:
                table.hide()
                if label:
                    label.hide()
            else:
                table.show()
                if label:
                    label.show()
            table.resizeRowsToContents()
            height = table.horizontalHeader().height()
            for i in range(table.rowCount()):
                height += table.rowHeight(i)
            height += 20 # Increased buffer for full row visibility
            table.setFixedHeight(height)
        self.adjustSize()
        base_height = self.height()
        frame_overhead = self.frameGeometry().height() - self.height()
        taskbar_and_program_bar_size = 30
        max_content_height = self.available_height - frame_overhead - taskbar_and_program_bar_size
        total_rows = sum(table.rowCount() for table in tables)
        desired_extra_per_row = 10
        desired_extra_window = 50
        projected_height = base_height + (total_rows * desired_extra_per_row) + desired_extra_window
        if projected_height > max_content_height:
            if total_rows > 0:
                max_extra_per_row = max(0, (max_content_height - base_height - desired_extra_window) // total_rows)
                extra_per_row = min(desired_extra_per_row, max_extra_per_row)
                extra_window = max_content_height - base_height - (total_rows * extra_per_row)
                if extra_window < 0:
                    extra_window = 0
            else:
                extra_per_row = 0
                extra_window = min(desired_extra_window, max_content_height - base_height)
        else:
            extra_per_row = desired_extra_per_row
            extra_window = desired_extra_window
        # Apply extra per row
        for table in tables:
            for i in range(table.rowCount()):
                table.setRowHeight(i, table.rowHeight(i) + extra_per_row)
        # Now adjust table heights with potential scrolling
        max_visible_rows = 4
        for table in tables:
            row_count = table.rowCount()
            if row_count > max_visible_rows:
                table.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
                height = table.horizontalHeader().height()
                for i in range(min(row_count, max_visible_rows)):
                    height += table.rowHeight(i)
                height += 20 # Increased buffer for full row visibility
                table.setFixedHeight(height)
            else:
                table.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
                height = table.horizontalHeader().height()
                for i in range(row_count):
                    height += table.rowHeight(i)
                height += 20 # Increased buffer for full row visibility
                table.setFixedHeight(height)
        self.adjustSize()
        self.setFixedWidth(self.screen_width)
        self.adjustSize()
        final_height = min(self.height() + extra_window, max_content_height)
        self.setFixedHeight(final_height)
        self.setGeometry(-5, taskbar_and_program_bar_size, self.screen_width, final_height)
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
        self.update_remove_button_state()
    def update_remove_button_state(self):
        total_selected = sum(len(self.accepted_rows[t]) for t in self.accepted_rows)
        self.remove_btn.setEnabled(total_selected > 0)
    def remove_selected(self):
        for table in self.accepted_rows:
            rows = sorted(list(self.accepted_rows[table]), reverse=True)
            for r in rows:
                idx_item = table.item(r, 0)
                if idx_item:
                    orig_idx = int(idx_item.text())
                    self.remove_rows_by_index(orig_idx)
            self.accepted_rows[table].clear()
        self.adjust_tables_and_window()
        self.update_remove_button_state()
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
        tables = [getattr(self, attr, None) for attr in ['differ_table', 'crm_table', 'proc_table'] if
                  getattr(self, attr, None)]
        remaining_indices = set()
        for t in tables:
            for r in range(t.rowCount()):
                idx_item = t.item(r, 0)
                if idx_item:
                    remaining_indices.add(int(idx_item.text()))
        removed_indices = set(self.orig_indices) - remaining_indices
        print(f"Removed (accepted) indices: {len(removed_indices)}")
        print(f"Remaining (unselected) indices: {len(remaining_indices)}")
        # Load original matching_df
        original_matching_path = LISTS_DIR / self.date_str / "withdrawals_matching.xlsx"
        matching_df = pd.read_excel(original_matching_path)
        # Update accepted rows: set as matched, warning=False
        for idx in removed_indices:
            if idx in matching_df.index:
                matching_df.at[idx, 'warning'] = False
                matching_df.at[idx, 'match_status'] = 1
                matching_df.at[idx, 'payment_status'] = 1
                matching_df.at[idx, 'comment'] = "Warning accepted as match"
        # For unselected: Drop them from matching_df and create split rows
        unselected_split_rows = []
        reverse_rename = {
            'CRM Email': 'crm_email',
            'CRM Amount': 'crm_amount',
            'CRM Currency': 'crm_currency',
            'CRM TP': 'crm_tp',
            'CRM Processor Name': 'crm_processor_name',
            'CRM Last 4 Digits': 'crm_last4',
            'PSP Email': 'proc_email',
            'PSP Amount': 'proc_amount',
            'PSP Currency': 'proc_currency',
            'PSP TP': 'proc_tp',
            'PSP Processor Name': 'proc_processor_name',
            'PSP Last 4 Digits': 'proc_last4',
        }
        for idx in remaining_indices:
            local_idx = self.orig_to_local.get(idx, None)
            if local_idx is None:
                continue
            row = self.warnings_df.loc[local_idx].rename(reverse_rename)
            has_crm = pd.notna(row.get('crm_email', np.nan))
            has_proc = pd.notna(row.get('proc_email', np.nan))
            print(f"Unselected row {idx}: has_crm={has_crm}, has_proc={has_proc}")
            orig_comment = self.warnings_df.loc[local_idx]['comment']
            # Clean comment for splits (no prefix, will add suffix below)
            clean_comment = process_unmatched_comment(orig_comment)  # In case orig has legacy prefix
            # Drop the original unselected row
            if idx in matching_df.index:
                matching_df = matching_df.drop(idx)
            # Create CRM split if applicable
            if has_crm:
                crm_row_dict = row.to_dict()
                proc_cols = [c for c in matching_df.columns if c.startswith('proc_')]
                for col in proc_cols:
                    crm_row_dict[col] = np.nan
                crm_row_dict['match_status'] = 0
                crm_row_dict['payment_status'] = 0
                crm_row_dict['warning'] = False
                crm_row_dict['comment'] = f"{clean_comment} [unmatched_warning]"
                crm_row_dict['crm_type'] = 'Withdrawal'
                # No re-format: already done in run_initial_phase
                unselected_split_rows.append(crm_row_dict)
            # Create Proc split if applicable
            if has_proc:
                proc_row_dict = row.to_dict()
                crm_cols = [c for c in matching_df.columns if c.startswith('crm_') and c != 'crm_type']
                for col in crm_cols:
                    proc_row_dict[col] = np.nan
                proc_row_dict['match_status'] = 0
                proc_row_dict['payment_status'] = 0
                proc_row_dict['warning'] = False
                proc_row_dict['comment'] = f"{clean_comment} [unmatched_warning]"
                proc_row_dict['crm_type'] = np.nan
                # No re-format: already done in run_initial_phase
                unselected_split_rows.append(proc_row_dict)
        # Append the split rows to matching_df
        if unselected_split_rows:
            split_df = pd.DataFrame(unselected_split_rows)
            matching_df = pd.concat([matching_df, split_df], ignore_index=True)
        print(f"Updated matching_df shape after splits: {matching_df.shape}")
        print(f"Unselected CRM splits: {sum(1 for r in unselected_split_rows if pd.notna(r.get('crm_email')))}")
        print(f"Unselected Proc splits: {sum(1 for r in unselected_split_rows if pd.notna(r.get('proc_email')))}")
        # Save updated matching_df
        output_dir = OUTPUT_DIR / self.date_str
        updated_matching_path = output_dir / "withdrawals_matching_updated.xlsx"
        matching_df.to_excel(updated_matching_path, index=True)
        print(f"Updated matching saved to {updated_matching_path}")
        print("Processing complete. Opening export window.")
        # Reorder: Create/show fourth BEFORE close (keeps app alive during init)
        self.fourth_window = FourthWindow(self.date_str)
        self.fourth_window.show()
        print("Debug: Fourth window shown")
        self.close()  # Now safe—fourth is active