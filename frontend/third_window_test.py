from PyQt5.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QTextEdit, QLabel, QTableWidget, QTableWidgetItem, QMessageBox, QDesktopWidget, QApplication, QHeaderView, QScrollArea
from PyQt5.QtCore import Qt, QItemSelectionModel, QItemSelection, QThread, pyqtSignal
import pandas as pd
import numpy as np
import re
from src.config import LISTS_DIR, OUTPUT_DIR
import shutil
from src.output import clean_value, format_date, process_comment, save_excel, generate_unmatched_crm_withdrawals, generate_unmatched_proc_withdrawals, generate_warning_withdrawals,process_unmatched_comment
from fourth_window import FourthWindow # Import to open next window
class ThirdWindow(QWidget):
    def __init__(self, date_str):
        super().__init__()
        self.date_str = date_str
        self.screen_width = QApplication.desktop().screenGeometry().width()
        self.available_height = QApplication.desktop().availableGeometry().height()
        self.initUI()
        self.load_thread = LoadWarningsThread(self.date_str)
        self.load_thread.dataLoaded.connect(self.on_data_loaded)
        self.load_thread.errorOccurred.connect(self.on_load_error)
        self.load_thread.start()
    def initUI(self):
        self.setWindowTitle('Review Warning Withdrawals')
        layout = QVBoxLayout()
        layout.setSpacing(10)
        layout.setContentsMargins(10, 10, 10, 10)
        # Scroll area for tables
        self.tables_container = QWidget()
        self.tables_layout = QVBoxLayout()
        self.tables_container.setLayout(self.tables_layout)
        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(False)
        self.scroll_area.setWidget(self.tables_container)
        self.scroll_area.setWidgetResizable(True)
        layout.addWidget(self.scroll_area)
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
        self.loading_label = QLabel("Loading warnings...")
        self.loading_label.setAlignment(Qt.AlignCenter)
        self.tables_layout.addWidget(self.loading_label)
        self.setMinimumSize(600, 200) # Ensure buttons visible even if no content
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

    def on_data_loaded(self, data_dict):
        self.loading_label.hide()
        self.tables_layout.removeWidget(self.loading_label)
        self.warnings_df = data_dict['warnings_df']
        self.orig_indices = data_dict['orig_indices']
        self.orig_to_local = data_dict['orig_to_local']
        self.original_matching_df = data_dict['original_matching_df']
        if data_dict.get('no_warnings', False):
            QMessageBox.information(self, "Info", "No warnings file found. Skipping review and proceeding to export.")
            self.on_next()  # Auto-proceed if no warnings
            return
        if self.warnings_df.empty:
            self.add_no_warnings_label()
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
        self.warnings_df['crm_date'] = self.warnings_df['crm_date'].apply(
            lambda x: format_date(x, is_proc=False))  # Explicit format for dates
        self.warnings_df['crm_amount'] = self.warnings_df['crm_amount'].apply(
            lambda x: -abs(x) if pd.notna(x) else x)
        self.warnings_df['proc_amount'] = self.warnings_df['proc_amount'].apply(
            lambda x: -abs(x) if pd.notna(x) else x)
        self.warnings_df['comment'] = self.warnings_df['comment'].apply(process_comment)
        self.warnings_df['display_comment'] = self.warnings_df['comment'].apply(self.get_display_comment)
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
        self.display_df['comment'] = self.warnings_df['display_comment']
        # Split into differ and other
        differ_mask = self.warnings_df['comment'].str.contains("Cross-processor fallback match", na=False)
        differ_local_indices = list(self.display_df[differ_mask].index)
        self.differ_orig_indices = [self.orig_indices[l] for l in differ_local_indices]
        differ_df = self.display_df.loc[differ_local_indices].copy()
        other_warnings_df = self.warnings_df[~differ_mask].copy()  # Use original for match extraction
        other_display_df = self.display_df[~differ_mask].copy()
        print("Differ local indices:", differ_local_indices)
        print("Differ orig indices:", self.differ_orig_indices)
        self.accepted_rows = {}
        # For other: Extract match keys from original comment
        other_warnings_df[['match_type', 'match_value']] = pd.DataFrame(
            other_warnings_df['comment'].apply(self.extract_match_key).tolist(), index=other_warnings_df.index
        )
        # Sort
        other_sorted = other_warnings_df.sort_values(['match_type', 'match_value'])
        # Pair and merge
        merged_rows = []
        self.other_paired_orig = []  # List of (crm_orig, psp_orig)
        for i in range(0, len(other_sorted), 2):
            if i + 1 < len(other_sorted):
                row1 = other_sorted.iloc[i]
                row2 = other_sorted.iloc[i + 1]
                # Determine which is CRM and PSP
                if pd.notna(row1['CRM Email']):
                    crm_row = row1
                    psp_row = row2
                else:
                    crm_row = row2
                    psp_row = row1
                # Merge
                merged = pd.Series()
                for col in ['CRM Email', 'CRM Amount', 'CRM Currency', 'CRM TP', 'CRM Processor Name',
                            'CRM Last 4 Digits']:
                    merged[col] = crm_row[col]
                for col in ['PSP Email', 'PSP Amount', 'PSP Currency', 'PSP TP', 'PSP Processor Name',
                            'PSP Last 4 Digits']:
                    merged[col] = psp_row[col]
                merged['comment'] = self.display_df.loc[crm_row.name]['comment']  # Use display comment
                merged_rows.append(merged)
                # Paired orig
                crm_local = crm_row.name
                psp_local = psp_row.name
                crm_orig = self.orig_indices[crm_local]
                psp_orig = self.orig_indices[psp_local]
                self.other_paired_orig.append((crm_orig, psp_orig))
        other_merged_df = pd.DataFrame(merged_rows)
        print("Other merged shape:", other_merged_df.shape)
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
            visible_columns = [''] + display_columns  # Empty for button column
            self.differ_table.setColumnCount(len(visible_columns) + 1)  # +1 for hidden orig_index
            self.differ_table.setHorizontalHeaderLabels(['orig_index'] + visible_columns)
            self.differ_table.horizontalHeader().setVisible(True)
            self.differ_table.verticalHeader().setVisible(False)
            self.differ_table.setRowCount(len(differ_df))
            self.accepted_rows[self.differ_table] = set()
            center_cols = ['CRM Email', 'PSP Email', 'CRM Amount', 'PSP Amount', 'CRM TP', 'PSP TP',
                           'CRM Last 4 Digits', 'PSP Last 4 Digits', 'CRM Currency', 'PSP Currency',
                           'CRM Processor Name', 'PSP Processor Name']
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
            self.differ_table.setColumnWidth(1, 40)  # Narrow button column with space
            self.differ_sub_layout.addWidget(self.differ_table)
            self.tables_layout.addLayout(self.differ_sub_layout)
        # Add other table if not empty
        if not other_merged_df.empty:
            if len(other_merged_df) == 1:
                other_label_text = 'Warnings - Withdrawal Detected'
            else:
                other_label_text = 'Warnings - Withdrawals Detected'
            self.other_label = QLabel(other_label_text)
            self.other_label.setFixedHeight(30)
            self.other_sub_layout = QVBoxLayout()
            self.other_sub_layout.setSpacing(0)
            self.other_sub_layout.addWidget(self.other_label)
            self.other_table = QTableWidget()
            self.other_table.setSelectionMode(QTableWidget.NoSelection)
            self.other_table.setEditTriggers(QTableWidget.NoEditTriggers)
            visible_columns = [''] + display_columns
            self.other_table.setColumnCount(len(visible_columns) + 2)  # +2 for hidden crm_orig and psp_orig
            self.other_table.setHorizontalHeaderLabels(['crm_orig', 'psp_orig'] + visible_columns)
            self.other_table.horizontalHeader().setVisible(True)
            self.other_table.verticalHeader().setVisible(False)
            self.other_table.setRowCount(len(other_merged_df))
            self.accepted_rows[self.other_table] = set()
            center_cols = ['CRM Email', 'PSP Email', 'CRM Amount', 'PSP Amount', 'CRM TP', 'PSP TP',
                           'CRM Last 4 Digits', 'PSP Last 4 Digits', 'CRM Currency', 'PSP Currency',
                           'CRM Processor Name', 'PSP Processor Name']
            for i in range(len(other_merged_df)):
                crm_orig, psp_orig = self.other_paired_orig[i]
                self.other_table.setItem(i, 0, QTableWidgetItem(str(crm_orig)))
                self.other_table.setItem(i, 1, QTableWidgetItem(str(psp_orig)))
                # Button column
                button = QPushButton('✅')
                button.setObjectName('row_button')
                button.setStyleSheet("color: green; background: transparent; border: none;")
                button.clicked.connect(self.make_toggle_accept(self.other_table))
                container = QWidget()
                container_layout = QHBoxLayout()
                container_layout.addStretch(1)
                container_layout.addWidget(button)
                container_layout.addStretch(1)
                container_layout.setAlignment(Qt.AlignCenter)
                container_layout.setContentsMargins(0, 0, 0, 0)
                container.setLayout(container_layout)
                container.setStyleSheet("background-color: #ffffff;")
                self.other_table.setCellWidget(i, 2, container)
                # Data columns
                for j, col in enumerate(display_columns):
                    val = other_merged_df.iloc[i][col]
                    item_text = self.format_cell_value(val, col)
                    item = QTableWidgetItem(item_text)
                    item.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
                    if col in center_cols:
                        item.setTextAlignment(Qt.AlignVCenter | Qt.AlignHCenter)
                    self.other_table.setItem(i, j + 3, item)
            self.other_table.hideColumn(0)
            self.other_table.hideColumn(1)
            self.other_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
            self.other_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.Fixed)
            self.other_table.setWordWrap(True)
            self.other_table.resizeRowsToContents()
            self.other_table.setColumnWidth(2, 40)  # Narrow button column with space
            self.other_sub_layout.addWidget(self.other_table)
            self.tables_layout.addLayout(self.other_sub_layout)
        self.adjust_tables_and_window()

    def on_load_error(self, error_msg):
        self.layout.removeWidget(self.loading_label)
        self.loading_label.deleteLater()
        QMessageBox.critical(self, "Error", f"Failed to load warnings: {error_msg}")
        self.close()

    def add_no_warnings_label(self):
        label = QLabel("No warnings to review.")
        label.setAlignment(Qt.AlignCenter)
        self.layout.insertWidget(self.layout.count() - 1, label)  # Add before buttons
        self.adjust_tables_and_window()
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

    def get_simplified_comment(self, comment):
        comment_str = str(comment)
        suffix = " and was considered a match after review"
        if "Matched similar email" in comment_str:
            return "Emails matched" + suffix
        elif "Matched the same last 4 digits" in comment_str:
            return "Last 4 Digits matched" + suffix
        elif "Cross-processor" in comment_str:
            return "Different processors" + suffix
        else:
            return "Warning accepted as match" + suffix

    def get_display_comment(self, comment):
        comment_str = str(comment)
        if "Matched similar email" in comment_str:
            return "Similar emails were detected"
        elif "Matched the same last4" in comment_str:
            return "Same last 4 digits detected"
        elif "Cross-processor" in comment_str:
            return "Matched row but on different processors"
        else:
            return "Warning accepted as match"
    def make_toggle_accept(self, table):
        def handler():
            button = self.sender()
            row = self.get_row_from_button(table, button)
            if row != -1:
                self.toggle_accept(table, row)
        return handler
    def get_button_col(self, table):
        return 2 if hasattr(self, 'other_table') and table == self.other_table else 1
    def get_row_from_button(self, table, button):
        button_col = self.get_button_col(table)
        for r in range(table.rowCount()):
            cell_widget = table.cellWidget(r, button_col)
            if cell_widget and cell_widget.layout().itemAt(1).widget() == button:
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
        if hasattr(self, 'other_table') and self.other_table:
            tables.append(self.other_table)
            labels.append(self.other_label if hasattr(self, 'other_label') else None)
            sub_layouts.append(self.other_sub_layout if hasattr(self, 'other_sub_layout') else None)
        # Hide empty tables/labels
        for idx, table in enumerate(tables):
            label = labels[idx]
            if table.rowCount() == 0:
                table.hide()
                if label:
                    label.hide()
            else:
                table.show()
                if label:
                    label.show()
        # Step 1: Resize rows to base contents (no extra yet)
        for table in tables:
            table.resizeRowsToContents()
        # Step 2: Initial cap to min(4 rows) without extra, set scroll policy
        max_visible_rows = 4
        for table in tables:
            row_count = table.rowCount()
            if row_count > max_visible_rows:
                height = table.horizontalHeader().height()
                for i in range(max_visible_rows):
                    height += table.rowHeight(i)
                height += 20  # Buffer
                table.setFixedHeight(height)
                table.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
            else:
                height = table.horizontalHeader().height()
                for i in range(row_count):
                    height += table.rowHeight(i)
                height += 20  # Buffer
                table.setFixedHeight(height)
                table.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        # Step 3: Adjust size and get base height with capped tables (no extra)
        self.adjustSize()
        base_height = self.height()
        # Step 4: Calculate total visible rows for projection/extras
        total_visible = sum(min(table.rowCount(), max_visible_rows) for table in tables)
        desired_extra_per_row = 10
        frame_overhead = self.frameGeometry().height() - self.height()
        taskbar_and_program_bar_size = 30
        desired_extra_window = 800
        max_content_height = self.available_height - frame_overhead - taskbar_and_program_bar_size
        projected = base_height + (total_visible * desired_extra_per_row) + desired_extra_window
        if projected > max_content_height:
            if total_visible > 0:
                max_extra_per_row = max(0, (max_content_height - base_height - desired_extra_window) // total_visible)
                extra_per_row = min(desired_extra_per_row, max_extra_per_row)
                extra_window = max_content_height - base_height - (total_visible * extra_per_row)
                if extra_window < 0:
                    extra_window = 0
            else:
                extra_per_row = 0
                extra_window = min(desired_extra_window, max_content_height - base_height)
        else:
            extra_per_row = desired_extra_per_row
            extra_window = desired_extra_window
        # Step 5: Apply extra per row to ALL rows (for consistency when scrolling)
        for table in tables:
            for i in range(table.rowCount()):
                table.setRowHeight(i, table.rowHeight(i) + extra_per_row)
        # Step 6: Re-cap heights with extras included
        for table in tables:
            row_count = table.rowCount()
            if row_count > max_visible_rows:
                height = table.horizontalHeader().height()
                for i in range(max_visible_rows):
                    height += table.rowHeight(i)
                height += 20  # Buffer
                table.setFixedHeight(height)
            else:
                height = table.horizontalHeader().height()
                for i in range(row_count):
                    height += table.rowHeight(i)
                height += 20  # Buffer
                table.setFixedHeight(height)
        # Step 7: Final adjust and set window size/geometry
        self.adjustSize()
        self.setFixedWidth(self.screen_width)
        self.adjustSize()
        final_height = min(self.height() + extra_window, max_content_height)
        self.setFixedHeight(final_height)
        self.setGeometry(0, taskbar_and_program_bar_size, self.screen_width, final_height)  # Change x to 0
    def toggle_accept(self, table, row):
        button_col = self.get_button_col(table)
        if row in self.accepted_rows[table]:
            self.accepted_rows[table].remove(row)
            button = table.cellWidget(row, button_col).layout().itemAt(1).widget()
            button.setText('✅')
            button.setStyleSheet("color: green; background: transparent; border: none;")
        else:
            self.accepted_rows[table].add(row)
            button = table.cellWidget(row, button_col).layout().itemAt(1).widget()
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
        for t in [getattr(self, attr, None) for attr in ['differ_table', 'other_table']]:
            if t:
                rows_to_remove = []
                for r in range(t.rowCount()):
                    idx_item = t.item(r, 0)
                    if idx_item and int(idx_item.text()) == orig_idx:
                        rows_to_remove.append(r)
                for r in sorted(rows_to_remove, reverse=True):
                    t.removeRow(r)

    def on_next(self):
        tables = [getattr(self, attr, None) for attr in ['differ_table', 'other_table'] if
                  getattr(self, attr, None)]
        remaining_indices = set()
        for t in tables:
            if t is self.other_table:
                for r in range(t.rowCount()):
                    crm_orig = int(t.item(r, 0).text())
                    psp_orig = int(t.item(r, 1).text())
                    remaining_indices.add(crm_orig)
                    remaining_indices.add(psp_orig)
            else:
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
        # Update accepted rows: for differ/cross, set directly
        for idx in removed_indices:
            if idx in matching_df.index:
                orig_comment = self.original_matching_df.at[
                    idx, 'comment'] if idx in self.original_matching_df.index else ''
                processed = process_comment(orig_comment)
                simplified = self.get_simplified_comment(processed)
                matching_df.at[idx, 'comment'] = simplified
                matching_df.at[idx, 'warning'] = False
                matching_df.at[idx, 'match_status'] = 1
                matching_df.at[idx, 'payment_status'] = 1
        # For accepted other pairs: merge PSP into CRM
        for display_r, (crm_orig, psp_orig) in enumerate(self.other_paired_orig):
            if crm_orig not in remaining_indices and psp_orig not in remaining_indices:  # Accepted
                if crm_orig in matching_df.index and psp_orig in matching_df.index:
                    psp_row = matching_df.loc[psp_orig]
                    for col in ['proc_email', 'proc_amount', 'proc_currency', 'proc_tp', 'proc_processor_name',
                                'proc_last4', 'proc_date', 'proc_firstname', 'proc_lastname',
                                'proc_amount_crm_currency']:
                        if col in matching_df.columns:
                            matching_df.at[crm_orig, col] = psp_row[col]
                    orig_comment = self.original_matching_df.at[
                        crm_orig, 'comment'] if crm_orig in self.original_matching_df.index else ''
                    processed = process_comment(orig_comment)
                    simplified = self.get_simplified_comment(processed)
                    matching_df.at[crm_orig, 'comment'] = simplified
                    matching_df.at[crm_orig, 'warning'] = False
                    matching_df.at[crm_orig, 'match_status'] = 1
                    matching_df.at[crm_orig, 'payment_status'] = 1
                    matching_df = matching_df.drop(psp_orig)
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
            orig_comment = self.original_matching_df.at[
                idx, 'comment'] if idx in self.original_matching_df.index else row.get('comment', '')
            # Prefix for unmatched
            prefixed_comment = f"Unmatched due to warning: {orig_comment}"
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
                crm_row_dict['comment'] = prefixed_comment
                crm_row_dict['crm_type'] = 'Withdrawal'
                unselected_split_rows.append(crm_row_dict)
            # Create Proc split if applicable
            if has_proc:
                proc_row_dict = row.to_dict()
                crm_cols = [c for c in matching_df.columns if c.startswith('crm_') and c != 'crm_type']
                for col in crm_cols:
                    proc_row_dict[col] = np.nan
                proc_row_dict['payment_method'] = np.nan
                proc_row_dict['match_status'] = 0
                proc_row_dict['payment_status'] = 0
                proc_row_dict['warning'] = False
                proc_row_dict['comment'] = prefixed_comment
                proc_row_dict['crm_type'] = np.nan
                unselected_split_rows.append(proc_row_dict)
        # Append the split rows to matching_df
        if unselected_split_rows:
            split_df = pd.DataFrame(unselected_split_rows)
            matching_df = pd.concat([matching_df, split_df], ignore_index=False)
        print(f"Updated matching_df shape after splits: {matching_df.shape}")
        print(f"Unselected CRM splits: {sum(1 for r in unselected_split_rows if pd.notna(r.get('crm_email')))}")
        print(f"Unselected Proc splits: {sum(1 for r in unselected_split_rows if pd.notna(r.get('proc_email')))}")
        # Save updated matching_df
        output_dir = OUTPUT_DIR / self.date_str
        updated_matching_path = output_dir / "withdrawals_matching_updated.xlsx"
        matching_df.to_excel(updated_matching_path, index=False)
        print(f"Updated matching saved to {updated_matching_path}")
        print("Processing complete. Opening export window.")
        # Reorder: Create/show fourth BEFORE close (keeps app alive during init)
        self.fourth_window = FourthWindow(self.date_str)
        self.fourth_window.show()
        print("Debug: Fourth window shown")
        self.close()  # Now safe—fourth is active
class LoadWarningsThread(QThread):
    dataLoaded = pyqtSignal(dict) # Emit dict with processed data
    errorOccurred = pyqtSignal(str) # For error handling
    def __init__(self, date_str):
        super().__init__()
        self.date_str = date_str
    def run(self):
        try:
            output_dir = OUTPUT_DIR / self.date_str
            output_dir.mkdir(parents=True, exist_ok=True)
            warnings_withdrawals_path = output_dir / "warnings_withdrawals.xlsx"
            if warnings_withdrawals_path.exists():
                warnings_withdrawals_path.unlink() # Safer than rmtree for single file
            generate_warning_withdrawals(self.date_str)
            if not warnings_withdrawals_path.exists():
                data_dict = {
                    'warnings_df': pd.DataFrame(columns=['crm_email', 'crm_amount', 'crm_currency', 'crm_tp', 'crm_processor_name', 'crm_last4', 'proc_email', 'proc_amount', 'proc_currency', 'proc_tp', 'proc_processor_name', 'proc_last4', 'comment', 'crm_date', 'proc_date']),
                    'orig_indices': np.array([]),
                    'orig_to_local': {},
                    'no_warnings': True
                }
                self.dataLoaded.emit(data_dict)
                return
            warnings_df = pd.read_excel(warnings_withdrawals_path)
            original_path = LISTS_DIR / self.date_str / "withdrawals_matching.xlsx"
            original_matching_df = pd.read_excel(original_path) if original_path.exists() else pd.DataFrame()
            if 'orig_index' in warnings_df.columns:
                warnings_df['orig_index'] = pd.to_numeric(warnings_df['orig_index'], errors='coerce').dropna().astype(int)
                orig_indices = warnings_df['orig_index'].values
                warnings_df = warnings_df.drop('orig_index', axis=1)
                orig_to_local = {orig_indices[i]: i for i in range(len(orig_indices))}
            else:
                orig_indices = np.arange(len(warnings_df))
                orig_to_local = {i: i for i in range(len(orig_indices))}
            data_dict = {
                'warnings_df': warnings_df,
                'orig_indices': orig_indices,
                'orig_to_local': orig_to_local,
                'no_warnings': False,
                'original_matching_df': original_matching_df
            }
            self.dataLoaded.emit(data_dict)
        except Exception as e:
            self.errorOccurred.emit(str(e))