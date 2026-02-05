"""
Copyright 2026 Saar Yanin
Licensed under the Apache License 2.0 with Commons Clause
See LICENSE for details.

Script: third_window.py
Description: This script implements a PyQt5-based GUI window for reviewing and accepting/rejecting warning withdrawals from the reconciliation process for a specific regulation (UK or ROW). It loads warnings and original matching data in a background thread, displays categorized tables (cross-processor and other warnings like similar emails or same last4), allows users to toggle acceptance via buttons, updates the matching DataFrame by accepting matches (updating comments, status, and handling under/overpaid with currency conversions) or splitting unmatched into separate CRM/PSP rows with prefixed comments, saves the updated matching file, and transitions to the next window (FourthWindow for export or another ThirdWindow for the other regulation if warnings exist).

Key Features:
- Background loading: Uses QThread (LoadWarningsThread) to load warnings_withdrawals.xlsx and withdrawals_matching.xlsx without freezing UI, emitting signals for data loaded or errors; cleans data (amounts, dates, comments, strings) and handles missing files with no_warnings flag.
- Data preparation: Cleans warnings_df (applies clean_value, format_date, process_comment, negative amounts); renames columns for display; splits into differ (cross-processor) and other warnings; merges paired other warnings by extracting match keys (last4/email) via regex, sorting, and combining CRM/PSP data.
- Table display: Creates separate QTableWidgets for differ and other warnings with toggle buttons (✅/X), hidden orig_index columns (and crm/psp_orig for other), centered/formatted cells (e.g., amounts to 1 decimal, integers for TP/last4); populates with display comments (simplified for UI).
- UI adjustments: Dynamically hides empty tables/labels, resizes rows/columns to contents with min/max widths per column type (e.g., 250-400 for comments, 150-250 for emails), caps visible rows (3 for differ, 5 for other) with scrollbars, calculates total height/width considering headers/margins/scrollbars/frame, caps to screen size, and centers window on showEvent with QTimer for post-show adjustments.
- Acceptance handling: Toggles row state with button style changes, enables remove button on selections; removes selected rows from tables on click, clearing accepted sets.
- Next processing: Collects remaining (unselected) orig_indices, computes removed (accepted); loads exchange rates for conversions; updates matching_df for accepted rows/pairs (sets match_status=1, warning=False, payment_status based on amount diffs/currencies, simplified comments); drops unselected rows and appends split unmatched (CRM-only or PSP-only with NaN fills, match_status=0, prefixed comments); saves to withdrawals_matching_updated.xlsx.
- Regulation flow: For 'uk', checks if 'row' has warnings and opens ThirdWindow('row') or directly FourthWindow; for 'row', opens FourthWindow.
- Edge cases: Handles no warnings by auto-proceeding to next; empty DFs with empty tables; missing orig_index by generating; currency mismatches without rates (fallback comments); no data in splits (forces both to prevent loss); frozen Windows stdout/stderr redirection to null; error messages on load failures closing window.
- Static methods: has_warnings checks if warnings file exists and non-empty for a regulation/date.

Dependencies:
- PyQt5 (QtWidgets for QWidget, layouts, buttons, labels, tables, message boxes, desktop widget, app, headers, scrolls, styles; QtCore for Qt, QThread, pyqtSignal, QTimer)
- pandas (for Excel loading, DataFrame manipulations, cleaning, merging, concatenating)
- numpy (for NaN handling)
- re (for regex in match key extraction)
- sys (for frozen app detection and stdout/stderr redirection)
- src.config (for setup_dirs_for_reg)
- src.output (for clean_value, format_date, process_comment)
- fourth_window (for FourthWindow class)
"""

from PyQt5.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QTableWidget, QTableWidgetItem, QMessageBox, QDesktopWidget, QApplication, QHeaderView, QScrollArea, QStyle
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QTimer
import pandas as pd
import numpy as np
import re
import sys
from src.config import setup_dirs_for_reg, RATES_DIR
from src.output import clean_value, format_date, process_comment
from fourth_window import FourthWindow

class DevNull:
    """Redirect stdout/stderr to null in frozen Windows app."""
    def __init__(self):
        pass
    def write(self, _):
        pass
    def flush(self):
        pass

if getattr(sys, 'frozen', False) and sys.platform.startswith('win'):
    sys.stdout = DevNull()
    sys.stderr = DevNull()

class ThirdWindow(QWidget):
    """
    ThirdWindow displays warnings for withdrawals and allows user to accept/reject matches.
    It loads data in a thread, displays tables for different warning types, and updates
    the matching file based on user selections before proceeding to the next window.
    """
    def __init__(self, date_str, regulation):
        super().__init__()
        self.date_str = date_str
        self.regulation = regulation
        self.dirs = setup_dirs_for_reg(self.regulation)
        self.screen_width = QApplication.desktop().screenGeometry().width()
        self.available_height = QApplication.desktop().availableGeometry().height()
        self.initUI()
        self.load_thread = LoadWarningsThread(self.date_str, self.regulation)
        self.load_thread.dataLoaded.connect(self.on_data_loaded)
        self.load_thread.errorOccurred.connect(self.on_load_error)
        self.load_thread.start()

    def initUI(self):
        """Initialize the user interface components."""
        self.setWindowTitle(f'Review Warning Withdrawals - {self.regulation.upper()}')
        layout = QVBoxLayout()
        layout.setSpacing(10)
        layout.setContentsMargins(10, 10, 10, 10)

        # Scroll area for tables
        self.tables_container = QWidget()
        self.tables_layout = QVBoxLayout()
        self.tables_container.setLayout(self.tables_layout)
        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setWidget(self.tables_container)
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
        self.setMinimumSize(600, 200)  # Ensure buttons visible even if no content

        # Apply stylesheet for UI elements
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

    @staticmethod
    def has_warnings(regulation, date_str):
        """Check if warnings file exists and is non-empty for the given regulation and date."""
        try:
            dirs = setup_dirs_for_reg(regulation)
            output_dir = dirs['output_dir'] / date_str
            warnings_path = output_dir / f"{regulation.upper()} warnings_withdrawals.xlsx"
            if not warnings_path.exists():
                return False
            df = pd.read_excel(warnings_path)
            return not df.empty
        except:
            return False

    def showEvent(self, event):
        """Event handler for when the window is shown; adjusts tables and window size."""
        print("showEvent called")
        super().showEvent(event)
        QTimer.singleShot(0, self.adjust_tables_and_window)

    def on_data_loaded(self, data_dict):
        """Callback when data is loaded; processes and displays warnings in tables."""
        self.loading_label.hide()
        self.tables_layout.removeWidget(self.loading_label)

        self.warnings_df = data_dict['warnings_df']
        self.orig_indices = data_dict['orig_indices']
        self.orig_to_local = data_dict['orig_to_local']
        self.original_matching_df = data_dict['original_matching_df']

        # Initialize defaults for no warnings case
        self.accepted_rows = {}
        self.other_paired_orig = []
        self.differ_orig_indices = []

        if data_dict.get('no_warnings', False) or self.warnings_df.empty:
            QTimer.singleShot(0, self.on_next)
            return

        # Clean and format warnings data
        self._clean_warnings_df()

        # Prepare display DataFrame
        self._prepare_display_df()

        # Split into differ (cross-processor) and other warnings
        differ_df, other_display_df, other_warnings_df = self._split_warning_types()

        print("Differ local indices:", list(differ_df.index))
        print("Differ orig indices:", self.differ_orig_indices)

        self.accepted_rows = {}

        # Process other warnings: merge paired rows
        other_merged_df = self._merge_other_warnings(other_warnings_df, other_display_df)

        print("Other merged shape:", other_merged_df.shape)

        # Add differ table if present
        if not differ_df.empty:
            self._add_differ_table(differ_df)

        # Add other table if present
        if not other_merged_df.empty:
            self._add_other_table(other_merged_df)

        self.adjust_tables_and_window()

    def _clean_warnings_df(self):
        """Clean specific columns in warnings_df and format dates/amounts/comments."""
        columns_to_clean = [
            'proc_date', 'proc_email', 'proc_tp', 'proc_firstname', 'proc_lastname',
            'proc_last4', 'proc_currency', 'proc_amount', 'proc_amount_crm_currency',
            'crm_amount', 'crm_tp', 'crm_last4'
        ]
        for col in columns_to_clean:
            if col in self.warnings_df.columns:
                self.warnings_df[col] = self.warnings_df[col].apply(clean_value)
        self.warnings_df['proc_date'] = self.warnings_df['proc_date'].apply(lambda x: format_date(x, is_proc=True))
        self.warnings_df['crm_date'] = self.warnings_df['crm_date'].apply(lambda x: format_date(x, is_proc=False))
        self.warnings_df['crm_amount'] = self.warnings_df['crm_amount'].apply(lambda x: -abs(x) if pd.notna(x) else x)
        self.warnings_df['proc_amount'] = self.warnings_df['proc_amount'].apply(lambda x: -abs(x) if pd.notna(x) else x)
        self.warnings_df['comment'] = self.warnings_df['comment'].apply(process_comment)
        self.warnings_df['display_comment'] = self.warnings_df['comment'].apply(self.get_display_comment)

    def _prepare_display_df(self):
        """Rename columns for display and create display_df with selected columns."""
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
        display_columns = [
            'CRM Email', 'CRM Amount', 'CRM Currency', 'CRM TP', 'CRM Processor Name', 'CRM Last 4 Digits',
            'PSP Email', 'PSP Amount', 'PSP Currency', 'PSP TP', 'PSP Processor Name',
            'PSP Last 4 Digits', 'comment'
        ]
        self.display_df = self.warnings_df[display_columns].copy()
        self.display_df['comment'] = self.warnings_df['display_comment']

    def _split_warning_types(self):
        """Split display_df into differ (cross-processor) and other warnings."""
        differ_mask = self.warnings_df['comment'].str.contains("Cross-processor fallback match", na=False)
        differ_local_indices = list(self.display_df[differ_mask].index)
        self.differ_orig_indices = [self.orig_indices[l] for l in differ_local_indices]
        differ_df = self.display_df.loc[differ_local_indices].copy()
        other_warnings_df = self.warnings_df[~differ_mask].copy()
        other_display_df = self.display_df[~differ_mask].copy()
        return differ_df, other_display_df, other_warnings_df

    def _merge_other_warnings(self, other_warnings_df, other_display_df):
        """Merge paired rows for other warnings based on match keys."""
        merged_rows = []
        self.other_paired_orig = []  # List of (crm_orig, psp_orig)
        if not other_warnings_df.empty:
            other_warnings_df[['match_type', 'match_value']] = pd.DataFrame(
                other_warnings_df['comment'].apply(self.extract_match_key).tolist(), index=other_warnings_df.index
            )
            # Sort by match_type and match_value
            other_sorted = other_warnings_df.sort_values(['match_type', 'match_value'])
            for i in range(0, len(other_sorted), 2):
                if i + 1 < len(other_sorted):
                    row1 = other_sorted.iloc[i]
                    row2 = other_sorted.iloc[i + 1]
                    # Determine CRM and PSP rows
                    if pd.notna(row1['CRM Email']):
                        crm_row = row1
                        psp_row = row2
                    else:
                        crm_row = row2
                        psp_row = row1
                    # Merge data into a single series
                    merged = pd.Series()
                    for col in ['CRM Email', 'CRM Amount', 'CRM Currency', 'CRM TP', 'CRM Processor Name',
                                'CRM Last 4 Digits']:
                        merged[col] = crm_row[col]
                    for col in ['PSP Email', 'PSP Amount', 'PSP Currency', 'PSP TP', 'PSP Processor Name',
                                'PSP Last 4 Digits']:
                        merged[col] = psp_row[col]
                    merged['comment'] = self.display_df.loc[crm_row.name]['comment']  # Use display comment
                    merged_rows.append(merged)
                    # Record original indices
                    crm_local = crm_row.name
                    psp_local = psp_row.name
                    crm_orig = self.orig_indices[crm_local]
                    psp_orig = self.orig_indices[psp_local]
                    self.other_paired_orig.append((crm_orig, psp_orig))
        return pd.DataFrame(merged_rows)

    def _add_differ_table(self, differ_df):
        """Add table for cross-processor warnings."""
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
        display_columns = self.display_df.columns.drop('comment').tolist() + ['comment']
        visible_columns = [''] + display_columns  # Empty for button column
        self.differ_table.setColumnCount(len(visible_columns) + 1)  # +1 for hidden orig_index
        self.differ_table.setHorizontalHeaderLabels(['orig_index'] + visible_columns)
        self.differ_table.horizontalHeader().setVisible(True)
        self.differ_table.verticalHeader().setVisible(False)
        self.differ_table.setRowCount(len(differ_df))
        self.accepted_rows[self.differ_table] = set()

        self._populate_table(self.differ_table, differ_df, self.differ_orig_indices, button_col=1, data_start=2)

        self.differ_table.hideColumn(0)
        self.differ_table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.differ_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Fixed)
        self.differ_table.setWordWrap(True)
        self.differ_table.setColumnWidth(1, 50)  # Narrow button column with space

        self.differ_sub_layout.addWidget(self.differ_table)
        self.tables_layout.addLayout(self.differ_sub_layout)

    def _add_other_table(self, other_merged_df):
        """Add table for other warnings (e.g., similar email, same last4)."""
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
        display_columns = self.display_df.columns.drop('comment').tolist() + ['comment']
        visible_columns = [''] + display_columns
        self.other_table.setColumnCount(len(visible_columns) + 2)  # +2 for hidden crm_orig and psp_orig
        self.other_table.setHorizontalHeaderLabels(['crm_orig', 'psp_orig'] + visible_columns)
        self.other_table.horizontalHeader().setVisible(True)
        self.other_table.verticalHeader().setVisible(False)
        self.other_table.setRowCount(len(other_merged_df))
        self.accepted_rows[self.other_table] = set()

        self._populate_table(self.other_table, other_merged_df, None, button_col=2, data_start=3, is_other=True)

        self.other_table.hideColumn(0)
        self.other_table.hideColumn(1)
        self.other_table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.other_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.Fixed)
        self.other_table.setWordWrap(True)
        self.other_table.setColumnWidth(2, 50)  # Narrow button column with space

        self.other_sub_layout.addWidget(self.other_table)
        self.tables_layout.addLayout(self.other_sub_layout)

    def _populate_table(self, table, df, orig_indices, button_col, data_start, is_other=False):
        """Populate a QTableWidget with data, buttons, and formatting."""
        center_cols = ['CRM Email', 'PSP Email', 'CRM Amount', 'PSP Amount', 'CRM TP', 'PSP TP',
                       'CRM Last 4 Digits', 'PSP Last 4 Digits', 'CRM Currency', 'PSP Currency',
                       'CRM Processor Name', 'PSP Processor Name']
        display_columns = df.columns.drop('comment').tolist() + ['comment']
        for i in range(len(df)):
            if is_other:
                crm_orig, psp_orig = self.other_paired_orig[i]
                table.setItem(i, 0, QTableWidgetItem(str(crm_orig)))
                table.setItem(i, 1, QTableWidgetItem(str(psp_orig)))
            else:
                table.setItem(i, 0, QTableWidgetItem(str(orig_indices[i])))

            # Add toggle button
            button = QPushButton('✅')
            button.setObjectName('row_button')
            button.setStyleSheet("color: green; background: transparent; border: none;")
            button.clicked.connect(self.make_toggle_accept(table))
            container = QWidget()
            container_layout = QHBoxLayout()
            container_layout.addStretch(1)
            container_layout.addWidget(button)
            container_layout.addStretch(1)
            container_layout.setAlignment(Qt.AlignCenter)
            container_layout.setContentsMargins(0, 0, 0, 0)
            container.setLayout(container_layout)
            container.setStyleSheet("background-color: #ffffff;")
            table.setCellWidget(i, button_col, container)

            # Populate data columns
            for j, col in enumerate(display_columns):
                val = df.iloc[i][col]
                item_text = self.format_cell_value(val, col)
                item = QTableWidgetItem(item_text)
                item.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
                if col in center_cols:
                    item.setTextAlignment(Qt.AlignVCenter | Qt.AlignHCenter)
                table.setItem(i, j + data_start, item)

    def on_load_error(self, error_msg):
        """Callback for load errors; shows message and closes window."""
        self.layout.removeWidget(self.loading_label)
        self.loading_label.deleteLater()
        QMessageBox.critical(self, "Error", f"Failed to load warnings: {error_msg}")
        self.close()

    def format_cell_value(self, val, col):
        """Format cell values for display based on column type."""
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
        """Extract match key (last4 or email) from comment for sorting/merging."""
        if pd.isna(comment) or str(comment).strip() == '':
            return '', ''
        comment_str = str(comment)
        # Prefer last occurrence of last4
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
        """Get simplified comment for accepted matches."""
        comment_str = str(comment)
        suffix = " and was considered a match after review"
        if "Matched similar email" in comment_str:
            return "Emails matched" + suffix
        elif "Matched the same last 4 digits" in comment_str:
            return "Last 4 Digits matched" + suffix
        elif "Cross-processor" in comment_str:
            return "Different processors" + suffix
        else:
            return "Warning accepted" + suffix

    def get_display_comment(self, comment):
        """Get display-friendly comment for warnings table."""
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
        """Create handler for toggling row acceptance."""
        def handler():
            button = self.sender()
            row = self.get_row_from_button(table, button)
            if row != -1:
                self.toggle_accept(table, row)
        return handler

    def get_button_col(self, table):
        """Get button column index for the given table."""
        return 2 if hasattr(self, 'other_table') and table == self.other_table else 1

    def get_row_from_button(self, table, button):
        """Find row index from button widget."""
        button_col = self.get_button_col(table)
        for r in range(table.rowCount()):
            cell_widget = table.cellWidget(r, button_col)
            if cell_widget and cell_widget.layout().itemAt(1).widget() == button:
                return r
        return -1

    def adjust_tables_and_window(self):
        """Adjust table sizes, hide empty tables, and resize/center window."""
        # Collect tables and labels
        tables = []
        labels = []
        if hasattr(self, 'differ_table') and self.differ_table:
            tables.append(self.differ_table)
            labels.append(self.differ_label if hasattr(self, 'differ_label') else None)
        if hasattr(self, 'other_table') and self.other_table:
            tables.append(self.other_table)
            labels.append(self.other_label if hasattr(self, 'other_label') else None)

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

        # Step 1: Resize rows and columns to contents
        for table in tables:
            table.resizeColumnsToContents()
            table.resizeRowsToContents()
            # Enforce min/max widths for data columns
            if table == getattr(self, 'differ_table', None):
                button_col = 1
                data_start = 2
            elif table == getattr(self, 'other_table', None):
                button_col = 2
                data_start = 3
            else:
                continue
            table.setColumnWidth(button_col, 50)
            for j in range(data_start, table.columnCount()):
                col_label = table.horizontalHeaderItem(j).text()
                cw = table.columnWidth(j)
                if col_label == 'comment':
                    min_w, max_w = 250, 400
                elif 'Email' in col_label:
                    min_w, max_w = 150, 250
                elif 'Amount' in col_label or 'Last 4 Digits' in col_label or 'Currency' in col_label:
                    min_w, max_w = 80, 120
                elif 'TP' in col_label:
                    min_w, max_w = 100, 150
                elif 'Processor Name' in col_label:
                    min_w, max_w = 120, 200
                else:
                    min_w, max_w = 100, 200
                new_w = max(min_w, min(cw, max_w))
                table.setColumnWidth(j, new_w)

        # Step 2: Calculate heights and widths
        max_differ_rows = 3
        max_other_rows = 5
        total_height = 0
        max_table_width = 0
        scrollbar_extent = QApplication.style().pixelMetric(QStyle.PM_ScrollBarExtent)

        for table, label in zip(tables, labels):
            row_count = table.rowCount()
            is_differ_table = table == getattr(self, 'differ_table', None)
            max_visible_rows = max_differ_rows if is_differ_table else max_other_rows

            header_height = table.horizontalHeader().height()
            row_height_sum = sum(table.rowHeight(i) for i in range(min(row_count, max_visible_rows)))
            table_height = header_height + row_height_sum + 40

            if row_count > max_visible_rows:
                table.setFixedHeight(table_height)
                table.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
            else:
                table.setFixedHeight(table_height)
                table.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)

            table_width = sum(table.columnWidth(j) for j in range(table.columnCount())) + table.verticalHeader().width() + 2 * table.frameWidth() + 20
            if row_count > max_visible_rows:
                table_width += scrollbar_extent
            max_table_width = max(max_table_width, table_width)

            total_height += table_height
            if label and label.isVisible():
                total_height += label.height()

        # Step 3: Add buttons and margins
        button_layout = self.layout().itemAt(self.layout().count() - 1).layout()
        button_height = button_layout.itemAt(0).widget().height()
        total_button_height = button_height + button_layout.spacing()
        total_height += total_button_height

        layout_margins = self.layout().contentsMargins()
        total_height += layout_margins.top() + layout_margins.bottom()
        total_height += self.layout().spacing() * (len(tables) + 1)

        scroll_area_margins = self.scroll_area.contentsMargins()
        total_height += scroll_area_margins.top() + scroll_area_margins.bottom()
        frame_overhead = self.frameGeometry().height() - self.height() if self.isVisible() else 40

        # Step 4: Cap height to screen
        available_height = QApplication.desktop().availableGeometry().height()
        max_height = available_height - frame_overhead
        final_height = min(total_height + frame_overhead, max_height)

        # Step 5: Calculate width
        total_margins_width = layout_margins.left() + layout_margins.right() + scroll_area_margins.left() + scroll_area_margins.right() + 40
        window_width = max_table_width + total_margins_width
        window_width += 100
        available_width = QApplication.desktop().availableGeometry().width()
        window_width = max(1080, min(window_width, available_width - 20))
        window_width = max(window_width, 600)

        self.setFixedSize(window_width, final_height)

        # Step 6: Center window
        screen = QDesktopWidget().screenGeometry()
        x = (screen.width() - window_width) // 2
        y = (screen.height() - final_height) // 2
        self.setGeometry(x, y, window_width, final_height)

    def toggle_accept(self, table, row):
        """Toggle acceptance state of a row and update button appearance."""
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
        """Enable/disable remove button based on selected rows."""
        total_selected = sum(len(self.accepted_rows[t]) for t in self.accepted_rows)
        self.remove_btn.setEnabled(total_selected > 0)

    def remove_selected(self):
        """Remove selected (accepted) rows from tables."""
        for table in self.accepted_rows:
            rows = sorted(list(self.accepted_rows[table]), reverse=True)
            for r in rows:
                idx_item = table.item(r, 0)
                if idx_item:
                    orig_idx = int(idx_item.text())
                    self.remove_rows_by_index(orig_idx)
            self.accepted_rows[table].clear()
        QTimer.singleShot(0, self.adjust_tables_and_window)
        self.update_remove_button_state()

    def remove_rows_by_index(self, orig_idx):
        """Remove rows with given original index from all tables."""
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
        """Process user selections, update matching file, and open next window."""
        print("Starting on_next")
        # Collect remaining (unselected) original indices
        remaining_indices = self._collect_remaining_indices()

        # Calculate removed (accepted) indices
        removed_indices = set(self.orig_indices) - remaining_indices

        # Load exchange rates
        exchange_rate_map = self._load_exchange_rates()

        # Load and update matching_df
        self._update_matching_df(removed_indices, remaining_indices, exchange_rate_map)

        # Save updated matching_df
        self._save_updated_matching()

        # Proceed to next window based on regulation
        self._open_next_window()

        self.hide()
        QTimer.singleShot(0, self.close)

    def _collect_remaining_indices(self):
        """Collect original indices of remaining (unselected) rows."""
        remaining_indices = set()
        tables = [getattr(self, attr, None) for attr in ['differ_table', 'other_table'] if getattr(self, attr, None)]
        for t in tables:
            if t is getattr(self, 'other_table', None):
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
        return remaining_indices

    def _load_exchange_rates(self):
        """Load exchange rates from CSV file."""
        exchange_rate_map = {}
        rates_path = RATES_DIR / f"rates_{self.date_str}.csv"
        if rates_path.exists():
            rates_df = pd.read_csv(rates_path)
            rates_df['from_currency'] = rates_df['from_currency'].str.strip().str.upper()
            rates_df['to_currency'] = rates_df['to_currency'].str.strip().str.upper()
            exchange_rate_map = {
                (row['from_currency'], row['to_currency']): row['rate']
                for _, row in rates_df.iterrows()
            }
        return exchange_rate_map

    def _update_matching_df(self, removed_indices, remaining_indices, exchange_rate_map):
        """Update matching_df with accepted and unselected rows."""
        original_matching_path = self.dirs['lists_dir'] / self.date_str / f"{self.regulation}_withdrawals_matching.xlsx"
        matching_df = pd.read_excel(original_matching_path)

        # Update accepted rows (differ and regular)
        for idx in removed_indices:
            self._update_accepted_row(matching_df, idx, exchange_rate_map)

        # Update accepted other pairs (merge PSP into CRM)
        for display_r, (crm_orig, psp_orig) in enumerate(self.other_paired_orig):
            if crm_orig not in remaining_indices and psp_orig not in remaining_indices:
                self._merge_accepted_pair(matching_df, crm_orig, psp_orig, exchange_rate_map)

        print("Updated other paired rows")

        # Process unselected rows: drop and create split rows
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

        print(f"DEBUG: Processing {len(remaining_indices)} remaining indices for split rows")

        for idx in remaining_indices:
            local_idx = self.orig_to_local.get(idx, None)
            if local_idx is None:
                print(f"DEBUG: Could not find local_idx for orig_idx {idx}")
                continue

            # Determine if row has CRM/PSP data
            has_crm, has_proc = self._determine_split_types(idx, local_idx)

            # Get original row data
            orig_row_data = self.original_matching_df.loc[idx] if idx in self.original_matching_df.index else matching_df.loc[idx] if idx in matching_df.index else None
            orig_comment = orig_row_data['comment'] if orig_row_data is not None and 'comment' in orig_row_data else self.warnings_df.loc[local_idx, 'comment'] if 'comment' in self.warnings_df.columns else ''

            prefixed_comment = f"Unmatched due to warning: {orig_comment}"

            # Drop original row
            if idx in matching_df.index:
                print(f"DEBUG: Dropping row {idx} from matching_df")
                matching_df = matching_df.drop(idx)

            # Create CRM split if applicable
            if has_crm:
                crm_row_dict = self._create_crm_split(idx, orig_row_data, self.warnings_df.loc[local_idx].rename(reverse_rename), prefixed_comment)
                unselected_split_rows.append(crm_row_dict)

            # Create PSP split if applicable
            if has_proc:
                proc_row_dict = self._create_proc_split(idx, orig_row_data, self.warnings_df.loc[local_idx].rename(reverse_rename), prefixed_comment)
                unselected_split_rows.append(proc_row_dict)

        print(f"DEBUG: Unselected split rows created: {len(unselected_split_rows)}")

        # Append split rows to matching_df
        if unselected_split_rows:
            split_df = pd.DataFrame(unselected_split_rows)
            for col in matching_df.columns:
                if col not in split_df.columns:
                    split_df[col] = np.nan
            split_df = split_df[matching_df.columns]
            print(f"DEBUG: Split DF columns: {split_df.columns.tolist()}")
            print(f"DEBUG: Split DF shape: {split_df.shape}")
            print(f"DEBUG: Split DF PSP emails: {split_df['proc_email'].notna().sum()}")
            print(f"DEBUG: Split DF CRM emails: {split_df['crm_email'].notna().sum()}")
            matching_df = pd.concat([matching_df, split_df], ignore_index=True)
            print("Concat done")

        print(f"Updated matching_df shape after splits: {matching_df.shape}")
        print(f"Unselected CRM splits: {matching_df[(matching_df['match_status'] == 0) & (matching_df['crm_email'].notna())].shape[0]}")
        print(f"Unselected Proc splits: {matching_df[(matching_df['match_status'] == 0) & (matching_df['proc_email'].notna())].shape[0]}")

        self.matching_df = matching_df  # Save for saving later

    def _update_accepted_row(self, matching_df, idx, exchange_rate_map):
        """Update a single accepted row in matching_df."""
        if idx in matching_df.index:
            crm_amount = self.original_matching_df.at[idx, 'crm_amount'] if idx in self.original_matching_df.index else matching_df.at[idx, 'crm_amount']
            proc_amount_crm_currency = self.original_matching_df.at[idx, 'proc_amount_crm_currency'] if idx in self.original_matching_df.index else matching_df.at[idx, 'proc_amount_crm_currency']
            proc_amount = self.original_matching_df.at[idx, 'proc_amount'] if idx in self.original_matching_df.index else matching_df.at[idx, 'proc_amount']
            proc_currency = self.original_matching_df.at[idx, 'proc_currency'] if idx in self.original_matching_df.index else matching_df.at[idx, 'proc_currency']
            crm_currency = self.original_matching_df.at[idx, 'crm_currency'] if idx in self.original_matching_df.index else matching_df.at[idx, 'crm_currency']

            if (pd.notna(crm_amount) and pd.notna(proc_amount_crm_currency) and
                    abs(abs(crm_amount) - abs(proc_amount_crm_currency)) > 0.01):

                diff = abs(crm_amount) - abs(proc_amount_crm_currency)
                currency = crm_currency if pd.notna(crm_currency) else 'USD'
                type_ = "Underpaid" if diff > 0 else "Overpaid"
                abs_diff = abs(diff)
                comment = f"{type_} by {abs_diff:.2f} {currency}, Warning accepted and was considered a match after review."
                matching_df.at[idx, 'comment'] = comment
                matching_df.at[idx, 'payment_status'] = 0
            elif (pd.notna(crm_amount) and pd.notna(proc_amount) and
                  pd.notna(crm_currency) and pd.notna(proc_currency) and
                  crm_currency != proc_currency):
                proc_amount_crm_currency = self._convert_currency(proc_amount, proc_currency, crm_currency, exchange_rate_map, matching_df, idx)
                crm_amount_abs = abs(crm_amount)
                proc_amount_abs = abs(proc_amount)

                if pd.notna(proc_amount_crm_currency):
                    received_amount_crm = abs(proc_amount_crm_currency)
                    diff = crm_amount_abs - received_amount_crm
                    type_ = "Underpaid" if diff > 0 else "Overpaid"
                    abs_diff = abs(diff)
                    comment = f"{type_} by {abs_diff:.2f} {crm_currency}, Warning accepted and was considered a match after review."
                else:
                    comment = f"Client Requested {self._format_num(crm_amount_abs)} {crm_currency} and PSP shows {self._format_num(proc_amount_abs)} {proc_currency} . Different currencies, amount difference cannot be calculated . Warning accepted and was considered a match after review"

                matching_df.at[idx, 'comment'] = comment
                matching_df.at[idx, 'payment_status'] = 0
            else:
                orig_comment = self.original_matching_df.at[idx, 'comment'] if idx in self.original_matching_df.index else ''
                processed = process_comment(orig_comment)
                simplified = self.get_simplified_comment(processed)
                matching_df.at[idx, 'comment'] = simplified
                matching_df.at[idx, 'payment_status'] = 1

            matching_df.at[idx, 'warning'] = False
            matching_df.at[idx, 'match_status'] = 1

    def _merge_accepted_pair(self, matching_df, crm_orig, psp_orig, exchange_rate_map):
        """Merge accepted PSP data into CRM row for other pairs."""
        if crm_orig in matching_df.index and psp_orig in matching_df.index:
            psp_row = matching_df.loc[psp_orig]
            for col in ['proc_email', 'proc_amount', 'proc_currency', 'proc_tp', 'proc_processor_name',
                        'proc_last4', 'proc_date', 'proc_firstname', 'proc_lastname',
                        'proc_amount_crm_currency']:
                if col in matching_df.columns:
                    matching_df.at[crm_orig, col] = psp_row[col]

            crm_amount = matching_df.at[crm_orig, 'crm_amount']
            proc_amount_crm_currency = matching_df.at[crm_orig, 'proc_amount_crm_currency']
            proc_amount = matching_df.at[crm_orig, 'proc_amount']
            proc_currency = matching_df.at[crm_orig, 'proc_currency']
            crm_currency = matching_df.at[crm_orig, 'crm_currency']

            if (pd.notna(crm_amount) and pd.notna(proc_amount_crm_currency) and
                    abs(abs(crm_amount) - abs(proc_amount_crm_currency)) > 0.01):

                diff = abs(crm_amount) - abs(proc_amount_crm_currency)
                currency = crm_currency if pd.notna(crm_currency) else 'USD'
                type_ = "Underpaid" if diff > 0 else "Overpaid"
                abs_diff = abs(diff)
                comment = f"{type_} by {abs_diff:.2f} {currency}, Warning accepted and was considered a match after review."
                matching_df.at[crm_orig, 'comment'] = comment
                matching_df.at[crm_orig, 'payment_status'] = 0
            elif (pd.notna(crm_amount) and pd.notna(proc_amount) and
                  pd.notna(crm_currency) and pd.notna(proc_currency) and
                  crm_currency != proc_currency):
                proc_amount_crm_currency = self._convert_currency(proc_amount, proc_currency, crm_currency, exchange_rate_map, matching_df, crm_orig)
                crm_amount_abs = abs(crm_amount)
                proc_amount_abs = abs(proc_amount)

                if pd.notna(proc_amount_crm_currency):
                    received_amount_crm = abs(proc_amount_crm_currency)
                    diff = crm_amount_abs - received_amount_crm
                    type_ = "Underpaid" if diff > 0 else "Overpaid"
                    abs_diff = abs(diff)
                    comment = f"{type_} by {abs_diff:.2f} {crm_currency}, Warning accepted and was considered a match after review."
                else:
                    comment = f"Client Requested {self._format_num(crm_amount_abs)} {crm_currency} and PSP shows {self._format_num(proc_amount_abs)} {proc_currency} . Different currencies, amount difference cannot be calculated . Warning accepted and was considered a match after review"

                matching_df.at[crm_orig, 'comment'] = comment
                matching_df.at[crm_orig, 'payment_status'] = 0
            else:
                orig_comment = self.original_matching_df.at[crm_orig, 'comment'] if crm_orig in self.original_matching_df.index else ''
                processed = process_comment(orig_comment)
                simplified = self.get_simplified_comment(processed)
                matching_df.at[crm_orig, 'comment'] = simplified
                matching_df.at[crm_orig, 'payment_status'] = 1

            matching_df.at[crm_orig, 'warning'] = False
            matching_df.at[crm_orig, 'match_status'] = 1
            matching_df = matching_df.drop(psp_orig)

    def _convert_currency(self, proc_amount, proc_currency, crm_currency, exchange_rate_map, matching_df, idx):
        """Convert proc_amount to crm_currency if needed."""
        proc_amount_crm_currency = matching_df.at[idx, 'proc_amount_crm_currency']
        if pd.isna(proc_amount_crm_currency) and exchange_rate_map:
            from_curr = str(proc_currency).upper()
            to_curr = str(crm_currency).upper()
            key = (from_curr, to_curr)
            if key in exchange_rate_map:
                rate = exchange_rate_map[key]
                proc_amount_crm_currency = proc_amount * rate
            else:
                inv_key = (to_curr, from_curr)
                if inv_key in exchange_rate_map:
                    rate = exchange_rate_map[inv_key]
                    proc_amount_crm_currency = proc_amount / rate
            if not pd.isna(proc_amount_crm_currency):
                matching_df.at[idx, 'proc_amount_crm_currency'] = proc_amount_crm_currency
        return proc_amount_crm_currency

    def _format_num(self, num):
        """Format number for comments, removing trailing zeros."""
        if pd.isna(num):
            return '0'
        rounded = round(abs(num), 2)
        formatted = f"{rounded:.2f}"
        return formatted.rstrip('0').rstrip('.') if formatted.endswith('00') else formatted

    def _determine_split_types(self, idx, local_idx):
        """Determine if row should split into CRM and/or PSP unmatched."""
        orig_has_crm = False
        orig_has_proc = False
        if idx in self.original_matching_df.index:
            orig_row = self.original_matching_df.loc[idx]
            orig_has_crm = pd.notna(orig_row.get('crm_email', np.nan))
            orig_has_proc = pd.notna(orig_row.get('proc_email', np.nan))
            print(f"DEBUG: Original row {idx}: has_crm={orig_has_crm}, has_proc={orig_has_proc}")

        row = self.warnings_df.loc[local_idx]
        warnings_has_crm = pd.notna(row.get('CRM Email', np.nan))
        warnings_has_proc = pd.notna(row.get('PSP Email', np.nan))
        print(f"DEBUG: Warnings row {idx}: has_crm={warnings_has_crm}, has_proc={warnings_has_proc}")

        has_crm = orig_has_crm or warnings_has_crm
        has_proc = orig_has_proc or warnings_has_proc

        if not has_crm and not has_proc:
            if idx in self.original_matching_df.index:
                orig_row = self.original_matching_df.loc[idx]
                has_proc_data = any(pd.notna(orig_row.get(col, np.nan)) for col in
                                    ['proc_amount', 'proc_currency', 'proc_processor_name', 'proc_last4'])
                if has_proc_data:
                    has_proc = True
                    print(f"DEBUG: Detected PSP data in other fields for row {idx}, forcing PSP split")

            has_proc_data_warnings = any(pd.notna(row.get(col, np.nan)) for col in
                                         ['PSP Amount', 'PSP Currency', 'PSP Processor Name', 'PSP Last 4 Digits'])
            if has_proc_data_warnings:
                has_proc = True
                print(f"DEBUG: Detected PSP data in warnings other fields for row {idx}, forcing PSP split")

        if not has_crm and not has_proc:
            print(f"DEBUG WARNING: Row {idx} has no detected data but is a warning row. Creating both splits to prevent data loss.")
            has_crm = True
            has_proc = True

        print(f"DEBUG: Final decision for row {idx}: has_crm={has_crm}, has_proc={has_proc}")
        return has_crm, has_proc

    def _create_crm_split(self, idx, orig_row_data, warnings_row, prefixed_comment):
        """Create CRM unmatched split row."""
        print(f"DEBUG: Creating CRM split for row {idx}")
        crm_row_dict = {}
        if orig_row_data is not None:
            for col in orig_row_data.index:
                if col.startswith('crm_') or col in ['payment_method', 'regulation', 'crm_type']:
                    crm_row_dict[col] = orig_row_data[col]

        if not crm_row_dict:
            crm_row_dict = warnings_row.to_dict()
            proc_cols = [c for c in crm_row_dict.keys() if c.startswith('proc_')]
            for col in proc_cols:
                crm_row_dict[col] = np.nan

        crm_row_dict['match_status'] = 0
        crm_row_dict['payment_status'] = 0
        crm_row_dict['warning'] = False
        crm_row_dict['comment'] = prefixed_comment
        if 'crm_type' not in crm_row_dict:
            crm_row_dict['crm_type'] = 'Withdrawal'

        print(f"DEBUG: CRM split for {idx} - email: {crm_row_dict.get('crm_email')}, amount: {crm_row_dict.get('crm_amount')}")
        return crm_row_dict

    def _create_proc_split(self, idx, orig_row_data, warnings_row, prefixed_comment):
        """Create PSP unmatched split row."""
        print(f"DEBUG: Creating PSP split for row {idx}")
        proc_row_dict = {}
        if idx in self.original_matching_df.index:
            orig_row = self.original_matching_df.loc[idx]
            for col in orig_row.index:
                if col.startswith('proc_'):
                    proc_row_dict[col] = orig_row[col]
                    if pd.notna(orig_row[col]):
                        print(f"DEBUG: PSP field from original {col} = {orig_row[col]}")

        for col in warnings_row.index:
            if col.startswith('proc_') and col not in proc_row_dict:
                proc_row_dict[col] = warnings_row[col]
                if pd.notna(warnings_row[col]):
                    print(f"DEBUG: PSP field from warnings {col} = {warnings_row[col]}")

        if not any(pd.notna(proc_row_dict.get(col, np.nan)) for col in ['proc_email', 'proc_amount', 'proc_currency']):
            print(f"DEBUG: No PSP data found for row {idx}, checking original row directly")
            if idx in self.original_matching_df.index:
                orig_row = self.original_matching_df.loc[idx]
                for col in ['proc_email', 'proc_amount', 'proc_currency', 'proc_processor_name', 'proc_last4']:
                    if col in orig_row.index and pd.notna(orig_row[col]):
                        proc_row_dict[col] = orig_row[col]
                        print(f"DEBUG: Direct PSP field {col} = {orig_row[col]}")

        crm_cols = [c for c in self.original_matching_df.columns if c.startswith('crm_') and c != 'crm_type']
        for col in crm_cols:
            proc_row_dict[col] = np.nan

        proc_row_dict['payment_method'] = np.nan
        proc_row_dict['match_status'] = 0
        proc_row_dict['payment_status'] = 0
        proc_row_dict['warning'] = False
        proc_row_dict['comment'] = prefixed_comment
        proc_row_dict['crm_type'] = np.nan

        print(f"DEBUG: PSP split for {idx} - email: {proc_row_dict.get('proc_email')}, amount: {proc_row_dict.get('proc_amount')}, currency: {proc_row_dict.get('proc_currency')}, processor: {proc_row_dict.get('proc_processor_name')}")
        return proc_row_dict

    def _save_updated_matching(self):
        """Save the updated matching_df to file."""
        output_dir = self.dirs['output_dir'] / self.date_str
        updated_matching_path = output_dir / "withdrawals_matching_updated.xlsx"
        self.matching_df.to_excel(updated_matching_path, index=False)
        print(f"Updated matching saved to {updated_matching_path}")
        print("Processing complete.")

    def _open_next_window(self):
        """Open the next window based on regulation and warnings presence."""
        if self.regulation == 'uk':
            print("Opening ROW review window.")
            has = ThirdWindow.has_warnings('row', self.date_str)
            print(f"has_warnings for row: {has}")
            if has:
                print("Opening ROW ThirdWindow")
                self.next_window = ThirdWindow(self.date_str, 'row')
            else:
                print("No warnings for ROW, directly opening export window.")
                self.next_window = FourthWindow(self.date_str)
        else:
            print("Opening export window.")
            self.next_window = FourthWindow(self.date_str)
        print("Next window created")
        self.next_window.show()
        print("Next window shown")

class LoadWarningsThread(QThread):
    """Thread to load warnings data in background."""
    dataLoaded = pyqtSignal(dict)
    errorOccurred = pyqtSignal(str)

    def __init__(self, date_str, regulation):
        super().__init__()
        self.date_str = date_str
        self.regulation = regulation

    def run(self):
        try:
            dirs = setup_dirs_for_reg(self.regulation)
            output_dir = dirs['output_dir'] / self.date_str
            output_dir.mkdir(parents=True, exist_ok=True)

            warnings_withdrawals_path = output_dir / f"{self.regulation.upper()} warnings_withdrawals.xlsx"
            original_path = dirs['lists_dir'] / self.date_str / f"{self.regulation}_withdrawals_matching.xlsx"

            # Load original matching file and clean
            original_matching_df = pd.read_excel(original_path) if original_path.exists() else pd.DataFrame()
            if not original_matching_df.empty:
                original_matching_df['crm_amount'] = original_matching_df['crm_amount'].apply(clean_value)
                original_matching_df['proc_amount'] = original_matching_df['proc_amount'].apply(clean_value)
                original_matching_df['proc_amount_crm_currency'] = original_matching_df['proc_amount_crm_currency'].apply(clean_value)
                original_matching_df['crm_amount'] = pd.to_numeric(original_matching_df['crm_amount'], errors='coerce')
                original_matching_df['proc_amount'] = pd.to_numeric(original_matching_df['proc_amount'], errors='coerce')
                original_matching_df['proc_amount_crm_currency'] = pd.to_numeric(original_matching_df['proc_amount_crm_currency'], errors='coerce')
                original_matching_df['comment'] = original_matching_df['comment'].fillna('').astype(str)
                str_columns = ['crm_firstname', 'crm_lastname', 'proc_firstname', 'proc_lastname',
                               'crm_email', 'proc_email',
                               'crm_currency', 'proc_currency',
                               'crm_processor_name', 'proc_processor_name',
                               'payment_method', 'regulation',
                               'crm_last4', 'proc_last4',
                               'crm_tp', 'proc_tp',
                               'crm_type']
                for col in str_columns:
                    if col in original_matching_df.columns:
                        join = 'email' in col
                        original_matching_df[col] = original_matching_df[col].apply(lambda x: clean_value(x, join_list=join))

            # Handle no warnings file
            if not warnings_withdrawals_path.exists():
                data_dict = {
                    'warnings_df': pd.DataFrame(columns=[
                        'crm_email', 'crm_amount', 'crm_currency', 'crm_tp', 'crm_processor_name', 'crm_last4',
                        'proc_email', 'proc_amount', 'proc_currency', 'proc_tp', 'proc_processor_name', 'proc_last4',
                        'comment', 'crm_date', 'proc_date'
                    ]),
                    'orig_indices': [],
                    'orig_to_local': {},
                    'no_warnings': True,
                    'original_matching_df': original_matching_df
                }
                self.dataLoaded.emit(data_dict)
                return

            # Load warnings file
            warnings_df = pd.read_excel(warnings_withdrawals_path)

            if 'orig_index' in warnings_df.columns:
                warnings_df['orig_index'] = pd.to_numeric(warnings_df['orig_index'], errors='coerce').dropna().astype(int)
                orig_indices = [int(x) for x in warnings_df['orig_index'].values]
                warnings_df = warnings_df.drop('orig_index', axis=1)
                orig_to_local = {orig_indices[i]: i for i in range(len(orig_indices))}
            else:
                orig_indices = list(range(len(warnings_df)))
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