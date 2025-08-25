import sys
import os
import re
from PyQt5.QtWidgets import (QApplication, QMainWindow, QFileDialog, QPushButton, QLabel, QVBoxLayout, QWidget,
                             QTableWidget, QTableWidgetItem, QHBoxLayout, QLineEdit)
from PyQt5.QtCore import Qt
from pathlib import Path
from datetime import datetime
import pandas as pd
from src.reports_creator import process_files_in_parallel, combine_processed_files
from src.withdrawals_matcher_test import ReconciliationEngine
from src.config import PROCESSOR_DIR, CRM_DIR, DATA_DIR

class ReconciliationApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Reconciliation Tool Demo")
        self.setGeometry(100, 100, 600, 400)

        # Main widget and layout
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        main_layout = QVBoxLayout(main_widget)

        # File selection
        file_layout = QHBoxLayout()
        self.file_label = QLabel("No files selected")
        self.file_button = QPushButton("Select Files")
        self.file_button.clicked.connect(self.select_files)
        file_layout.addWidget(self.file_label)
        file_layout.addWidget(self.file_button)
        main_layout.addLayout(file_layout)

        # Date input
        date_layout = QHBoxLayout()
        self.date_label = QLabel("Date (YYYY-MM-DD):")
        self.date_edit = QLineEdit(datetime.now().strftime("%Y-%m-%d"))
        date_layout.addWidget(self.date_label)
        date_layout.addWidget(self.date_edit)
        main_layout.addLayout(date_layout)

        # Process button
        self.process_button = QPushButton("Generate Reports")
        self.process_button.clicked.connect(self.generate_reports)
        main_layout.addWidget(self.process_button)

        # Table for results
        self.table = QTableWidget()
        main_layout.addWidget(self.table)

        # Export button
        self.export_button = QPushButton("Export Updated Lists")
        self.export_button.clicked.connect(self.export_lists)
        self.export_button.setEnabled(False)
        main_layout.addWidget(self.export_button)

        # Status label
        self.status_label = QLabel("Ready")
        main_layout.addWidget(self.status_label)

        self.selected_files = []
        self.results_df = None

    def select_files(self):
        files, _ = QFileDialog.getOpenFileNames(self, "Select Raw Files", "",
                                               "CSV Files (*.csv);;Excel Files (*.xlsx)")
        if files:
            self.selected_files = files
            self.file_label.setText(f"Selected: {len(files)} files")

    def generate_reports(self):
        if not self.selected_files:
            self.status_label.setText("Please select files first!")
            return

        date = self.date_edit.text()
        if not re.match(r"\d{4}-\d{2}-\d{2}", date):
            self.status_label.setText("Invalid date format!")
            return

        # Move files to processor_reports and crm_reports
        processor_dir = PROCESSOR_DIR
        crm_dir = CRM_DIR
        for file in self.selected_files:
            if "crm" in file.lower():
                dest = crm_dir / os.path.basename(file)
            else:
                dest = processor_dir / os.path.basename(file)
            os.replace(file, dest)

        # Process files
        try:
            # Deposits
            process_files_in_parallel(self.selected_files, processor_name="all", is_crm=False, transaction_type="deposit")
            for proc in ["paypal", "safecharge", "powercash", "shift4", "skrill", "trustpayments", "neteller", "zotapay", "bitpay", "ezeebill", "paymentasia"]:
                process_files_in_parallel([str(crm_dir / f"crm_{date}.xlsx")], processor_name=proc, is_crm=True, transaction_type="deposit")
            combine_processed_files(date=date, processors=["paypal", "safecharge", "powercash", "shift4", "skrill", "trustpayments", "neteller", "zotapay", "bitpay", "ezeebill", "paymentasia"], transaction_type="deposit")

            # Withdrawals
            process_files_in_parallel(self.selected_files, processor_name="all", is_crm=False, transaction_type="withdrawal")
            for proc in ["paypal", "safecharge", "powercash", "shift4", "skrill", "trustpayments", "neteller", "zotapay", "bitpay", "ezeebill", "paymentasia"]:
                process_files_in_parallel([str(crm_dir / f"crm_{date}.xlsx")], processor_name=proc, is_crm=True, transaction_type="withdrawal")
            combine_processed_files(date=date, processors=["paypal", "safecharge", "powercash", "shift4", "skrill", "trustpayments", "neteller", "zotapay_paymentasia"], transaction_type="withdrawal", extra_processors=["zotapay_paymentasia"])

            # Load results (simplified to tracking list format)
            deposits_path = DATA_DIR / "lists" / date / "deposits_matching.xlsx"
            withdrawals_path = DATA_DIR / "lists" / date / "withdrawals_matching.xlsx"

            deposits_df = pd.read_excel(deposits_path)
            withdrawals_df = pd.read_excel(withdrawals_path)

            # Convert to tracking list format
            deposits_df = deposits_df.rename(columns={
                'crm_date': 'date', 'crm_amount': 'amount', 'crm_currency': 'currency', 'crm_tp': 'tp',
                'crm_last4': 'last4', 'crm_processor_name': 'processor_name', 'comment': 'comment'
            })[['date', 'amount', 'currency', 'tp', 'last4', 'processor_name', 'comment']]
            withdrawals_df = withdrawals_df.rename(columns={
                'crm_date': 'date', 'proc_amount': 'amount', 'proc_currency': 'currency', 'crm_tp': 'tp',
                'crm_last4': 'last4', 'proc_processor_name': 'processor_name', 'comment': 'comment'
            })[['date', 'amount', 'currency', 'tp', 'last4', 'processor_name', 'comment']]

            # Display in table
            self.results_df = pd.concat([deposits_df, withdrawals_df], ignore_index=True)
            self.update_table()
            self.export_button.setEnabled(True)
            self.status_label.setText("Reports generated successfully!")

        except Exception as e:
            self.status_label.setText(f"Error: {str(e)}")

    def update_table(self):
        self.table.setRowCount(len(self.results_df))
        self.table.setColumnCount(len(self.results_df.columns))
        self.table.setHorizontalHeaderLabels(self.results_df.columns)

        for row in range(len(self.results_df)):
            for col in range(len(self.results_df.columns)):
                item = QTableWidgetItem(str(self.results_df.iloc[row, col]))
                item.setFlags(item.flags() | Qt.ItemIsEditable)  # Allow editing
                self.table.setItem(row, col, item)

    def export_lists(self):
        if self.results_df is not None:
            date = self.date_edit.text()
            out_dir = DATA_DIR / "lists" / date
            out_dir.mkdir(parents=True, exist_ok=True)
            deposits_out = out_dir / "unapproved_deposits_updated.xlsx"
            withdrawals_out = out_dir / "underpays_withdrawals_updated.xlsx"
            self.results_df[self.results_df['amount'] >= 0].to_excel(deposits_out, index=False)
            self.results_df[self.results_df['amount'] < 0].to_excel(withdrawals_out, index=False)
            self.status_label.setText(f"Exported to {deposits_out} and {withdrawals_out}")

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = ReconciliationApp()
    window.show()
    sys.exit(app.exec_())