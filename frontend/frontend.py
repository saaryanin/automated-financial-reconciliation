import sys
import os
from PyQt5.QtWidgets import (QApplication, QWidget, QVBoxLayout, QHBoxLayout, QLabel, QLineEdit, 
                             QPushButton, QGridLayout, QFileDialog, QDateEdit, QMessageBox, QCalendarWidget)
from PyQt5.QtCore import Qt, QDate
import pandas as pd

# Use direct import from src.config
from src.config import RATES_DIR

class ReconciliationWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.initUI()

    def initUI(self):
        self.setWindowTitle('CRM-Processor Reconciliation System')
        self.setStyleSheet("""
            QWidget {
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #667eea, stop:1 #764ba2);
            }
            QLabel {
                color: #2c3e50;
            }
            QLineEdit {
                padding: 8px;
                border: 2px solid #e9ecef;
                border-radius: 4px;
                font-size: 14px;
                max-width: 100px;
            }
            QLineEdit:focus {
                border-color: #667eea;
                box-shadow: 0 0 0 2px rgba(102, 126, 234, 0.1);
            }
            QPushButton {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #667eea, stop:1 #764ba2);
                color: white;
                border: none;
                padding: 10px 20px;
                border-radius: 4px;
                font-size: 14px;
                font-weight: 600;
            }
            QPushButton:hover {
                transform: translateY(-2px);
                box-shadow: 0 6px 15px rgba(102, 126, 234, 0.3);
            }
            QPushButton:disabled {
                background: #6c757d;
                cursor: not-allowed;
                transform: none;
                box-shadow: none;
            }
            .upload-section {
                background: #f0f0f0;
                color: #333;
            }
            QDateEdit {
                padding: 4px;
                border: 2px solid #e9ecef;
                border-radius: 4px;
                font-size: 12px;
                background: #f8f9fa;
                color: #2c3e50;
                max-width: 90px;
            }
            QDateEdit::drop-down {
                width: 22px;
                border-left: 1px solid #e9ecef;
                subcontrol-origin: padding;
                subcontrol-position: top right;
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #e9ecef, stop:1 #fff);
            }
            QDateEdit::drop-down:hover {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #d1d7e0, stop:1 #f8f9fa);
                border-left: 1px solid #667eea;
            }
            QDateEdit::down-arrow {
                image: url(:/qt-project.org/styles/commonstyle/images/downarrow-16.png);
                width: 16px;
                height: 16px;
            }
            QCalendarWidget {
                background: #f8f9fa;
                border: 1px solid #e9ecef;
                border-radius: 4px;
            }
            QCalendarWidget QToolButton {
                background: #f0f0f0;
                color: #2c3e50;
                font-size: 12px;
                padding: 4px;
                border: none;
            }
            QCalendarWidget QToolButton:hover {
                background: #d1d7e0;
                color: #1a252f;
            }
            QCalendarWidget QMenu {
                background: #fff;
                color: #2c3e50;
            }
            QCalendarWidget QMenu::item:selected {
                background: #667eea;
                color: white;
            }
        """)
        # Adjust window position to be higher and centered
        screen = QApplication.desktop().screenGeometry()
        self.setGeometry((screen.width() - 800) // 2, 50, 800, 500)  # Centered, 50px from top

        # Main layout
        main_layout = QVBoxLayout()
        self.setLayout(main_layout)

        # Header
        header = QLabel('CRM-Processor Reconciliation System')
        header.setAlignment(Qt.AlignCenter)
        header.setStyleSheet("background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #2c3e50, stop:1 #34495e); color: white; padding: 15px; text-align: center; font-size: 20px; font-weight: 600;")
        main_layout.addWidget(header)

        # Currency Section
        currency_section = QWidget()
        currency_layout = QVBoxLayout()
        currency_section.setLayout(currency_layout)
        currency_section.setStyleSheet(
            "background: #f8f9fa; border-radius: 6px; padding: 15px; border: 1px solid #e9ecef;")
        currency_label = QLabel('💱 Currency Exchange Rates')
        currency_label.setStyleSheet("font-size: 16px; margin-bottom: 10px;")
        currency_layout.addWidget(currency_label)

        currency_grid = QGridLayout()
        currency_grid.setHorizontalSpacing(10)
        currency_grid.setVerticalSpacing(5)
        self.rate_inputs = {}
        rates = [('USD', 'EUR'), ('USD', 'GBP'), ('USD', 'MYR'), ('USD', 'CNY'), ('EUR', 'GBP')]
        for i, (from_curr, to_curr) in enumerate(rates):
            label = QLabel(f"{from_curr}/{to_curr}")
            label.setStyleSheet("font-size: 12px;")
            input_field = QLineEdit()
            input_field.setPlaceholderText(
                f"{0.8706 if from_curr == 'USD' and to_curr == 'EUR' else 0.7561 if from_curr == 'USD' and to_curr == 'GBP' else 4.6800 if from_curr == 'USD' and to_curr == 'MYR' else 7.2450 if from_curr == 'USD' and to_curr == 'CNY' else 0.8687}")
            input_field.textChanged.connect(self.update_reciprocal_rates)
            calc_label = QLabel(f"{to_curr}/{from_curr}: 0.0000")
            calc_label.setStyleSheet("font-size: 10px; color: #6c757d; font-style: italic;")
            self.rate_inputs[f"{from_curr}_{to_curr}"] = (input_field, calc_label)
            currency_grid.addWidget(label, i, 0)
            currency_grid.addWidget(input_field, i, 1)
            currency_grid.addWidget(calc_label, i, 2)
        currency_layout.addLayout(currency_grid)

        main_layout.addWidget(currency_section)

        # Date Picker (Compact)
        date_widget = QWidget()
        date_layout = QHBoxLayout()
        date_widget.setLayout(date_layout)
        date_label = QLabel("Date:")
        date_label.setStyleSheet("font-size: 12px; margin-right: 0px;")  # Removed margin
        self.date_edit = QDateEdit()
        self.date_edit.setDate(QDate.currentDate())  # Set to 01/09/2025
        self.date_edit.setCalendarPopup(True)
        self.date_edit.setDisplayFormat("dd/MM/yyyy")  # Israel format
        self.date_edit.setMaximumWidth(90)
        date_layout.addWidget(date_label, alignment=Qt.AlignRight)  # Align label to right
        date_layout.addWidget(self.date_edit)
        date_layout.setSpacing(0)  # No spacing between label and editor
        date_layout.setContentsMargins(0, 0, 0, 0)  # Remove extra margins
        currency_layout.addWidget(date_widget)

        main_layout.addWidget(currency_section)

        # File Upload Section
        file_section = QWidget()
        file_layout = QVBoxLayout()
        file_section.setLayout(file_layout)
        file_section.setStyleSheet("background: #f0f0f0; border-radius: 6px; padding: 15px; border: 1px solid #e9ecef; color: #333;")
        file_label = QLabel('📁 Upload Files')
        file_label.setStyleSheet("font-size: 16px; margin-bottom: 10px;")
        file_layout.addWidget(file_label)

        file_grid = QHBoxLayout()
        self.crm_file_btn = QPushButton('📊 CRM File')
        self.crm_file_btn.clicked.connect(lambda: self.select_file('crm'))
        self.processor_file_btn = QPushButton('💳 Processors Files')
        self.processor_file_btn.clicked.connect(lambda: self.select_file('processor'))
        file_grid.addWidget(self.crm_file_btn)
        file_grid.addWidget(self.processor_file_btn)
        file_layout.addLayout(file_grid)

        self.crm_file = None
        self.processor_files = []  # List for multiple files

        main_layout.addWidget(file_section)

        # Process Button
        self.process_btn = QPushButton('Start Processing')
        self.process_btn.setEnabled(False)
        self.process_btn.clicked.connect(self.save_rates_and_process)
        main_layout.addWidget(self.process_btn)

    def update_reciprocal_rates(self):
        for key, (input_field, calc_label) in self.rate_inputs.items():
            from_curr, to_curr = key.split('_')
            rate = float(input_field.text()) if input_field.text() else 0
            if rate > 0:
                calc_label.setText(f"{to_curr}/{from_curr}: {(1 / rate):.4f}")

    def select_file(self, file_type):
        file_dialog = QFileDialog()
        if file_type == 'crm':
            file_path, _ = file_dialog.getOpenFileName(self, "Select CRM File", "", "CSV Files (*.csv *.xlsx *.xls)")
            if file_path:
                self.crm_file = file_path
                self.crm_file_btn.setText(f"📊 {os.path.basename(file_path)}")
        else:  # processor, multiple files
            file_paths, _ = file_dialog.getOpenFileNames(self, "Select Processors Files", "", "CSV Files (*.csv *.xlsx *.xls)")
            if file_paths:
                self.processor_files = file_paths
                names = [os.path.basename(p) for p in file_paths]
                self.processor_file_btn.setText(f"💳 {', '.join(names)}")
        self.check_files_ready()

    def check_files_ready(self):
        self.process_btn.setEnabled(bool(self.crm_file and self.processor_files))

    def save_rates_and_process(self):
        date = self.date_edit.date().toString("yyyy-MM-dd")
        rates_data = []
        for key, (input_field, _) in self.rate_inputs.items():
            from_curr, to_curr = key.split('_')
            rate = float(input_field.text()) if input_field.text() else 0
            if rate > 0:
                rates_data.append([from_curr, to_curr, rate])
                reciprocal_rate = 1 / rate
                if (to_curr, from_curr) not in [k.split('_') for k in self.rate_inputs.keys()]:
                    rates_data.append([to_curr, from_curr, reciprocal_rate])

        if rates_data:
            df = pd.DataFrame(rates_data, columns=['from_currency', 'to_currency', 'rate'])
            file_path = RATES_DIR / f"rates_{date}.csv"
            df.to_csv(file_path, index=False)
            QMessageBox.information(self, "Success", f"Rates saved to {file_path}")
            # Proceed to processing (to be implemented in next steps)
        else:
            QMessageBox.warning(self, "Error", "No valid rates entered.")

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = ReconciliationWindow()
    window.show()
    sys.exit(app.exec_())