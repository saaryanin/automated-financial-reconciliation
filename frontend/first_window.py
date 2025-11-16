import os
import shutil
from PyQt5.QtWidgets import (QApplication, QWidget, QVBoxLayout, QHBoxLayout, QLabel, QLineEdit,
                             QPushButton, QGridLayout, QFileDialog, QMessageBox, QCalendarWidget,
                             QToolButton, QSizePolicy)
from PyQt5.QtCore import Qt, QDate, QRegExp
from PyQt5.QtGui import QRegExpValidator
from second_window import SecondWindow  # NEW: Import the new second window class
import pandas as pd
import re
from pathlib import Path
from src.config import RATES_DIR, RAW_ATTACHED_FILES, setup_dirs_for_reg
from src.files_renamer import PROCESSOR_PATTERNS


class DropButton(QPushButton):
    def __init__(self, text, window, parent=None):
        super().__init__(text, parent)
        self.window = window
        self.setAcceptDrops(True)
        self.setMinimumHeight(250)

    def dragEnterEvent(self, event):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()
            if not self.window.crm_file and not self.window.processor_files:
                self.setStyleSheet("""
                    border: 4px dashed #4a90e2;
                    background: #e6f0fa;
                    min-height: 250px;
                    border-radius: 8px;
                """)
            print("Drag enter accepted")

    def dropEvent(self, event):
        mime_data = event.mimeData()
        if mime_data.hasUrls():
            file_paths = [u.toLocalFile() for u in mime_data.urls()]
            try:
                for source_path in file_paths:
                    file_name = os.path.basename(source_path)
                    if file_name in self.window.moved_files:
                        self.window.show_warning("Duplicate Drop", f"{file_name} already moved.")
                        continue
                    if file_name.startswith("crm_"):
                        if self.window.crm_file:
                            self.window.show_warning("Duplicate CRM", "CRM file already set. Only one allowed.")
                            continue
                        dest_path = RAW_ATTACHED_FILES / file_name
                        shutil.copy(str(source_path), str(dest_path))
                        self.window.crm_file = str(dest_path)
                        self.window.moved_files.add(file_name)
                    else:
                        dest_path = RAW_ATTACHED_FILES / file_name
                        shutil.copy(str(source_path), str(dest_path))
                        self.window.processor_files.append(str(dest_path))
                        self.window.moved_files.add(file_name)
                self.window.update_upload_button()
            except Exception as e:
                print(f"Drop error: {e}")
                self.window.show_error("Error", f"Failed to process drop: {e}")
                self.setStyleSheet("")
        event.accept()

    def dragMoveEvent(self, event):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()

    def dragLeaveEvent(self, event):
        self.window.update_upload_button()
        if event is not None:
            event.accept()


class ReconciliationWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.crm_file = None
        self.processor_files = []
        self.date_button = None  # Add this for the custom calendar button
        self.valid_date_str = None  # Track valid date string
        self.initUI()
        self.moved_files = set()  # Track moved file names to avoid duplicates

    def _detect_processor(self, filename):
        """Detect processor name from filename based on keywords."""
        filename_lower = filename.lower()
        if filename_lower.startswith("crm_"):
            return "crm"
        if "transactionlog" in filename_lower:
            return "powercash"
        processors = [
            "safecharge", "safechargeuk", "bitpay", "ezeebill", "paypal", "zotapay", "paymentasia", "powercash",
            "trustpayments", "paysafe", "skrill", "neteller", "shift4", "barclays", "barclaycard"
        ]
        for processor in processors:
            if processor in filename_lower:
                return processor
        return "unknown"

    def initUI(self):
        self.setWindowTitle('CRM-Processor Reconciliation System')

        app = QApplication.instance()
        app.setStyleSheet("""
            QWidget {
                font-family: 'Segoe UI', Arial, sans-serif;
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #4a90e2, stop:1 #d3d8e8);
                border-radius: 10px;
                padding: 10px;
            }
            QLabel {
                color: #1a252f;
                font-weight: 500;
            }
            QLineEdit {
                padding: 8px;
                border: 2px solid #dfe6e9;
                border-radius: 4px;
                font-size: 14px;
                max-width: 100px;
            }
            QLineEdit:focus {
                border-color: #4a90e2;
                box-shadow: 0 0 5px rgba(74, 144, 226, 0.3);
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
            .section {
                background: #ffffff;
                border-radius: 8px;
                padding: 15px;
                margin-bottom: 15px;
                box-shadow: 0 2px 5px rgba(0, 0, 0, 0.1);
            }
            #upload-button {
                min-height: 200px;
                font-size: 24px;
                font-weight: bold;
            }
            #date-lineedit {
                padding: 8px;
                border: 2px solid #dfe6e9;
                border-radius: 4px 0 0 4px;
                font-size: 14px;
                background: #ffffff;
                color: #2c3e50;
                min-width: 80px;
            }
            #date-lineedit:focus {
                border-color: #4a90e2;
                box-shadow: 0 0 5px rgba(74, 144, 226, 0.3);
            }
            #date-button {
                padding: 8px 6px;
                border: 2px solid #dfe6e9;
                border-left: none;
                border-radius: 0 4px 4px 0;
                background: #ffffff;
                font-size: 14px;
                min-width: 14px;
                color: #1e90ff;
                font-weight: bold;
            }
            #date-button:hover {
                background: #e1f5fe;
            }
            QCalendarWidget {
                background: #4a90e2;
                border: 1px solid #e9ecef;
                border-radius: 4px;
                min-width: 280px;
            }
            QCalendarWidget QAbstractItemView {
                background: #ffffff;
                color: #1e90ff;
            }
            QCalendarWidget QToolButton {
                background: #f0f0f0;
                color: #1e90ff;
                font-size: 12px;
                padding: 4px;
                border: none;
            }
            QCalendarWidget QToolButton:hover {
                background: #d1d7e0;
                color: #1a252f;
            }
            QCalendarWidget QMenu {
                background: #ffffff;
                color: #1e90ff;
            }
            QCalendarWidget QMenu::item:selected {
                background: #4a90e2;
                color: #ffffff;
            }
            QMessageBox {
                background-color: #ffffff;
                border: 2px solid #4a90e2;
                border-radius: 8px;
                padding: 10px;
            }
            QMessageBox QLabel {
                color: #1a252f;
            }
            QMessageBox QPushButton {
                background: #4a90e2;
                color: #ffffff;
                border: none;
                padding: 8px 16px;
                border-radius: 4px;
            }
            QMessageBox QPushButton:hover {
                background: #357abd;
            }
        """)

        screen = QApplication.desktop().screenGeometry()
        self.setGeometry((screen.width() - 500) // 2, 50, 500, 600)

        main_layout = QVBoxLayout()
        main_layout.setSpacing(20)
        main_layout.setContentsMargins(20, 20, 20, 20)
        self.setLayout(main_layout)

        header = QLabel('CRM-Processor Reconciliation System')
        header.setAlignment(Qt.AlignCenter)
        header.setStyleSheet("""
            background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #2c3e50, stop:1 #34495e);
            color: #ffffff;
            padding: 10px;
            border-radius: 6px;
            font-size: 24px;
            font-weight: 700;
        """)
        main_layout.addWidget(header)

        currency_section = QWidget()
        currency_section.setObjectName("section")
        currency_layout = QVBoxLayout()
        currency_section.setLayout(currency_layout)
        currency_label = QLabel('💱 Currency Exchange Rates')
        currency_label.setStyleSheet("font-size: 18px; margin-bottom: 10px;")
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

        date_container = QWidget()
        date_container.setStyleSheet("""
            background: #ffffff;
            padding: 10px;
            border-radius: 4px;
            margin: 5px 0;
        """)

        date_layout = QHBoxLayout()
        date_label = QLabel("Date:")
        date_label.setStyleSheet("font-size: 12px; margin-right: 5px;")
        self.date_lineedit = QLineEdit()
        self.date_lineedit.setObjectName("date-lineedit")
        today = QDate.currentDate()
        yesterday = today.addDays(-1)
        if today.dayOfWeek() == 1:  # Monday (Qt: 1=Mon)
            yesterday = today.addDays(-3)  # Last Friday
        selected_date = yesterday.toString("dd/MM/yyyy")
        self.date_lineedit.setText(selected_date)
        self.date_lineedit.setMaximumWidth(90)
        self.valid_date_str = selected_date

        self.date_lineedit.setReadOnly(False)
        date_regex = QRegExp(r'^\d{1,2}/\d{1,2}/\d{4}$')
        validator = QRegExpValidator(date_regex)
        self.date_lineedit.setValidator(validator)
        self.date_lineedit.editingFinished.connect(self.on_date_edited)

        self.calendar = QCalendarWidget()
        self.calendar.setGridVisible(True)
        self.calendar.setVerticalHeaderFormat(QCalendarWidget.NoVerticalHeader)
        self.calendar.setWindowFlags(Qt.Popup)  # Removed FramelessWindowHint
        self.calendar.clicked.connect(self.calendar_date_selected)
        self.calendar.currentPageChanged.connect(self.update_calendar_layout)  # New: Handle page changes

        self.date_button = QToolButton()
        self.date_button.setAutoFillBackground(True)
        self.date_button.setObjectName("date-button")
        self.date_button.setText("📅")
        self.date_button.clicked.connect(self.show_calendar_popup)

        date_layout.addWidget(date_label, alignment=Qt.AlignRight)
        date_layout.addWidget(self.date_lineedit)
        date_layout.addWidget(self.date_button)
        date_layout.setSpacing(2)
        date_layout.setContentsMargins(0, 0, 0, 0)

        date_widget = QWidget()
        date_widget.setLayout(date_layout)
        date_container_layout = QVBoxLayout()
        date_container_layout.addWidget(date_widget, alignment=Qt.AlignHCenter)
        date_container_layout.setContentsMargins(0, 0, 0, 0)
        date_container.setLayout(date_container_layout)

        currency_layout.addWidget(date_container, alignment=Qt.AlignHCenter)

        main_layout.addWidget(currency_section)

        file_section = QWidget()
        file_section.setObjectName("section")
        file_layout = QVBoxLayout()
        file_section.setLayout(file_layout)

        self.upload_btn = DropButton('📁 Attach Files Here', self)
        self.upload_btn.setObjectName("upload-button")
        self.upload_btn.clicked.connect(lambda: self.select_file('all'))
        self.upload_btn.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)
        file_layout.addWidget(self.upload_btn)

        main_layout.addWidget(file_section)

        button_layout = QHBoxLayout()
        button_layout.addStretch(1)
        self.process_btn = QPushButton('Start Processing')
        self.process_btn.setEnabled(False)
        self.process_btn.clicked.connect(self.save_rates_and_process)
        button_layout.addWidget(self.process_btn)

        self.reset_btn = QPushButton('Reset')
        self.reset_btn.clicked.connect(self.reset_fields)
        button_layout.addWidget(self.reset_btn)
        button_layout.addStretch(1)
        main_layout.addLayout(button_layout)

    def update_upload_button(self):
        if self.crm_file or self.processor_files:
            num_files = len(self.processor_files) + (1 if self.crm_file else 0)
            self.upload_btn.setText(f"{num_files} Files Were Attached")
            self.upload_btn.setStyleSheet("""
                border: none;
                background: #2c3e50;
                color: #ffffff;
                font-size: 24px;
                font-weight: bold;
                min-height: 250px;
                border-radius: 8px;
            """)
        else:
            self.upload_btn.setText("📁 Attach Files Here")
            self.upload_btn.setStyleSheet("")
        self.check_files_ready()

    def update_calendar_layout(self, year, month):
        """Force layout update when the calendar page changes."""
        self.calendar.updateGeometry()
        self.calendar.adjustSize()

    def on_date_edited(self):
        """Validate and update date on editing finished."""
        text = self.date_lineedit.text()
        date = QDate.fromString(text, "dd/MM/yyyy")
        if date.isValid():
            self.valid_date_str = text
            self.calendar.setSelectedDate(date)
        else:
            self.date_lineedit.setText(self.valid_date_str)
            self.show_warning("Invalid Date", "Invalid date entered. Please use dd/MM/yyyy format and a valid date.")

    def calendar_date_selected(self, date):
        """Handle date selection from calendar popup."""
        self.date_lineedit.setText(date.toString("dd/MM/yyyy"))
        self.valid_date_str = date.toString("dd/MM/yyyy")
        self.calendar.hide()

    def show_calendar_popup(self):
        """Show the calendar popup at the button position."""
        if self.calendar.isVisible():
            self.calendar.hide()
        else:
            button_pos = self.date_button.mapToGlobal(self.date_button.rect().bottomLeft())
            calendar_width = self.calendar.width()
            calendar_height = self.calendar.height()
            screen = QApplication.desktop().screenGeometry()
            x_pos = min(button_pos.x() + 5, screen.width() - calendar_width)
            y_pos = min(button_pos.y(), screen.height() - calendar_height - 10)
            self.calendar.move(x_pos, y_pos)
            self.calendar.show()

    def show_warning(self, title, text):
        msg = QMessageBox(self)
        msg.setIcon(QMessageBox.NoIcon)
        msg.setWindowTitle(title)
        msg.setText(text)
        msg.exec_()

    def show_error(self, title, text):
        msg = QMessageBox(self)
        msg.setIcon(QMessageBox.NoIcon)
        msg.setWindowTitle(title)
        msg.setText(text)
        msg.exec_()

    def show_info(self, title, text):
        msg = QMessageBox(self)
        msg.setIcon(QMessageBox.NoIcon)
        msg.setWindowTitle(title)
        msg.setText(text)
        msg.exec_()

    def update_reciprocal_rates(self):
        for key, (input_field, calc_label) in self.rate_inputs.items():
            from_curr, to_curr = key.split('_')
            rate = float(input_field.text()) if input_field.text() else 0
            if rate > 0:
                calc_label.setText(f"{to_curr}/{from_curr}: {(1 / rate):.4f}")
            else:
                calc_label.setText(f"{to_curr}/{from_curr}: 0.0000")
        self.check_files_ready()

    def select_file(self, file_type):
        file_dialog = QFileDialog()
        if file_type == 'all':
            file_paths, _ = file_dialog.getOpenFileNames(self, "Select Files", "", "CSV Files (*.csv *.xlsx *.xls)")
            if file_paths:
                for source_path in file_paths:
                    file_name = os.path.basename(source_path)
                    if file_name in self.moved_files:
                        self.show_warning("Duplicate", f"{file_name} already selected.")
                        continue
                    if file_name.startswith("crm_"):
                        if self.crm_file:
                            self.show_warning("Duplicate CRM", "CRM file already set. Only one allowed.")
                            continue
                        dest_path = RAW_ATTACHED_FILES / file_name
                        shutil.copy(source_path, str(dest_path))
                        self.crm_file = str(dest_path)
                        self.moved_files.add(file_name)
                    else:
                        dest_path = RAW_ATTACHED_FILES / file_name
                        shutil.copy(source_path, str(dest_path))
                        self.processor_files.append(str(dest_path))
                        self.moved_files.add(file_name)
                self.update_upload_button()

    def check_files_ready(self):
        files_ready = bool(self.crm_file and self.processor_files)
        rates_entered = any(float(input_field.text()) if input_field.text() else 0 > 0
                            for input_field, _ in self.rate_inputs.values())

        self.process_btn.setEnabled(files_ready and rates_entered)

    def is_recognized(self, filename):
        filename_lower = filename.lower()
        if self._detect_processor(filename) != "unknown":
            return True
        for config in PROCESSOR_PATTERNS.values():
            if re.match(config["pattern"], filename_lower):
                return True
        return False

    def save_rates_and_process(self):
        unrecognized = []
        attached_files = []
        if self.crm_file:
            attached_files.append(self.crm_file)
        attached_files.extend(self.processor_files)
        for file_path in attached_files:
            file_name = os.path.basename(file_path)
            if not self.is_recognized(file_name):
                unrecognized.append(file_name)
        if unrecognized:
            if len(unrecognized) == 1:
                msg = f"The file {unrecognized[0]} has not been recognized by the system. Please change its name according to the following format: processor name_YYYY-MM-DD for example: safecharge_2025-10-17."
            else:
                files_list = ", ".join(unrecognized)
                msg = f"The files {files_list} have not been recognized by the system. Please change their names according to the following format: processor name_YYYY-MM-DD for example: safecharge_2025-10-17."
            self.show_warning("Unrecognized Files",
                              msg + "\nResetting the attached window. Please attach again after renaming.")
            self.reset_attachments()
            return

        selected_date = QDate.fromString(self.date_lineedit.text(), "dd/MM/yyyy").toString("yyyy-MM-dd")

        # Cleanup old files for the selected date
        for reg in ['row', 'uk']:
            dirs = setup_dirs_for_reg(reg, create=False)

            # Remove CRM file
            crm_path = dirs['crm_dir'] / f"crm_{selected_date}.xlsx"
            if crm_path.exists():
                crm_path.unlink()

            # Remove processor files
            for f in dirs['processor_dir'].glob(f"*_{selected_date}.*"):
                f.unlink()

            # Remove rates file
            rates_path = dirs['rates_dir'] / f"rates_{selected_date}.csv"
            if rates_path.exists():
                rates_path.unlink()

            # Remove processed CRM dated folders including combined
            combined_crm_dated = dirs['combined_crm_dir'] / selected_date
            if combined_crm_dated.exists():
                shutil.rmtree(combined_crm_dated)
            for proc_dir in dirs['processed_crm_dir'].iterdir():
                if proc_dir.is_dir():
                    dated = proc_dir / selected_date
                    if dated.exists():
                        shutil.rmtree(dated)

            # Remove processed processors dated folders including combined
            for proc_dir in dirs['processed_processor_dir'].iterdir():
                if proc_dir.is_dir():
                    dated = proc_dir / selected_date
                    if dated.exists():
                        shutil.rmtree(dated)

            # Remove lists dated folder
            lists_dated = dirs['lists_dir'] / selected_date
            if lists_dated.exists():
                shutil.rmtree(lists_dated)

            # Clear output dir (already in original code, but ensure)
            output_date_dir = dirs['output_dir'] / selected_date
            if output_date_dir.exists():
                shutil.rmtree(output_date_dir)
                print(f"Cleared output dir for {selected_date} in {reg.upper()}")

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
            file_path = RATES_DIR / f"rates_{selected_date}.csv"
            df.to_csv(file_path, index=False)
            for reg in ['row', 'uk']:
                dirs = setup_dirs_for_reg(reg, create=True)
                reg_rates_path = dirs['rates_dir'] / f"rates_{selected_date}.csv"
                shutil.copy(file_path, reg_rates_path)
            self.hide()
            self.open_second_window()
        else:
            self.show_warning("Error", "No valid rates entered.")

    def reset_attachments(self):
        if self.crm_file and os.path.exists(self.crm_file):
            os.remove(self.crm_file)
        self.crm_file = None

        for file_path in self.processor_files:
            if os.path.exists(file_path):
                os.remove(file_path)
        self.processor_files = []

        for file in RAW_ATTACHED_FILES.glob("*.*"):
            if file.is_file() and file.name != ".gitkeep":
                file.unlink()

        self.moved_files.clear()

        self.update_upload_button()

    def reset_fields(self):
        for _, (input_field, calc_label) in self.rate_inputs.items():
            input_field.clear()
            calc_label.setText("0.0000")

        today = QDate.currentDate()
        yesterday = today.addDays(-1)
        if today.dayOfWeek() == 1:  # Monday (Qt: 1=Mon)
            yesterday = today.addDays(-3)  # Last Friday
        date_str = yesterday.toString("dd/MM/yyyy")
        self.date_lineedit.setText(date_str)
        self.valid_date_str = date_str
        self.calendar.setSelectedDate(yesterday)

        if self.crm_file and os.path.exists(self.crm_file):
            os.remove(self.crm_file)
        self.crm_file = None

        for file_path in self.processor_files:
            if os.path.exists(file_path):
                os.remove(file_path)
        self.processor_files = []

        self.process_btn.setEnabled(False)

        for file in RAW_ATTACHED_FILES.glob("*.*"):
            if file.is_file() and file.name != ".gitkeep":
                file.unlink()

        self.moved_files.clear()

        self.upload_btn.setText("📁 Attach Files Here")
        self.upload_btn.setStyleSheet("")

        self.show_info("Reset", "All fields and attachments have been reset.")

    def open_second_window(self):
        print("Debug: Creating SecondWindow")
        selected_date = QDate.fromString(self.date_lineedit.text(), "dd/MM/yyyy").toString("yyyy-MM-dd")
        self.second_window = SecondWindow(selected_date)
        self.second_window.show()
        print("Debug: SecondWindow shown")
        self.close()


if __name__ == "__main__":
    import sys

    app = QApplication(sys.argv)
    window = ReconciliationWindow()
    window.show()
    sys.exit(app.exec_())