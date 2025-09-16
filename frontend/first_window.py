import sys
import os
import shutil
from PyQt5.QtWidgets import (QApplication, QWidget, QVBoxLayout, QHBoxLayout, QLabel, QLineEdit,
                             QPushButton, QGridLayout, QFileDialog, QDateEdit, QMessageBox, QCalendarWidget,
                             QToolButton)
from PyQt5.QtCore import Qt, QDate, QMimeData, QProcess, QTimer, QRegExp
from PyQt5.QtGui import QRegExpValidator
from second_window import SecondWindow  # NEW: Import the new second window class
import pandas as pd
import re
from pathlib import Path
from src.config import RATES_DIR, CRM_DIR, PROCESSOR_DIR, RAW_ATTACHED_FILES


class DropButton(QPushButton):
    def __init__(self, text, window, parent=None):
        super().__init__(text, parent)
        self.window = window
        self.setAcceptDrops(True)
        self.setMinimumSize(200, 100)

    def dragEnterEvent(self, event):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()
            self.setStyleSheet("""
                border: 4px dashed #4a90e2;
                background: #e6f0fa;
                min-height: 100px;
                min-width: 200px;
                border-radius: 8px;
            """)
            print("Drag enter accepted")

    def dropEvent(self, event):
        mime_data = event.mimeData()
        if mime_data.hasUrls():
            file_paths = [u.toLocalFile() for u in mime_data.urls()]
            try:
                if self.objectName() == "crm-button":  # CRM button
                    success = False
                    if len(file_paths) == 1:
                        source_path = file_paths[0]
                        file_name = os.path.basename(source_path)
                        if file_name in self.window.moved_files:
                            self.window.show_warning("Duplicate Drop", f"{file_name} already moved.")
                            self.setStyleSheet("")
                            return
                        dest_path = RAW_ATTACHED_FILES / file_name
                        shutil.copy(str(source_path), str(dest_path))
                        self.window.crm_file = str(dest_path)
                        self.setText(f"📊 {file_name}")
                        self.window.moved_files.add(file_name)
                        success = True
                    else:
                        self.window.show_warning("Invalid Drop", "Please drop only one file for CRM.")
                        self.setStyleSheet("")
                    if success:
                        self.setStyleSheet("""
                            border: 4px dashed #003366;
                            background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #e0f7fa, stop:1 #c1e7f0);
                            min-height: 100px;
                            min-width: 200px;
                            border-radius: 8px;
                        """)
                else:  # Processors button
                    new_files = []
                    for source_path in file_paths:
                        file_name = os.path.basename(source_path)
                        if file_name.startswith("crm_"):  # Detect CRM file
                            self.window.show_warning("Invalid Drop", "CRM files should be dropped in the CRM area.")
                            self.setStyleSheet("")
                            continue  # Reject drop for CRM files
                        if file_name in self.window.moved_files:
                            self.window.show_warning("Duplicate Drop", f"{file_name} already moved.")
                            continue
                        dest_path = RAW_ATTACHED_FILES / file_name
                        shutil.copy(str(source_path), str(dest_path))
                        self.window.moved_files.add(file_name)
                        new_files.append(str(dest_path))
                    self.window.processor_files += new_files
                    if new_files:
                        names = [os.path.basename(p) for p in self.window.processor_files]
                        self.setText(f"💳 {', '.join(names)}")
                    if len(self.window.processor_files) > 0:
                        self.setStyleSheet("""
                            border: 4px dashed #006600;
                            background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #e8f5e9, stop:1 #c8e6c9);
                            min-height: 100px;
                            min-width: 200px;
                            border-radius: 8px;
                        """)
                    else:
                        self.setStyleSheet("")
                self.window.check_files_ready()
            except Exception as e:
                print(f"Drop error: {e}")
                self.window.show_error("Error", f"Failed to process drop: {e}")
                self.setStyleSheet("")
        event.accept()

    def _detect_processor(self, filename):
        """Detect processor name from filename based on patterns."""
        filename_lower = filename.lower()
        for processor in ["safecharge", "bitpay", "ezeebill", "paypal", "zotapay", "paymentasia", "powercash",
                          "trustpayments", "paysafe"]:
            if processor in filename_lower:
                return processor
        return "unknown"  # Default if no match

    def dragMoveEvent(self, event):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()

    def dragLeaveEvent(self, event):
        self.setStyleSheet("")
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

    def initUI(self):
        self.setWindowTitle('CRM-Processor Reconciliation System')

        # No custom icon path needed anymore - we're using Unicode/standard styles

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
            #crm-button, #processor-button {
                min-height: 100px;
                min-width: 200px;
                font-size: 14px;
            }
            /* Custom date picker styles */
            #date-lineedit {
                padding: 8px;  /* Match rate input padding for alignment */
                border: 2px solid #dfe6e9;  /* Match rate input border */
                border-radius: 4px 0 0 4px;  /* Slightly rounded to match overall theme */
                font-size: 14px;
                background: #ffffff;  /* Pure white to match rate inputs */
                color: #2c3e50;
                min-width: 80px;
            }
            #date-lineedit:focus {
                border-color: #4a90e2;
                box-shadow: 0 0 5px rgba(74, 144, 226, 0.3);
            }
            #date-button {
                padding: 8px 6px;  /* Adjusted for icon size */
                border: 2px solid #dfe6e9;
                border-left: none;
                border-radius: 0 4px 4px 0;
                background: #ffffff;  /* Pure white to match */
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
                min-width: 270px;
                max-height: 170px;
            }
            QCalendarWidget QAbstractItemView {
                background: #ffffff;
                color: #1e90ff; /* Blue for all numbers */
            }
            QCalendarWidget QToolButton {
                background: #f0f0f0;
                color: #1e90ff; /* Blue for tool buttons */
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

        # Adjust window position and size
        screen = QApplication.desktop().screenGeometry()
        self.setGeometry((screen.width() - 900) // 2, 50, 900, 600)

        # Main layout
        main_layout = QVBoxLayout()
        main_layout.setSpacing(20)
        main_layout.setContentsMargins(20, 20, 20, 20)
        self.setLayout(main_layout)

        # Header
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

        # Currency Section
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

        # Custom Date Picker with Unicode Calendar Icon Button - Centered
        date_container = QWidget()  # NEW: Explicit container layer for the date row
        date_container.setStyleSheet("""
            background: #ffffff;
            padding: 10px;
            border-radius: 4px;
            margin: 5px 0;
        """)

        date_layout = QHBoxLayout()
        date_label = QLabel("Date:")
        date_label.setStyleSheet("font-size: 12px; margin-right: 5px;")  # Slight margin for spacing
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

        # Enable manual editing with format validator
        self.date_lineedit.setReadOnly(False)
        date_regex = QRegExp(r'^\d{1,2}/\d{1,2}/\d{4}$')
        validator = QRegExpValidator(date_regex)
        self.date_lineedit.setValidator(validator)
        self.date_lineedit.editingFinished.connect(self.on_date_edited)

        # Create popup calendar
        self.calendar = QCalendarWidget()
        self.calendar.setGridVisible(True)
        self.calendar.setMinimumSize(270, 150)
        self.calendar.setMaximumHeight(150)
        self.calendar.setSelectedDate(yesterday)
        self.calendar.setVerticalHeaderFormat(QCalendarWidget.NoVerticalHeader)
        self.calendar.setWindowFlags(Qt.Popup | Qt.FramelessWindowHint)
        self.calendar.clicked.connect(self.calendar_date_selected)

        # Calendar popup button with Unicode emoji
        self.date_button = QToolButton()
        self.date_button.setAutoFillBackground(True)
        self.date_button.setObjectName("date-button")
        self.date_button.setText("📅")  # Using Unicode calendar emoji as recommended
        self.date_button.clicked.connect(self.show_calendar_popup)

        date_layout.addWidget(date_label, alignment=Qt.AlignRight)
        date_layout.addWidget(self.date_lineedit)
        date_layout.addWidget(self.date_button)
        date_layout.setSpacing(2)  # Tight spacing for compact look
        date_layout.setContentsMargins(0, 0, 0, 0)

        date_widget = QWidget()
        date_widget.setLayout(date_layout)
        date_container_layout = QVBoxLayout()
        date_container_layout.addWidget(date_widget, alignment=Qt.AlignHCenter)
        date_container_layout.setContentsMargins(0, 0, 0, 0)
        date_container.setLayout(date_container_layout)

        currency_layout.addWidget(date_container, alignment=Qt.AlignHCenter)

        main_layout.addWidget(currency_section)

        # File Upload Section
        file_section = QWidget()
        file_section.setObjectName("section")
        file_layout = QVBoxLayout()
        file_section.setLayout(file_layout)
        file_label = QLabel('📁 Upload Files')
        file_label.setStyleSheet("font-size: 18px; margin-bottom: 10px;")
        file_layout.addWidget(file_label)

        file_grid = QHBoxLayout()
        file_grid.setSpacing(20)
        self.crm_file_btn = DropButton('📊 CRM File', self)
        self.crm_file_btn.setObjectName("crm-button")
        self.crm_file_btn.clicked.connect(lambda: self.select_file('crm'))
        file_grid.addWidget(self.crm_file_btn)

        self.processor_file_btn = DropButton('💳 Processors Files', self)
        self.processor_file_btn.setObjectName("processor-button")
        self.processor_file_btn.clicked.connect(lambda: self.select_file('processor'))
        file_grid.addWidget(self.processor_file_btn)
        file_layout.addLayout(file_grid)

        main_layout.addWidget(file_section)

        # Process and Reset Buttons
        button_layout = QHBoxLayout()
        button_layout.addStretch(1)  # Stretch to center the buttons
        self.process_btn = QPushButton('Start Processing')
        self.process_btn.setEnabled(False)
        self.process_btn.clicked.connect(self.save_rates_and_process)
        button_layout.addWidget(self.process_btn)

        self.reset_btn = QPushButton('Reset')
        self.reset_btn.clicked.connect(self.reset_fields)
        button_layout.addWidget(self.reset_btn)
        button_layout.addStretch(1)  # Stretch to center the buttons
        main_layout.addLayout(button_layout)

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
            # Position the calendar below the button, shifted right by 5 pixels
            button_pos = self.date_button.mapToGlobal(self.date_button.rect().bottomLeft())
            self.calendar.move(button_pos.x() + 5, button_pos.y())
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
        if file_type == 'crm':
            file_path, _ = file_dialog.getOpenFileName(self, "Select CRM File", "", "CSV Files (*.csv *.xlsx *.xls)")
            if file_path:
                # To make consistent with drop, copy the file
                file_name = os.path.basename(file_path)
                if file_name in self.moved_files:
                    self.show_warning("Duplicate", f"{file_name} already selected.")
                    return
                dest_path = RAW_ATTACHED_FILES / file_name
                shutil.copy(file_path, str(dest_path))
                self.crm_file = str(dest_path)
                self.crm_file_btn.setText(f"📊 {file_name}")
                self.moved_files.add(file_name)
                self.crm_file_btn.setStyleSheet("""
                    border: 4px dashed #003366;
                    background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #e0f7fa, stop:1 #c1e7f0);
                    min-height: 100px;
                    min-width: 200px;
                    border-radius: 8px;
                """)
        else:
            file_paths, _ = file_dialog.getOpenFileNames(self, "Select Processors Files", "",
                                                         "CSV Files (*.csv *.xlsx *.xls)")
            if file_paths:
                new_files = []
                for source_path in file_paths:
                    file_name = os.path.basename(source_path)
                    if file_name.startswith("crm_"):
                        self.show_warning("Invalid File", "CRM files should be selected in CRM area.")
                        continue
                    if file_name in self.moved_files:
                        self.show_warning("Duplicate", f"{file_name} already selected.")
                        continue
                    dest_path = RAW_ATTACHED_FILES / file_name
                    shutil.copy(source_path, str(dest_path))
                    self.moved_files.add(file_name)
                    new_files.append(str(dest_path))
                self.processor_files += new_files
                if new_files:
                    names = [os.path.basename(p) for p in self.processor_files]
                    self.processor_file_btn.setText(f"💳 {', '.join(names)}")
                    self.processor_file_btn.setStyleSheet("""
                        border: 4px dashed #006600;
                        background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #e8f5e9, stop:1 #c8e6c9);
                        min-height: 100px;
                        min-width: 200px;
                        border-radius: 8px;
                    """)
        self.check_files_ready()

    def check_files_ready(self):
        files_ready = bool(self.crm_file and self.processor_files)
        rates_entered = any(float(input_field.text()) if input_field.text() else 0 > 0
                            for input_field, _ in self.rate_inputs.values())

        self.process_btn.setEnabled(files_ready and rates_entered)

    def save_rates_and_process(self):
        selected_date = QDate.fromString(self.date_lineedit.text(), "dd/MM/yyyy").toString("yyyy-MM-dd")
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
            # Removed: self.show_info("Success", f"Rates saved to {file_path}")

            # NEW: Hide and open second window instead of running reports_creator
            self.hide()
            self.open_second_window()
        else:
            self.show_warning("Error", "No valid rates entered.")

    def rename_processor_files(self, file_paths):
        selected_date = QDate.fromString(self.date_lineedit.text(), "dd/MM/yyyy").toString("yyyy-MM-dd")
        for source_path in file_paths:
            file_name = os.path.basename(source_path)
            dest_path = PROCESSOR_DIR / file_name
            if not re.match(r"^[a-zA-Z]+_\d{4}-\d{2}-\d{2}\.(csv|xlsx|xls)$", file_name):
                processor = self.crm_file_btn._detect_processor(file_name)
                new_name = f"{processor}_{selected_date}{Path(source_path).suffix}"
                dest_path = PROCESSOR_DIR / new_name
            shutil.move(str(source_path), str(dest_path))
        self.processor_files = [str(PROCESSOR_DIR / n) for n in [os.path.basename(p) for p in file_paths]]

    def reset_fields(self):
        # Reset exchange rates
        for _, (input_field, calc_label) in self.rate_inputs.items():
            input_field.clear()
            calc_label.setText("0.0000")

        # Reset date
        today = QDate.currentDate()
        yesterday = today.addDays(-1)
        if today.dayOfWeek() == 1:  # Monday (Qt: 1=Mon)
            yesterday = today.addDays(-3)  # Last Friday
        date_str = yesterday.toString("dd/MM/yyyy")
        self.date_lineedit.setText(date_str)
        self.valid_date_str = date_str

        # Reset attached files and delete from directories
        if self.crm_file and os.path.exists(self.crm_file):
            os.remove(self.crm_file)
        self.crm_file = None
        self.crm_file_btn.setText("📊 CRM File")
        self.crm_file_btn.setStyleSheet("")  # Reset button style to remove hover look

        for file_path in self.processor_files:
            if os.path.exists(file_path):
                os.remove(file_path)
        self.processor_files = []
        self.processor_file_btn.setText("💳 Processors Files")
        self.processor_file_btn.setStyleSheet("")  # Reset button style to remove hover look

        # Disable process button
        self.process_btn.setEnabled(False)

        # Optional: Clear RAW_ATTACHED_FILES if needed
        for file in RAW_ATTACHED_FILES.glob("*.*"):
            if file.is_file() and file.name != ".gitkeep":
                file.unlink()

        self.moved_files.clear()

        self.show_info("Reset", "All fields and attachments have been reset.")

    def open_second_window(self):
        print("Debug: Creating SecondWindow")
        selected_date = QDate.fromString(self.date_lineedit.text(), "dd/MM/yyyy").toString("yyyy-MM-dd")
        self.second_window = SecondWindow(selected_date)
        self.second_window.show()
        print("Debug: SecondWindow shown")
        self.close()  # Close first window