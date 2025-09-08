from PyQt5.QtWidgets import QWidget, QVBoxLayout, QPushButton, QTextEdit, QFileDialog, QMessageBox, QDesktopWidget
from PyQt5.QtCore import QProcess
import os
import shutil
from src.config import OUTPUT_DIR  # Import OUTPUT_DIR from config
import sys

class SecondWindow(QWidget):
    def __init__(self, date_str):
        super().__init__()
        print("Debug: SecondWindow __init__ started")
        self.date_str = date_str
        self.initUI()
        print("Debug: initUI completed")
        self.run_output_script()
        print("Debug: run_output_script called")

    def initUI(self):
        print("Debug: initUI started")
        self.setWindowTitle('Processing Output')
        self.setGeometry(300, 300, 800, 600)  # Adjust size as needed
        qr = self.frameGeometry()
        cp = QDesktopWidget().availableGeometry().center()
        qr.moveCenter(cp)
        self.move(qr.topLeft())

        layout = QVBoxLayout()

        # Export button (initially disabled)
        self.export_btn = QPushButton('Export')
        self.export_btn.setEnabled(False)
        self.export_btn.clicked.connect(self.export_files)
        layout.addWidget(self.export_btn)

        # Console output (QTextEdit)
        self.console = QTextEdit()
        self.console.setReadOnly(True)
        layout.addWidget(self.console)

        self.setLayout(layout)
        print("Debug: initUI finished")

    def run_output_script(self):
        print("Debug: run_output_script started")
        script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src', 'output.py'))
        if not os.path.exists(script_path):
            print(f"Error: output.py not found at {script_path}")
            QMessageBox.critical(self, "Error", f"output.py not found at {script_path}")
            return

        python_executable = sys.executable  # Use current Python (better for venv)
        print(f"Debug: Using Python: {python_executable}, Script: {script_path}, Date: {self.date_str}")

        self.process = QProcess(self)
        self.process.readyReadStandardOutput.connect(self.handle_stdout)
        self.process.readyReadStandardError.connect(self.handle_stderr)
        self.process.finished.connect(self.process_finished)
        self.process.start(python_executable, [script_path, self.date_str])
        print("Debug: QProcess started")

    def handle_stdout(self):
        data = self.process.readAllStandardOutput()
        stdout = bytes(data).decode("utf8")
        self.console.append(stdout)
        print("Debug: STDOUT received")

    def handle_stderr(self):
        data = self.process.readAllStandardError()
        stderr = bytes(data).decode("utf8")
        self.console.append(stderr)
        print("Debug: STDERR received")

    def process_finished(self):
        print("Debug: process_finished called")
        self.console.append("output.py completed.")
        self.export_btn.setEnabled(True)

    def export_files(self):
        print("Debug: export_files started")
        dest_folder = QFileDialog.getExistingDirectory(self, "Select Folder to Export To")
        if dest_folder:
            source_folder = OUTPUT_DIR / self.date_str
            if source_folder.exists():
                for file in source_folder.iterdir():
                    if file.is_file():
                        shutil.copy(str(file), dest_folder)
                QMessageBox.information(self, "Success", f"Files exported to {dest_folder}")
            else:
                QMessageBox.warning(self, "Error", f"No files found in output/{self.date_str}")
        print("Debug: export_files finished")