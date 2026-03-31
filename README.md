# Reconciliation System
## Overview
This program automates the reconciliation process between a CRM file containing daily transactions for company clients across regions and multiple processor files that reflect the actual executed transactions. It identifies discrepancies such as successful executions, underpays, overpays, unapproved deposits, and other anomalies. The output includes Excel files that facilitate manual investigation of unusual cases, tracking of held funds that may need to be returned to clients, and monitoring of other irregularities. This tool streamlines what was previously a manual process, improving efficiency and accuracy in financial reconciliation.
The system supports separation between regions: a "ROW" (Rest of World) folder for the original implementation and a "UK" folder for the UK-specific regulation, which includes an additional processor (Barclaycard) while maintaining similar logic to the processors in ROW. All temporary files, inputs, and outputs are organized within these regional folders to ensure full separation.
## Requirements
- Python 3.x
To set up the environment and install dependencies:
1. Create a virtual environment (recommended to isolate dependencies):
   python -m venv venv
2. Activate the virtual environment:
   - On Unix/macOS: `source venv/bin/activate`
   - On Windows: `venv\Scripts\activate`
3. Install dependencies:
  
   pip install -r requirements.txt
  
   (The `requirements.txt` file is located in the root folder.)
  
   *Note: To deactivate the virtual environment after use, run `deactivate`.*
## How to Run
### Option 1: Run via Python
1. Ensure all dependencies are installed (see Requirements above).
2. Execute the main entry point:
  
   python main.py
  
   This launches the GUI workflow, starting with `first_window.py` and progressing through the subsequent frontend scripts.
### Option 2: Build and Run as Executable
1. Build the executable using the provided spec file:
  
   pyinstaller ReconciliationSystem.spec
  
   This generates a `dist` folder containing the executable and a `build` folder with build artifacts.
2. Run the executable from the `dist` folder.
## Project Structure and Script Overview
The project is divided into backend scripts (core logic for processing and reconciliation) and frontend scripts (GUI components for user interaction). Below is a high-level overview of each script's purpose. For detailed explanations, including implementation details, edge cases handled, and code structure, refer to the docstring comments at the top of each script file. These comments provide in-depth insights for code reviewers, making it easier to understand the logic without disrupting the README's brevity.
### Backend Scripts
- **files_renamer.py**: Handles renaming of raw processor files and incorporates manually adjusted processor and CRM files. Ensures consistent naming conventions for downstream processing.
- **preprocess.py**: Filters and combines all input files (CRM and processors) into unified datasets, preparing them for matching.
- **withdrawals_matcher.py**: Performs matching logic on combined withdrawals data. This is the core engine behind generating `withdrawals_matching.xlsx`.
- **deposits_matcher.py**: Performs matching logic on combined deposits data. This is the core engine behind generating `deposits_matching.xlsx`.
- **reports_creator.py**: Generates key output files including `withdrawals_matching.xlsx`, `deposits_matching.xlsx`, and `unmatched_shifted_deposits.xlsx`. Incorporates date variables and rates from the frontend.
- **output.py**: Processes the matched files to create final reports such as `total_shifts_by_currency.xlsx`, `Unmatched CRM Deposits.xlsx`, `Unmatched CRM Withdrawals.xlsx`, `Unmatched Processors Deposits.xlsx`, `Unmatched Processors Withdrawals.xlsx`, `warnings_withdrawals.xlsx`, and `withdrawals_matching_updated.xlsx`.
- **config.py**: Establishes base and temporary directory structures for the application, supporting both development and frozen environments, and provides functions for regulation-specific directory paths.
- **cross_regulation_matcher.py**: Conducts cross-regulation matching for unmatched withdrawals between ROW and UK datasets, removes matched entries from original files, and saves results to regulation-specific cross-match files.
- **shifts_handler.py**: Processes unmatched shifted deposits after a specified cutoff time for ROW and UK, calculates matched sums by currency, saves unmatched shifted rows in raw CRM format, and updates matching files.
- **utils.py**: Provides helper functions for data cleaning, normalization, logging setup, file handling, date manipulation, holiday fetching, and regulation categorization used across the project.
### Frontend Scripts
- **main.py**: The primary entry point that initializes the application and launches `first_window.py`. Manages the overall flow between frontend components.
- **first_window.py**: Displays the initial GUI window. Responsible for creating the rates file and setting the `DATE` variable used in `reports_creator.py` and `output.py`.
- **second_window.py**: Shows a progress bar tracking the execution of `reports_creator.py` and parts of `output.py`.
- **third_window.py**: Provides an editable interface displaying rows from `warnings_withdrawals.xlsx`, using data from `withdrawals_matching_updated.xlsx`. Unselected rows are moved to the end of unmatched withdrawals files.
- **fourth_window.py**: Includes an export button to save final output files (`total_shifts_by_currency.xlsx`, unmatched deposits/withdrawals, etc.) to a user-selected location. Also displays the table from `total_shifts_by_currency.xlsx`.