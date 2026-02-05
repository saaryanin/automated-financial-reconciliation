\# Reconciliation System

\## Overview

This program automates the reconciliation process between a CRM file containing daily transactions for company clients across regions and multiple processor files that reflect the actual executed transactions. It identifies discrepancies such as successful executions, underpays, overpays, unapproved deposits, and other anomalies. The output includes Excel files that facilitate manual investigation of unusual cases, tracking of held funds that may need to be returned to clients, and monitoring of other irregularities. This tool streamlines what was previously a manual process, improving efficiency and accuracy in financial reconciliation.

The system supports separation between regions: a "ROW" (Rest of World) folder for the original implementation and a "UK" folder for the UK-specific regulation, which includes an additional processor (Barclaycard) while maintaining similar logic to the processors in ROW. All temporary files, inputs, and outputs are organized within these regional folders to ensure full separation.

\## Requirements

\- Python 3.x

To set up the environment and install dependencies:

1\. Create a virtual environment (recommended to isolate dependencies):

&nbsp;  python -m venv venv

2\. Activate the virtual environment:

&nbsp;  - On Unix/macOS: `source venv/bin/activate`

&nbsp;  - On Windows: `venv\\Scripts\\activate`

3\. Install dependencies:

&nbsp; 

&nbsp;  pip install -r requirements.txt

&nbsp; 

&nbsp;  (The `requirements.txt` file is located in the root folder.)

&nbsp; 

&nbsp;  \*Note: To deactivate the virtual environment after use, run `deactivate`.\*

\## How to Run



\### Option 1: Run via Python

1\. Ensure all dependencies are installed (see Requirements above).

2\. Execute the main entry point:

&nbsp; 

&nbsp;  python main.py

&nbsp; 

&nbsp;  This launches the GUI workflow, starting with `first\_window.py` and progressing through the subsequent frontend scripts.

\### Option 2: Build and Run as Executable

1\. Build the executable using the provided spec file:

&nbsp; 

&nbsp;  pyinstaller ReconciliationSystem.spec

&nbsp; 

&nbsp;  This generates a `dist` folder containing the executable and a `build` folder with build artifacts.

2\. Run the executable from the `dist` folder.

\## Project Structure and Script Overview

The project is divided into backend scripts (core logic for processing and reconciliation) and frontend scripts (GUI components for user interaction). Below is a high-level overview of each script's purpose. For detailed explanations, including implementation details, edge cases handled, and code structure, refer to the docstring comments at the top of each script file. These comments provide in-depth insights for code reviewers, making it easier to understand the logic without disrupting the README's brevity.

\### Backend Scripts

\- \*\*files\_renamer.py\*\*: Handles renaming of raw processor files and incorporates manually adjusted processor and CRM files. Ensures consistent naming conventions for downstream processing.

\- \*\*preprocess.py\*\*: Filters and combines all input files (CRM and processors) into unified datasets, preparing them for matching.

\- \*\*withdrawals\_matcher.py\*\*: Performs matching logic on combined withdrawals data. This is the core engine behind generating `withdrawals\_matching.xlsx`.

\- \*\*deposits\_matcher.py\*\*: Performs matching logic on combined deposits data. This is the core engine behind generating `deposits\_matching.xlsx`.

\- \*\*reports\_creator.py\*\*: Generates key output files including `withdrawals\_matching.xlsx`, `deposits\_matching.xlsx`, and `unmatched\_shifted\_deposits.xlsx`. Incorporates date variables and rates from the frontend.

\- \*\*output.py\*\*: Processes the matched files to create final reports such as `total\_shifts\_by\_currency.xlsx`, `Unmatched CRM Deposits.xlsx`, `Unmatched CRM Withdrawals.xlsx`, `Unmatched Processors Deposits.xlsx`, `Unmatched Processors Withdrawals.xlsx`, `warnings\_withdrawals.xlsx`, and `withdrawals\_matching\_updated.xlsx`.

\- \*\*config.py\*\*: Establishes base and temporary directory structures for the application, supporting both development and frozen environments, and provides functions for regulation-specific directory paths.

\- \*\*cross\_regulation\_matcher.py\*\*: Conducts cross-regulation matching for unmatched withdrawals between ROW and UK datasets, removes matched entries from original files, and saves results to regulation-specific cross-match files.

\- \*\*shifts\_handler.py\*\*: Processes unmatched shifted deposits after a specified cutoff time for ROW and UK, calculates matched sums by currency, saves unmatched shifted rows in raw CRM format, and updates matching files.

\- \*\*utils.py\*\*: Provides helper functions for data cleaning, normalization, logging setup, file handling, date manipulation, holiday fetching, and regulation categorization used across the project.

\### Frontend Scripts

\- \*\*main.py\*\*: The primary entry point that initializes the application and launches `first\_window.py`. Manages the overall flow between frontend components.

\- \*\*first\_window.py\*\*: Displays the initial GUI window. Responsible for creating the rates file and setting the `DATE` variable used in `reports\_creator.py` and `output.py`.

\- \*\*second\_window.py\*\*: Shows a progress bar tracking the execution of `reports\_creator.py` and parts of `output.py`.

\- \*\*third\_window.py\*\*: Provides an editable interface displaying rows from `warnings\_withdrawals.xlsx`, using data from `withdrawals\_matching\_updated.xlsx`. Unselected rows are moved to the end of unmatched withdrawals files.

\- \*\*fourth\_window.py\*\*: Includes an export button to save final output files (`total\_shifts\_by\_currency.xlsx`, unmatched deposits/withdrawals, etc.) to a user-selected location. Also displays the table from `total\_shifts\_by\_currency.xlsx`.

