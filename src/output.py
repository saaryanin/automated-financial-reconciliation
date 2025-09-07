# src/output.py

import sys
import pandas as pd
from pathlib import Path
from src.config import OUTPUT_DIR
from src.shifts_handler import main as handle_shifts

if __name__ == "__main__":
    DATE = sys.argv[1] if len(sys.argv) > 1 else "2025-09-07"  # Default date for testing; use command-line arg in production
    matched_sums = handle_shifts(DATE)
    if matched_sums:
        output_dir = OUTPUT_DIR / DATE
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "total_shifts_by_currency.csv"
        df = pd.DataFrame([matched_sums])
        df.to_csv(output_path, index=False)
        print(f"Total shifts by currency saved to {output_path}")