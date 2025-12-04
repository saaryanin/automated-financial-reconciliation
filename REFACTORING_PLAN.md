# Source Code Refactoring Plan

## Overview
This document outlines the comprehensive refactoring plan for all files in the `src/` folder to improve readability, maintainability, and reduce code duplication while maintaining 100% functionality.

## ✅ Completed Refactorings

### 1. config.py (40 lines → 69 lines)
**Improvements:**
- ✅ Added environment variable support (`RECONCILIATION_TEMP_DIR`)
- ✅ Extracted directory creation into `ensure_directories()` function
- ✅ Added type hints and comprehensive docstrings
- ✅ Created helper functions `_get_base_dir()` and `_get_temp_dir()`
- ✅ Better organization and code clarity

### 2. utils.py (185 lines → 280 lines)
**Improvements:**
- ✅ Added comprehensive type hints throughout
- ✅ Created `CancelledRow` dataclass replacing hardcoded dictionary
- ✅ Extracted all constants (CURRENCY_NORMALIZATION, REGULATION_MAPPING, etc.)
- ✅ Organized into logical sections with clear headers
- ✅ Improved all function documentation
- ✅ Made cache validity configurable

---

## 📋 Pending Refactorings

### 3. shifts_handler.py (125 lines) - MEDIUM PRIORITY

**Current Issues:**
- Magic numbers for cutoff hours (21, 22)
- Complex DST calculation inline
- `save_unmatched_shifted()` does too many things (50 lines, 6 responsibilities)
- No type hints

**Refactoring Plan:**
```python
# Extract constants
BST_CUTOFF_HOUR = 21  # 9 PM during British Summer Time
GMT_CUTOFF_HOUR = 22  # 10 PM during Greenwich Mean Time
DST_START_MONTH = 3   # March
DST_END_MONTH = 10    # October

# Break down save_unmatched_shifted into:
def filter_unmatched_deposits(shifted_df: pd.DataFrame) -> pd.DataFrame:
    """Filter for unmatched deposits only."""

def extract_transaction_ids(df: pd.DataFrame) -> List[str]:
    """Extract valid transaction IDs from dataframe."""

def load_and_filter_crm(date_str: str, transaction_ids: List[str]) -> pd.DataFrame:
    """Load CRM file and filter for matching transaction IDs."""

def remove_unwanted_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Remove system columns from dataframe."""

def save_unmatched_shifted(shifted_df: pd.DataFrame, date_str: str) -> None:
    """Orchestrate saving unmatched shifted deposits."""
    unmatched = filter_unmatched_deposits(shifted_df)
    ids = extract_transaction_ids(unmatched)
    crm_rows = load_and_filter_crm(date_str, ids)
    cleaned = remove_unwanted_columns(crm_rows)
    # Save logic
```

**Expected Result:** ~140 lines, much more readable

---

### 4. deposits_matcher.py (77 lines) - LOW PRIORITY

**Current Issues:**
- Appears to be stubbed/incomplete
- Minimal documentation

**Refactoring Plan:**
- Add comprehensive docstrings
- Add type hints
- Clarify the purpose and implementation status
- If incomplete, add TODO comments

**Expected Result:** ~85 lines with better documentation

---

### 5. files_renamer.py (411 lines) - HIGH PRIORITY

**Current Issues:**
- Large nested PROCESSOR_PATTERNS dictionary (100+ lines)
- Complex validation logic in `validate_and_rename()`
- Poor variable naming (`df`, `proc` inconsistency)
- Regex patterns hard to test

**Refactoring Plan:**

**Step 1:** Extract processor patterns to separate config file:
```python
# src/processor_patterns.json
{
  "safecharge": {
    "pattern": "126728__transaction-search_[0-9]+_[a-z0-9]+(?i:\\.csv|\\.xlsx|\\.xls)",
    "date_column": "Date",
    "header_row": 11
  },
  // ... etc
}
```

**Step 2:** Create processor pattern validator class:
```python
@dataclass
class ProcessorPattern:
    """Configuration for a processor file pattern."""
    pattern: str
    date_format: Optional[str] = None
    date_column: Optional[str] = None
    header_row: int = 0
    dest_dir: Optional[Path] = None

class ProcessorMatcher:
    """Handles processor file pattern matching and renaming."""
    def __init__(self, patterns_file: Path):
        self.patterns = self._load_patterns(patterns_file)

    def match_processor(self, filename: str) -> Optional[ProcessorPattern]:
        """Match filename to processor pattern."""

    def extract_date(self, file_path: Path, pattern: ProcessorPattern) -> Optional[str]:
        """Extract date from file using pattern configuration."""

    def rename_and_move(self, file_path: Path) -> bool:
        """Rename file and move to appropriate directory."""
```

**Step 3:** Simplify main logic:
```python
def run_renamer(incoming_dir: Path = INCOMING_DIR, forced_date: str = None) -> int:
    """Scan and rename files in incoming directory."""
    matcher = ProcessorMatcher(PATTERNS_FILE)
    renamed_count = 0

    for file in incoming_dir.glob("*.[csv|xlsx|xls]"):
        if matcher.rename_and_move(file):
            renamed_count += 1

    return renamed_count
```

**Expected Result:** ~250 lines (main code) + JSON config file

---

### 6. output.py (1,091 lines) - CRITICAL PRIORITY

**Current Issues:**
- 8+ nearly identical report generation functions
- Massive code duplication (same column lists repeated 5+ times)
- Inconsistent error handling
- 500+ lines could be eliminated

**Refactoring Plan:**

**Step 1:** Create base report generator class:
```python
@dataclass
class ReportConfig:
    """Configuration for generating a report."""
    source_file: str
    output_name: str
    filters: Dict[str, Any]
    column_mapping: Dict[str, str]
    text_columns: List[str]
    sort_by: str
    sort_ascending: bool = False

class ReportGenerator:
    """Generic report generator with configurable behavior."""

    def __init__(self, date_str: str, config: ReportConfig):
        self.date_str = date_str
        self.config = config

    def load_data(self) -> pd.DataFrame:
        """Load source data with error handling."""

    def apply_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply configured filters."""

    def apply_cutoff(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply date cutoff if needed."""

    def select_and_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Select and rename columns per config."""

    def sort_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Sort data per config."""

    def save_report(self, df: pd.DataFrame) -> Path:
        """Save to Excel with formatting."""

    def generate(self) -> Optional[pd.DataFrame]:
        """Main generation pipeline."""
        df = self.load_data()
        if df is None:
            return None
        df = self.apply_filters(df)
        df = self.apply_cutoff(df)
        df = self.select_and_rename_columns(df)
        df = self.sort_data(df)
        self.save_report(df)
        return df
```

**Step 2:** Define report configurations:
```python
REPORT_CONFIGS = {
    'unmatched_crm_deposits': ReportConfig(
        source_file='deposits_matching.xlsx',
        output_name='unmatched_crm_deposits.xlsx',
        filters={'match_status': 0, 'proc_date_is_na': True},
        column_mapping=DEPOSIT_COLUMN_MAPPING,
        text_columns=['Last 4 Digits', 'Transaction ID'],
        sort_by='crm_date',
        sort_ascending=False
    ),
    # ... define all 8 reports here
}
```

**Step 3:** Simplify generation functions:
```python
def generate_unmatched_crm_deposits(date_str: str) -> Optional[pd.DataFrame]:
    """Generate unmatched CRM deposits report."""
    config = REPORT_CONFIGS['unmatched_crm_deposits']
    generator = ReportGenerator(date_str, config)
    return generator.generate()
```

**Expected Result:** ~400 lines (60% reduction)

---

### 7. preprocess.py (1,590 lines) - CRITICAL PRIORITY

**Current Issues:**
- 600+ lines of duplicated code (deposits vs withdrawals)
- `standardize_processor_columns_deposits()`: 206 lines with 11 processor branches
- `standardize_crm_columns()`: ~400 lines handling both transaction types
- Massive code duplication

**Refactoring Plan:**

**Step 1:** Create processor standardizer base class:
```python
class ProcessorStandardizer(ABC):
    """Base class for processor-specific standardization."""

    @abstractmethod
    def standardize_deposits(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize deposit records."""

    @abstractmethod
    def standardize_withdrawals(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize withdrawal records."""

    def _extract_last4(self, df: pd.DataFrame, column: str) -> pd.Series:
        """Common logic for extracting last 4 digits."""

    def _combine_date_time(self, df: pd.DataFrame, date_col: str, time_col: str) -> pd.Series:
        """Common logic for combining date/time columns."""
```

**Step 2:** Create processor-specific standardizers:
```python
# src/processors/paypal_standardizer.py
class PayPalStandardizer(ProcessorStandardizer):
    def standardize_deposits(self, df: pd.DataFrame) -> pd.DataFrame:
        # Only PayPal-specific deposit logic

    def standardize_withdrawals(self, df: pd.DataFrame) -> pd.DataFrame:
        # Only PayPal-specific withdrawal logic

# src/processors/safecharge_standardizer.py
class SafeChargeStandardizer(ProcessorStandardizer):
    # ... etc

# Create for all 11 processors
```

**Step 3:** Create standardizer registry:
```python
class StandardizerRegistry:
    """Registry of processor standardizers."""

    _standardizers = {
        'paypal': PayPalStandardizer(),
        'safecharge': SafeChargeStandardizer(),
        # ... register all 11
    }

    @classmethod
    def get(cls, processor: str) -> ProcessorStandardizer:
        return cls._standardizers.get(processor.lower())
```

**Step 4:** Simplify main functions:
```python
def standardize_processor_columns(
    df: pd.DataFrame,
    processor: str,
    transaction_type: str
) -> pd.DataFrame:
    """Standardize processor columns - unified for deposits and withdrawals."""
    standardizer = StandardizerRegistry.get(processor)
    if standardizer is None:
        return pd.DataFrame()

    if transaction_type == 'deposit':
        return standardizer.standardize_deposits(df)
    else:
        return standardizer.standardize_withdrawals(df)
```

**File Structure:**
```
src/
  processors/
    __init__.py
    base.py (ProcessorStandardizer, StandardizerRegistry)
    paypal.py
    safecharge.py
    powercash.py
    shift4.py
    skrill.py
    neteller.py
    trustpayments.py
    zotapay.py
    paymentasia.py
    bitpay.py
    ezeebill.py
  preprocess.py (main orchestration, ~400 lines)
```

**Expected Result:** ~900 total lines (40% reduction), much better organized

---

### 8. withdrawals_matcher.py (1,934 lines) - CRITICAL PRIORITY

**Current Issues:**
- Monolithic 1,934-line file
- `ReconciliationEngine` class: 700+ lines (god object)
- `_flag_warning()`: 180+ lines with 6 levels of nesting
- `_match_processor_to_crm_row()`: 187 lines
- Magic numbers everywhere

**Refactoring Plan:**

**Step 1:** Extract configuration to constants file:
```python
# src/matching/config.py
from dataclasses import dataclass

@dataclass
class ProcessorMatchConfig:
    """Configuration for processor-specific matching rules."""
    email_threshold: float = 0.0
    name_match_threshold: float = 0.0
    require_last4: bool = True
    require_email: bool = True
    enable_name_fallback: bool = True
    enable_exact_match: bool = True
    tolerance: float = 0.0
    matching_logic: str = "standard"
    allow_last4_only_if_email_blank: bool = False

PROCESSOR_CONFIGS = {
    'safecharge': ProcessorMatchConfig(
        email_threshold=0.6,
        require_last4=True,
        require_email=True,
        tolerance=0.1,
        allow_last4_only_if_email_blank=True
    ),
    # ... all other processors
}
```

**Step 2:** Extract warning validators:
```python
# src/matching/warning_validators.py
class WarningValidator:
    """Base class for warning validation rules."""

    @abstractmethod
    def validate(self, matches: List[Dict], processor_df: pd.DataFrame) -> None:
        """Apply validation rule and flag warnings."""

class EmailSimilarityValidator(WarningValidator):
    """Rule 1: Flag high email similarity in unmatched rows."""
    def validate(self, matches, processor_df):
        # Extracted from _flag_warning lines 537-583

class Last4MatchValidator(WarningValidator):
    """Rule 2: Flag last4 digit matches."""
    def validate(self, matches, processor_df):
        # Extracted from _flag_warning lines 584-635

class CrossProcessorValidator(WarningValidator):
    """Rule 3: Flag cross-processor matches."""
    def validate(self, matches, processor_df):
        # Extracted from _flag_warning lines 637-652

class Shift4PartialEmailValidator(WarningValidator):
    """Rule 4: Flag Shift4 partial email matches."""
    def validate(self, matches, processor_df):
        # Extracted from _flag_warning lines 653-699

class WarningFlagEngine:
    """Coordinates all warning validation rules."""
    def __init__(self):
        self.validators = [
            EmailSimilarityValidator(),
            Last4MatchValidator(),
            CrossProcessorValidator(),
            Shift4PartialEmailValidator()
        ]

    def flag_warnings(self, matches, processor_df):
        for validator in self.validators:
            validator.validate(matches, processor_df)
```

**Step 3:** Extract matching strategies:
```python
# src/matching/strategies.py
class MatchingStrategy(ABC):
    """Base class for processor-specific matching logic."""

    @abstractmethod
    def match(self, crm_row, proc_dict, last4_map, used, config) -> Optional[Tuple]:
        """Match CRM row to processor rows."""

class StandardMatcher(MatchingStrategy):
    """Standard matching for SafeCharge, PowerCash."""

class PayPalMatcher(MatchingStrategy):
    """PayPal-specific matching logic."""

class Shift4Matcher(MatchingStrategy):
    """Shift4-specific matching logic."""

# ... etc for all processors
```

**Step 4:** Simplify ReconciliationEngine:
```python
# src/matching/engine.py
class ReconciliationEngine:
    """Main reconciliation engine - coordination only."""

    def __init__(self, exchange_rate_map, config=None):
        self.exchange_rate_map = exchange_rate_map
        self.config = self._init_config(config)
        self.warning_engine = WarningFlagEngine()
        self.matcher_registry = MatcherRegistry()

    def match_withdrawals(self, crm_df, processor_df):
        """Main matching pipeline."""
        matches = self._perform_matching(crm_df, processor_df)
        self._add_unmatched_rows(matches, crm_df, processor_df)
        self._add_cancelled_rows(matches, crm_df)
        self.warning_engine.flag_warnings(matches, processor_df)
        return matches
```

**File Structure:**
```
src/
  matching/
    __init__.py
    config.py (ProcessorMatchConfig, PROCESSOR_CONFIGS)
    engine.py (ReconciliationEngine, ~200 lines)
    strategies.py (All matching strategies, ~400 lines)
    warning_validators.py (All warning validators, ~300 lines)
    utils.py (Helper functions, ~200 lines)
  withdrawals_matcher.py (backwards compatibility wrapper, ~50 lines)
```

**Expected Result:** ~1,150 total lines (40% reduction), highly modular

---

### 9. reports_creator.py (383 lines) - HIGH PRIORITY

**Current Issues:**
- `main()` function: 360 lines doing everything
- Deeply nested logic (5 levels)
- Hardcoded processor list
- Manual directory cleanup

**Refactoring Plan:**

**Step 1:** Extract directory management:
```python
def setup_directories(date: str) -> Path:
    """Setup and clean directories for the given date."""
    clear_data_directories(preserve=[LISTS_DIR, RATES_DIR])
    report_dir = LISTS_DIR / date
    if report_dir.exists():
        shutil.rmtree(report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    return report_dir

def clear_data_directories(preserve: List[Path]) -> None:
    """Clear DATA_DIR contents while preserving specified directories."""
    for item in DATA_DIR.iterdir():
        if item in preserve:
            continue
        if item.is_dir():
            for child in item.iterdir():
                shutil.rmtree(child) if child.is_dir() else child.unlink()
```

**Step 2:** Extract deposits processing:
```python
def process_deposits(date: str, processors: List[str]) -> Path:
    """Process all deposits and generate matching report."""
    # Preprocess processor files
    _preprocess_processor_deposits(processors, date)

    # Preprocess CRM files
    _preprocess_crm_deposits(processors, date)

    # Combine processed files
    combine_processed_files(date, processors, 'deposit', {})

    # Append unmatched shifted
    append_unmatched_to_combined(date)

    # Generate matching report
    report_path = _generate_deposits_matching_report(date)

    # Handle shifts
    handle_shifts(date)

    return report_path
```

**Step 3:** Extract withdrawals processing:
```python
def process_withdrawals(date: str, processors: List[str], exchange_rates: Dict) -> Path:
    """Process all withdrawals and generate matching report."""
    # Preprocess files
    _preprocess_withdrawals(processors, date)

    # Combine files
    _combine_zotapay_paymentasia(date)
    combine_processed_files(date, processors + ['zotapay_paymentasia'], 'withdrawal', exchange_rates)

    # Run matching
    report_path = _run_withdrawals_matching(date, exchange_rates)

    return report_path
```

**Step 4:** Simplify main:
```python
def main(date: str = None) -> None:
    """Main entry point for report creation."""
    start_time = time.time()

    # Setup
    date = date or sys.argv[1] if len(sys.argv) > 1 else "2025-03-24"
    processors = PROCESSORS  # from config file
    report_dir = setup_directories(date)

    # Rename files
    run_renamer(forced_date=date)

    # Process deposits
    deposits_report = process_deposits(date, processors)
    logging.info(f"Deposits report: {deposits_report}")

    # Load exchange rates
    exchange_rates = load_exchange_rates(date)

    # Process withdrawals
    withdrawals_report = process_withdrawals(date, processors, exchange_rates)
    logging.info(f"Withdrawals report: {withdrawals_report}")

    # Summary
    elapsed = time.time() - start_time
    logging.info(f"Total time: {elapsed:.2f} seconds")
```

**Expected Result:** ~200 lines (50% reduction), much clearer

---

## Summary of Expected Improvements

| File | Current Lines | Expected Lines | Reduction | Priority |
|------|--------------|----------------|-----------|----------|
| config.py | 40 | 69 | ✅ Done | Complete |
| utils.py | 185 | 280 | ✅ Done | Complete |
| shifts_handler.py | 125 | 140 | -12% | Medium |
| deposits_matcher.py | 77 | 85 | -10% | Low |
| files_renamer.py | 411 | 250 | 39% | High |
| output.py | 1,091 | 400 | 63% | Critical |
| preprocess.py | 1,590 | 900 | 43% | Critical |
| withdrawals_matcher.py | 1,934 | 1,150 | 41% | Critical |
| reports_creator.py | 383 | 200 | 48% | High |
| **Total** | **5,836** | **3,474** | **40%** | - |

## Key Principles Applied

1. **Extract Constants**: Move magic numbers and hardcoded values to named constants
2. **Extract Functions**: Break large functions into focused, single-purpose functions
3. **Extract Classes**: Use classes to encapsulate related behavior
4. **Use Dataclasses**: Replace dictionaries with typed dataclasses
5. **Strategy Pattern**: Use for processor-specific logic
6. **Template Method**: Use for common workflows with variations
7. **Type Hints**: Add throughout for better IDE support and documentation
8. **Clear Naming**: Use descriptive names that explain intent

## Testing Strategy

After each refactoring:
1. Run existing test suites (if any)
2. Compare output files byte-for-byte with previous version
3. Verify performance hasn't degraded
4. Ensure all error cases still work correctly

## Next Steps

1. ✅ Complete: config.py and utils.py
2. 🔄 In Progress: Create this refactoring plan
3. 📝 Next: Refactor shifts_handler.py (quickest win)
4. 📝 Then: Tackle the critical priority files in order:
   - withdrawals_matcher.py (split into modules)
   - preprocess.py (split into processor-specific files)
   - output.py (create generic report generator)
   - reports_creator.py (break down main function)
   - files_renamer.py (extract patterns to config)

---

*Document created: 2025-12-04*
*Last updated: 2025-12-04*
