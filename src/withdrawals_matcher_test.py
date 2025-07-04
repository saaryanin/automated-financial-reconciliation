import numpy as np
from difflib import SequenceMatcher
from datetime import datetime, timedelta
from functools import lru_cache
import pandas as pd
import logging, threading, time
from concurrent.futures import ThreadPoolExecutor, as_completed


class ProcessorConfig:
    def __init__(self,
                 email_threshold=0.0,
                 name_match_threshold=0.0,
                 require_last4=True,
                 require_email=True,
                 enable_name_fallback=True,
                 enable_exact_match=True,
                 tolerance=0.0,
                 matching_logic="standard"):
        self.email_threshold = email_threshold
        self.name_match_threshold = name_match_threshold
        self.require_last4 = require_last4
        self.require_email = require_email
        self.enable_name_fallback = enable_name_fallback
        self.enable_exact_match = enable_exact_match
        self.tolerance = tolerance
        self.matching_logic = matching_logic


# Processor-specific configurations
PROCESSOR_CONFIGS = {
    'safecharge': ProcessorConfig(
        email_threshold=0.15,
        require_last4=True,
        require_email=True,
        tolerance=0.1,
    ),
    'powercash': ProcessorConfig(
        email_threshold=0.6,
        require_last4=True,
        require_email=True,
        tolerance=0.1,
    ),
    'paypal': ProcessorConfig(
        email_threshold=0.6,
        name_match_threshold=0.75,
        require_last4=False,
        require_email=False,
        enable_name_fallback=True,
        tolerance = 0.1,
        matching_logic="paypal"
    ),
    'shift4': ProcessorConfig(
        email_threshold=0.6,
        name_match_threshold=0.70,
        require_last4=True,
        require_email=True,
        enable_name_fallback=True,
        enable_exact_match=True,
        tolerance=0.1,
        matching_logic="shift4"
    ),
    'skrill': ProcessorConfig(
        email_threshold=0.6,
        require_last4=False,
        require_email=True,
        enable_name_fallback=False,
        enable_exact_match=False,
        tolerance=0.1,
        matching_logic="skrill"
    ),
    'neteller': ProcessorConfig(
        email_threshold=0.6,
        require_last4=False,
        require_email=True,
        enable_name_fallback=False,
        enable_exact_match=False,
        tolerance=0.1,
        matching_logic="skrill"   # we'll just reuse the Skrill logic
    ),
    'bitpay': ProcessorConfig(
        email_threshold=0.6,
        require_last4=False,
        require_email=True,
        enable_name_fallback=True,
        enable_exact_match=False,
        tolerance=0.1,
        matching_logic="bitpay"
    ),
    'zotapay_paymentasia': ProcessorConfig(  # Add this new configuration
        email_threshold=1,
        require_last4=False,
        require_email=False,
        enable_name_fallback=False,
        enable_exact_match=True,
        tolerance=0.1,  # 2% tolerance
        matching_logic="zotapay_paymentasia"
    ),
    "trustpayments": ProcessorConfig(
        email_threshold=0.65,
        name_match_threshold=0.75,
        require_last4=False,
        require_email=False,
        enable_name_fallback=False,
        enable_exact_match=False,
        tolerance=0.1,
        matching_logic="trustpayments"
    ),
    # ... other processors ...
}


def load_(csv_path):
    df = pd.read_csv(csv_path)
    return {
        (row['from_currency'], row['to_currency']): row['rate']
        for _, row in df.iterrows()
    }


class ReconciliationEngine:
    def __init__(self, exchange_rate_map, config=None):
        self.exchange_rate_map = exchange_rate_map
        self.config = {
            'top_candidates': 100,
            'enable_fallback': True,
            'enable_diagnostics': True,
            'log_level': logging.INFO,
            'deep_search': True,
            'timeout': 300,
            'auto_adjust': True
        }
        if config:
            self.config.update(config)

        self.logger = logging.getLogger('ReconciliationEngine')
        self.logger.setLevel(self.config['log_level'])
        if not self.logger.handlers:
            ch = logging.StreamHandler()
            ch.setLevel(self.config['log_level'])
            ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
            self.logger.addHandler(ch)

        self.metrics = {k: 0 for k in ['total_crm', 'matched_main', 'matched_fallback', 'unmatched']}
        self.metrics.update({
            'processing_time': 0,
            'combo_distribution': {},
            'currency_matches': {},
            'row_times': [],
            'correct_payments': 0,
            'incorrect_payments': 0
        })
        self.diagnostics, self.lock = [], threading.Lock()
        self.start_time, self.estimated_time, self.parameter_adjusted = None, None, False

    def get_processor_config(self, processor_name):
        """Get configuration for specific processor"""
        processor_name = processor_name.lower()
        # ─── alias PowerCash to SafeCharge ────────────────────────────────
        if processor_name == "powercash":
            processor_name = "safecharge"
        # ───────────────────────────────────────────────────────────────────
        return PROCESSOR_CONFIGS.get(processor_name, ProcessorConfig())

    @lru_cache(maxsize=None)
    def enhanced_email_similarity(self, e1, e2):
        e1 = '' if pd.isna(e1) else str(e1)
        e2 = '' if pd.isna(e2) else str(e2)
        if not e1 or not e2:
            return 0.0
        l1 = e1.lower().split('@')[0] if '@' in e1 else e1.lower()
        l2 = e2.lower().split('@')[0] if '@' in e2 else e2.lower()

        return SequenceMatcher(None, l1, l2).ratio()

    def name_in_email(self, name, email):
        if not name or not email or pd.isna(name) or pd.isna(email):
            return False
        name = str(name).lower().strip()
        email_local = str(email).split('@')[0].lower() if '@' in email else str(email).lower()
        return name in email_local

    def convert_amount(self, amount, from_cur, to_cur):
        if from_cur == to_cur:
            return amount, 1.0

        # Try direct rate
        rate = self.exchange_rate_map.get((from_cur, to_cur))
        if rate:
            return amount * rate, rate

        # Try USD bridge if available
        usd_rate1 = self.exchange_rate_map.get(('USD', to_cur))
        usd_rate2 = self.exchange_rate_map.get((from_cur, 'USD'))

        if usd_rate1 and usd_rate2:
            usd_amount = amount * usd_rate2
            return usd_amount * usd_rate1, usd_rate2 * usd_rate1

        self.logger.error(f"Missing conversion: {from_cur}->{to_cur}")
        return None, None

    def preprocess_processor_amounts(self, processor_df, crm_currency):
        def convert(row):
            amt_conv, rate = self.convert_amount(row['proc_amount'], row['proc_currency'], crm_currency)
            return pd.Series({'proc_amount_converted': amt_conv if amt_conv is not None else row['proc_amount'],
                              'proc_rate': rate or 1.0})

        converted = processor_df.apply(convert, axis=1)
        return pd.concat([processor_df, converted], axis=1)

    def generate_report(self):
        return {
            'metrics': self.metrics,
            'diagnostics': self.diagnostics if self.config['enable_diagnostics'] else None,
            'estimated_time': self.estimated_time,
            'parameters_adjusted': self.parameter_adjusted
        }


    def _match_processor_to_crm_row(self, proc_row, crm_row, proc_config):
        def safe_lower_strip(value):
            if pd.isna(value):
                return ''
            return str(value).lower().strip()
        """
        Try to match one processor row to one CRM row.

        Return (match_dict, diagnostics_dict) or None if no match.
        """
        # Extract relevant fields
        proc_amount = proc_row.get('proc_amount')
        proc_currency = proc_row.get('proc_currency')
        proc_email = safe_lower_strip(proc_row.get('proc_email'))
        proc_last4 = str(proc_row.get('proc_last4') or '').strip()
        proc_tp = str(proc_row.get('proc_tp') or '').strip()

        crm_amount = crm_row.get('crm_amount')
        crm_currency = crm_row.get('crm_currency')
        crm_email = safe_lower_strip(crm_row.get('crm_email'))
        crm_last4 = str(crm_row.get('crm_last4') or '').strip()
        crm_tp = str(crm_row.get('crm_tp') or '').strip()

        # Convert processor amount to CRM currency
        proc_amount_conv, rate = self.convert_amount(proc_amount, proc_currency, crm_currency)
        if proc_amount_conv is None:
            return None

        # Compare amounts with tolerance
        abs_tol = 0.1
        rel_tol = proc_config.tolerance * abs(crm_amount)
        tol = abs_tol if proc_currency == crm_currency else rel_tol
        if abs(proc_amount_conv - crm_amount) > tol:
            return None

        # Check last4 match or email similarity
        last4_valid = crm_last4 not in ("0", "0000", "", "nan")
        last4_match = last4_valid and crm_last4 == proc_last4

        # Email similarity using your function
        email_sim = self.enhanced_email_similarity(crm_email, proc_email)
        if proc_config.require_email and email_sim < proc_config.email_threshold and not last4_match:
            return None

        # If required last4 and no match, fail
        if proc_config.require_last4 and not last4_match and email_sim < 0.75:
            return None

        # Build match dict (similar to other match_*_row methods)
        match = {
            'crm_date': crm_row.get('crm_date'),
            'crm_email': crm_email,
            'crm_firstname': crm_row.get('crm_firstname', ''),
            'crm_lastname': crm_row.get('crm_lastname', ''),
            'crm_last4': crm_last4,
            'crm_currency': crm_currency,
            'crm_amount': crm_amount,
            'crm_processor_name': crm_row.get('crm_processor_name'),

            'proc_date': proc_row.get('proc_date'),
            'proc_email': proc_email,
            'proc_firstname': proc_row.get('proc_firstname', ''),
            'proc_lastname': proc_row.get('proc_lastname', ''),
            'proc_last4': proc_last4,
            'proc_currency': proc_currency,
            'proc_amount': proc_amount,
            'proc_amount_crm_currency': proc_amount_conv,
            'proc_processor_name': proc_row.get('proc_processor_name'),

            'email_similarity_avg': round(email_sim, 4),
            'last4_match': last4_match,
            'name_fallback_used': False,  # you can add fallback logic if you want
            'exact_match_used': last4_match and abs(proc_amount_conv - crm_amount) <= tol,
            'converted': proc_currency != crm_currency,
            'proc_combo_len': 1,
            'crm_combo_len': 1,
            'match_status': 1,
            'payment_status': 1,
            'comment': "Cross-processor fallback match",
            'matched_proc_indices': [proc_row.name],  # proc_row.name is index in DataFrame
            'crm_row_index': crm_row.name  # track CRM index to mark used_crm
        }

        return match, {}

    def _cross_processor_last_chance(self, crm_df, processor_df, used_crm, used_proc, matches):
        cross_processors = {"shift4", "safecharge", "powercash", "paypal", "trustpayments"}

        # Filter unmatched processor rows from allowed processors
        unmatched_proc_rows = processor_df[
            processor_df['proc_processor_name'].str.lower().isin(cross_processors) &
            (~processor_df.index.isin(used_proc))
            ]

        if unmatched_proc_rows.empty:
            return

        proc_dict = processor_df.to_dict('index')
        crm_dict = crm_df.to_dict('index')
        crm_last4_map = crm_df.groupby('crm_last4').indices  # for quick filtering by last4 if needed

        for proc_idx, proc_row in unmatched_proc_rows.iterrows():
            # Try to find matching CRM rows that are not used yet
            candidate_crm_indices = [i for i in crm_df.index if i not in used_crm]

            best_match = None
            best_diag = None

            # Attempt match for each candidate CRM row
            for crm_idx in candidate_crm_indices:
                crm_row = crm_dict[crm_idx]
                # Get the matching function based on CRM processor name
                crm_proc_name = (crm_row.get('crm_processor_name') or '').lower()
                proc_config = self.get_processor_config(crm_proc_name)

                # Call appropriate match function with CRM row and current processor row only
                # Note: You might want to write a "_match_processor_row_to_crm" or reuse existing with slight tweak
                match_result = self._match_processor_to_crm_row(
                    proc_row, crm_row, proc_config
                )

                if match_result is not None:
                    match, diag = match_result
                    if match and (best_match is None or self._is_better_match(match, best_match)):
                        best_match = match
                        best_diag = diag

            if best_match:
                # Mark both as used
                used_proc.add(proc_idx)
                used_crm.add(best_match['crm_row_index'])  # You will need to add this index inside match

                best_match['comment'] = (best_match.get('comment', '') + " [Cross-processor fallback]").strip()
                best_match['cross_processor_fallback'] = True

                matches.append(best_match)

    def match_withdrawals(self, crm_df, processor_df):
        self.start_time = datetime.now()
        self.metrics['total_crm'] = len(crm_df)
        used_proc, used_crm, matches = set(), set(), []
        last4_map = processor_df.groupby('proc_last4').indices
        proc_dict = processor_df.to_dict('index')

        self._estimate_runtime(crm_df, proc_dict, last4_map)

        # match CRM rows
        for idx, row in crm_df.iterrows():
            if idx in used_crm:
                continue
            if self._check_timeout():
                break
            proc_name = str(row.get('crm_processor_name', '')).strip().lower()
            print(f"🧪 CRM idx={idx} processor_name: '{proc_name}'")

            try:
                result = self._match_crm_row(row, proc_dict, last4_map, used_proc)
                if str(row.get("crm_processor_name", "")).strip().lower() == "zotapay_paymentasia":
                    self.logger.debug(
                        f"[DEBUG] Trying to match Zota+PA row: TP={row.get('crm_tp')}, Email={row.get('crm_email')}, Amount={row.get('crm_amount')}")

                if result is None or not isinstance(result, tuple) or len(result) != 2:
                    raise ValueError(f"_match_crm_row() returned invalid result: {result}")

                match, diag = result

            except Exception as e:
                self.logger.error(f"Error processing row {idx}: {e}")
                match, diag = None, {'failure_reason': str(e)}

            crm_tp_val = row.get('crm_tp')

            if match:
                # collect proc_tp for this combo
                proc_tp_vals = [proc_dict[i].get('proc_tp', '') for i in match['matched_proc_indices']]

                ordered = {}
                for k, v in match.items():
                    ordered[k] = v
                    if k == 'proc_email':
                        ordered['proc_tp'] = proc_tp_vals
                    if k == 'crm_lastname':
                        ordered['crm_tp'] = crm_tp_val
                match = ordered

                matches.append(match)
                used_crm.add(idx)
                used_proc.update(match['matched_proc_indices'])
                self.metrics['currency_matches'][match['crm_currency']] = \
                    self.metrics['currency_matches'].get(match['crm_currency'], 0) + 1

                if match['payment_status'] == 1:
                    self.metrics['correct_payments'] += 1
                else:
                    self.metrics['incorrect_payments'] += 1

            else:
                unmatched = self._create_unmatched_crm_record(crm_df.loc[idx])
                ordered = {}
                for k, v in unmatched.items():
                    ordered[k] = v
                    if k == 'crm_lastname':
                        ordered['crm_tp'] = crm_tp_val
                    if k == 'proc_email':
                        ordered['proc_tp'] = []  # no proc match
                matches.append(ordered)
                self.metrics['unmatched'] += 1
                if self.config['enable_diagnostics']:
                    self.diagnostics.append({
                        'crm_idx': idx,
                        'failure_reason': diag.get('failure_reason', 'No candidates')
                    })

            if idx % 10 == 0:
                self._update_eta(len(crm_df), idx + 1)

            proc_name = str(row.get('crm_processor_name', '')).strip().lower()
            proc_rows_for_processor = processor_df[
                processor_df['proc_processor_name'].str.lower().str.strip() == proc_name
                ]

            if proc_rows_for_processor.empty:
                unmatched = self._create_unmatched_crm_record(row)
                ...
                self.diagnostics.append({
                    'crm_idx': idx,
                    'failure_reason': 'No processor data found for this CRM processor'
                })
                continue
            # --- LAST-CHANCE CROSS-PROCESSOR MATCHING ---
        self._cross_processor_last_chance(crm_df, processor_df, used_crm, used_proc, matches)
        # unmatched processor‐only rows
        for idx, row in processor_df.iterrows():
            if idx not in used_proc:
                base = {
                    'crm_date': None,
                    'crm_email': None,
                    'crm_firstname': None,
                    'crm_lastname': None,
                    'crm_tp': None,
                    'crm_last4': None,
                    'crm_currency': None,
                    'crm_amount': None,
                    'crm_processor_name': None,
                    'proc_date': [row.get('proc_date')],
                    'proc_email': [row.get('proc_email')],
                    'proc_last4': [row.get('proc_last4')],
                    'proc_currency': [row.get('proc_currency')],
                    'proc_amount': [row.get('proc_amount')],
                    'proc_processor_name': row.get('proc_processor_name'),
                    'proc_firstname': [row.get('proc_firstname')],
                    'proc_lastname': [row.get('proc_lastname')],
                    'email_similarity_avg': None,
                    'last4_match': None,
                    'name_fallback_used': False,
                    'exact_match_used': False,
                    'match_status': 0,
                    'payment_status': 0,
                    'comment': "No matching CRM row found",
                    'matched_proc_indices': [idx],
                }
                # build in order so that proc_tp follows proc_email
                entry = {}
                for k, v in base.items():
                    entry[k] = v
                    if k == 'proc_email':
                        entry['proc_tp'] = [row.get('proc_tp', '')]
                matches.append(entry)

        self.metrics['processing_time'] = (datetime.now() - self.start_time).total_seconds()
        self.logger.info(f"Total processing time: {timedelta(seconds=self.metrics['processing_time'])}")
        return matches

    def _match_crm_row(self, crm_row, proc_dict, last4_map, used):
        """
        Route each CRM row to its processor-specific matcher:
          • SafeCharge / PowerCash: standard logic
          • PayPal: paypal logic
          • Shift4: shift4 logic
          • Skrill/Neteller: skrill_neteller logic
          • Bitpay: bitpay logic
          • Zotapay/PaymentAsia combined: zotapay_paymentasia logic
        """
        proc = (crm_row.get('crm_processor_name') or '').strip().lower()

        # alias PowerCash → SafeCharge
        if proc == 'powercash':
            proc = 'safecharge'

        if proc == 'paypal':
            return self._match_paypal_row(
                crm_row, proc_dict, last4_map, used,
                self.get_processor_config('paypal')
            )

        if proc == 'shift4':
            return self._match_shift4_row(
                crm_row, proc_dict, last4_map, used,
                self.get_processor_config('shift4')
            )

        if proc in ('skrill', 'neteller'):
            return self._match_skrill_neteller_row(
                crm_row, proc_dict, last4_map, used,
                self.get_processor_config(proc),
                processor_name=proc
            )

        if proc == 'bitpay':
            return self._match_bitpay_row(
                crm_row, proc_dict, last4_map, used,
                self.get_processor_config('bitpay')
            )

        if proc == "zotapay_paymentasia":
            print(f"Routing to _match_zotapay_paymentasia_row for CRM TP: {crm_row.get('crm_tp')}")
            return self._match_zotapay_paymentasia_row(crm_row, proc_dict, last4_map, used)

        if proc == "trustpayments":
            return self._match_trustpayments_row(
                crm_row, proc_dict, last4_map, used,
                self.get_processor_config("trustpayments")
            )

        # Fallback to default standard matcher
        return self._match_standard_row(
            crm_row, proc_dict, last4_map, used,
            self.get_processor_config(proc)
        )

    def _match_standard_row(self, crm_row, proc_dict, last4_map, used, proc_config):
        crm_last4 = str(crm_row['crm_last4']) if not pd.isna(crm_row['crm_last4']) else ''
        crm_cur = crm_row['crm_currency']
        crm_amt = crm_row['crm_amount']
        crm_email = crm_row['crm_email']
        crm_first = str(crm_row.get('crm_firstname', ''))
        crm_last = str(crm_row.get('crm_lastname', ''))

        candidates = []
        indices = [i for i in proc_dict if i not in used]
        if crm_last4 and crm_last4 in last4_map and proc_config.require_last4:
            indices = last4_map[crm_last4]

        for i in indices:
            if i in used:
                continue
            if i not in proc_dict:
                self.logger.warning(f"Index {i} from last4_map not found in proc_dict, skipping.")
                continue
            row = proc_dict[i]

            proc_amt = row.get('proc_amount')
            proc_cur = row.get('proc_currency')
            if proc_amt is None or proc_cur is None:
                continue

            # Convert amount dynamically to CRM currency
            proc_amt_crm_cur, rate = self.convert_amount(proc_amt, proc_cur, crm_cur)
            if proc_amt_crm_cur is None:
                continue  # conversion failed, skip

            email_sim = self.enhanced_email_similarity(crm_email, row.get('proc_email', ''))
            proc_last4_str = str(row.get('proc_last4', ''))
            last4_match = (crm_last4 == proc_last4_str) and (crm_last4 not in ("0", "0000", "", "nan"))

            name_fallback = False
            if proc_config.enable_name_fallback:
                if crm_first:
                    name_fallback = self.name_in_email(crm_first, row.get('proc_email', ''))
                if not name_fallback and crm_last:
                    name_fallback = self.name_in_email(crm_last, row.get('proc_email', ''))

            valid_last4 = crm_last4 not in ("", "0", "0000", "nan")

            # STRICT matching rules:
            # If require_last4 and valid CRM last4, last4 MUST match or reject candidate
            if proc_config.require_last4 and valid_last4:
                if not last4_match:
                    continue  # reject if last4 mismatch

                # Also require email similarity threshold or name fallback
                if proc_config.require_email and email_sim < proc_config.email_threshold and not name_fallback:
                    continue

            else:
                # last4 not required or invalid, rely on email similarity threshold or fallback
                if proc_config.require_email and email_sim < proc_config.email_threshold and not name_fallback:
                    continue

            candidates.append({
                'index': i,
                'proc_amount_crm_currency': proc_amt_crm_cur,
                'proc_rate': rate,
                'email_score': email_sim,
                'row_data': row,
                'last4_match': last4_match,
                'name_fallback': name_fallback,
                'exact_match': False
            })

        if not candidates:
            return None, {'failure_reason': 'No candidates found'}

        # Pick best candidate by email similarity, last4 match, fallback
        candidates.sort(key=lambda c: (
            -c['email_score'],
            -int(c['last4_match']),
            -int(c['name_fallback'])
        ))
        best = candidates[0]

        proc_amt = best['proc_amount_crm_currency']
        crm_amt_abs = abs(crm_amt)
        proc_amt_abs = abs(proc_amt)

        diff = proc_amt_abs - crm_amt_abs
        abs_diff = abs(diff)
        tolerance = max(0.1, proc_config.tolerance * crm_amt_abs)

        payment_status = 1 if abs_diff <= tolerance else 0
        comment = ""
        if payment_status == 0:
            if diff < 0:
                comment = f"Client received less {abs_diff:.2f} {crm_cur}"
            else:
                comment = f"Client received more {abs_diff:.2f} {crm_cur}"

        proc_date_raw = best['row_data'].get('proc_date')
        proc_date_ts = pd.to_datetime(proc_date_raw, errors='coerce')
        proc_date_ts = proc_date_ts.normalize() if proc_date_ts is not pd.NaT else None

        match = {
            'crm_date': crm_row.get('crm_date'),
            'crm_email': crm_email,
            'crm_firstname': crm_row.get('crm_firstname', ''),
            'crm_lastname': crm_row.get('crm_lastname', ''),
            'crm_last4': crm_last4,
            'crm_currency': crm_cur,
            'crm_amount': crm_amt,
            'crm_processor_name': crm_row.get('crm_processor_name'),
            'proc_date': proc_date_ts,
            'proc_email': best['row_data'].get('proc_email'),
            'proc_firstname': best['row_data'].get('proc_firstname', ''),
            'proc_lastname': best['row_data'].get('proc_lastname', ''),
            'proc_last4': best['row_data'].get('proc_last4'),
            'proc_currency': best['row_data'].get('proc_currency'),
            'proc_amount': best['row_data'].get('proc_amount'),
            'proc_amount_crm_currency': round(best['proc_amount_crm_currency'], 4),
            'proc_processor_name': best['row_data'].get('proc_processor_name'),
            'email_similarity_avg': round(best['email_score'], 4),
            'last4_match': best['last4_match'],
            'name_fallback_used': best['name_fallback'],
            'exact_match_used': False,
            'converted': (best['proc_rate'] != 1.0),
            'proc_combo_len': 1,
            'crm_combo_len': 1,
            'match_status': 1,
            'payment_status': payment_status,
            'comment': comment,
            'matched_proc_indices': [best['index']]
        }

        return match, {}

    def _match_zotapay_paymentasia_row(self, crm_row, proc_dict, last4_map, used, proc_config):
        crm_tp = str(crm_row.get('crm_tp', '')).strip()
        crm_amt = abs(crm_row['crm_amount'])
        crm_cur = str(crm_row['crm_currency']).strip().upper()
        crm_email = str(crm_row.get('crm_email', '')).strip().lower()
        crm_last4 = str(crm_row.get('crm_last4', '')).strip()

        if not crm_tp:
            return None, {'failure_reason': 'Missing CRM TP'}

        abs_tol = 0.1
        rel_tol = proc_config.tolerance * crm_amt

        candidates = []
        indices = [i for i in proc_dict if i not in used]

        for idx in indices:
            row = proc_dict[idx]

            proc_tp = str(row.get('proc_tp', '')).strip()
            if proc_tp != crm_tp:
                continue

            proc_amt_crm_cur = row.get('proc_amount_crm_currency')
            if proc_amt_crm_cur is None:
                continue

            proc_email = str(row.get('proc_email', '')).strip().lower()
            proc_last4 = str(row.get('proc_last4', '')).strip()
            proc_cur = str(row.get('proc_currency', '')).strip().upper()

            email_sim = self.enhanced_email_similarity(crm_email, proc_email)
            last4_valid = crm_last4 not in ("0", "0000", "", "nan")
            last4_match = last4_valid and (crm_last4 == proc_last4)

            if not (last4_match or email_sim >= proc_config.email_threshold):
                continue

            diff = proc_amt_crm_cur - crm_amt
            abs_diff = abs(diff)
            tolerance = abs_tol if proc_cur == crm_cur else rel_tol
            amount_match = abs_diff <= tolerance
            payment_status = 1 if amount_match else 0

            comment = ""
            if not amount_match:
                comment = f"Client received {'less' if diff < 0 else 'more'} {abs_diff:.2f} {crm_cur}"

            candidates.append({
                'index': idx,
                'proc_tp': proc_tp,
                'proc_date': row.get('proc_date'),
                'proc_email': proc_email,
                'proc_firstname': row.get('proc_firstname', ''),
                'proc_lastname': row.get('proc_lastname', ''),
                'proc_last4': proc_last4,
                'proc_currency': proc_cur,
                'proc_amount': row.get('proc_amount'),
                'proc_amount_crm_currency': round(proc_amt_crm_cur, 4),
                'payment_status': payment_status,
                'comment': comment,
                'email_similarity': email_sim,
                'last4_match': last4_match,
                'exact_match': last4_match and amount_match,
                'name_fallback': False  # could add if desired
            })

        if not candidates:
            return None, {'failure_reason': f'No zotapay_paymentasia match for TP: {crm_tp}'}

        # Sort candidates by exact match, email similarity, last4 match
        candidates.sort(key=lambda c: (
            -int(c['exact_match']),
            -c['email_similarity'],
            -int(c['last4_match'])
        ))

        best_candidate = candidates[0]

        match = {
            'crm_date': crm_row.get('crm_date'),
            'crm_email': crm_email,
            'crm_firstname': crm_row.get('crm_firstname', ''),
            'crm_lastname': crm_row.get('crm_lastname', ''),
            'crm_last4': crm_last4,
            'crm_currency': crm_cur,
            'crm_amount': crm_amt,
            'crm_processor_name': crm_row.get('crm_processor_name', 'zotapay_paymentasia'),

            'proc_date': best_candidate['proc_date'],
            'proc_email': best_candidate['proc_email'],
            'proc_firstname': best_candidate['proc_firstname'],
            'proc_lastname': best_candidate['proc_lastname'],
            'proc_last4': best_candidate['proc_last4'],
            'proc_currency': best_candidate['proc_currency'],
            'proc_amount': best_candidate['proc_amount'],
            'proc_amount_crm_currency': best_candidate['proc_amount_crm_currency'],
            'proc_processor_name': proc_dict[best_candidate['index']].get('proc_processor_name'),

            'email_similarity_avg': round(best_candidate['email_similarity'], 4),
            'last4_match': best_candidate['last4_match'],
            'name_fallback_used': best_candidate['name_fallback'],
            'exact_match_used': best_candidate['exact_match'],
            'converted': best_candidate['proc_currency'] != crm_cur,
            'proc_combo_len': 1,
            'crm_combo_len': 1,
            'match_status': 1,
            'payment_status': best_candidate['payment_status'],
            'comment': best_candidate['comment'],
            'matched_proc_indices': [best_candidate['index']]
        }
        return match, {}

    def _match_bitpay_row(self, crm_row, proc_dict, last4_map, used, proc_config):
        crm_cur = crm_row['crm_currency']
        crm_amt = abs(crm_row['crm_amount'])
        crm_email = (crm_row.get('crm_email') or '').lower().strip()
        crm_first = str(crm_row.get('crm_firstname', '')).lower().strip()
        crm_last = str(crm_row.get('crm_lastname', '')).lower().strip()
        crm_last4 = str(crm_row.get('crm_last4', '')).strip()

        abs_tol = 0.1
        rel_tol = proc_config.tolerance * crm_amt

        candidates = []
        indices = [i for i in proc_dict if i not in used]

        for i in indices:
            row = proc_dict[i]
            proc_amt_crm = row.get('proc_amount_crm_currency')
            if proc_amt_crm is None:
                continue

            proc_email = str(row.get('proc_email', '')).lower().strip()
            email_sim = self.enhanced_email_similarity(crm_email, proc_email)
            email_match = email_sim >= proc_config.email_threshold

            proc_first = str(row.get('proc_firstname', '')).lower().strip()
            proc_last = str(row.get('proc_lastname', '')).lower().strip()
            name_match = (
                    SequenceMatcher(None, crm_first, proc_first).ratio() >= proc_config.name_match_threshold or
                    SequenceMatcher(None, crm_last, proc_last).ratio() >= proc_config.name_match_threshold
            )

            if not (email_match or name_match):
                continue

            proc_last4 = str(row.get('proc_last4', '')).strip()
            last4_valid = crm_last4 not in ("0", "0000", "", "nan")
            last4_match = last4_valid and (crm_last4 == proc_last4)

            diff = proc_amt_crm - crm_amt
            abs_diff = abs(diff)
            proc_cur = str(row.get('proc_currency', '')).strip()
            same_currency = crm_cur == proc_cur
            tol = abs_tol if same_currency else rel_tol
            amount_match = abs_diff <= tol

            if not amount_match:
                continue

            candidates.append({
                'index': i,
                'proc_amount_crm_currency': proc_amt_crm,
                'email_score': email_sim,
                'currency': proc_cur,
                'rate': row.get('proc_rate', 1.0),
                'row_data': row,
                'name_fallback': name_match and not email_match,
                'last4_match': last4_match,
                'exact_match': amount_match and last4_match,
            })

        if not candidates:
            return None, {'failure_reason': 'No BitPay candidates found'}

        # Sort candidates: prioritize exact match, last4 match, higher email similarity, then name fallback
        candidates.sort(key=lambda c: (
            -int(c['exact_match']),
            -int(c['last4_match']),
            -c['email_score'],
            -int(c['name_fallback'])
        ))

        best = candidates[0]
        payment_status = 1 if best['exact_match'] else 0
        comment = "" if payment_status else "Amount or identifiers mismatch"

        match = {
            'crm_date': crm_row.get('crm_date'),
            'crm_email': crm_email,
            'crm_firstname': crm_row.get('crm_firstname', ''),
            'crm_lastname': crm_row.get('crm_lastname', ''),
            'crm_last4': crm_last4,
            'crm_currency': crm_cur,
            'crm_amount': crm_amt,
            'crm_processor_name': 'bitpay',

            'proc_date': best['row_data'].get('proc_date'),
            'proc_email': best['row_data'].get('proc_email'),
            'proc_firstname': best['row_data'].get('proc_firstname', ''),
            'proc_lastname': best['row_data'].get('proc_lastname', ''),
            'proc_last4': best['row_data'].get('proc_last4'),
            'proc_currency': best['row_data'].get('proc_currency'),
            'proc_amount': best['row_data'].get('proc_amount'),
            'proc_amount_crm_currency': best['proc_amount_crm_currency'],
            'proc_processor_name': 'bitpay',

            'exchange_rate': best['rate'],
            'email_similarity_avg': round(best['email_score'], 4),
            'last4_match': best['last4_match'],
            'name_fallback_used': best['name_fallback'],
            'exact_match_used': best['exact_match'],
            'match_status': 1,
            'payment_status': payment_status,
            'comment': comment,
            'matched_proc_indices': [best['index']]
        }
        return match, {}

    def _match_shift4_row(self, crm_row, proc_dict, last4_map, used, proc_config):
        crm_last4 = str(crm_row.get('crm_last4', '')).strip()
        crm_cur = crm_row['crm_currency']
        crm_amt = abs(crm_row['crm_amount'])
        crm_email = (crm_row.get('crm_email') or '').lower()
        crm_first = str(crm_row.get('crm_firstname', '')).lower().strip()
        crm_last = str(crm_row.get('crm_lastname', '')).lower().strip()

        if crm_last4 and crm_last4 in last4_map and proc_config.require_last4:
            indices = [i for i in last4_map[crm_last4] if i not in used]
        else:
            indices = [i for i in proc_dict if i not in used]

        candidates = []

        abs_tol = 0.1
        rel_tol = proc_config.tolerance * crm_amt

        for i in indices:
            row = proc_dict[i]

            proc_amt_raw = row.get('proc_amount')
            proc_cur = row.get('proc_currency')
            if proc_amt_raw is None or proc_cur is None:
                continue

            # Convert processor amount to CRM currency
            proc_amt_crm, rate = self.convert_amount(proc_amt_raw, proc_cur, crm_cur)
            if proc_amt_crm is None:
                continue

            proc_last4 = str(row.get('proc_last4', '')).strip()
            if proc_config.require_last4 and proc_last4 != crm_last4:
                continue

            loc_crm = crm_email.split('@')[0] if crm_email else ''
            loc_proc = str(row.get('proc_email', '')).lower().split('@')[0] if row.get('proc_email') else ''
            email_prefix = (loc_crm[:2] == loc_proc[:2]) if loc_crm and loc_proc else False
            email_score = 1 if email_prefix else 0

            proc_first = str(row.get('proc_firstname', '')).lower().strip()
            proc_last = str(row.get('proc_lastname', '')).lower().strip()
            first_prefix = crm_first[:3] == proc_first[:3] if crm_first and proc_first else False
            last_prefix = crm_last[:3] == proc_last[:3] if crm_last and proc_last else False

            if not (email_prefix or first_prefix or last_prefix):
                continue

            same_currency = crm_cur == proc_cur
            tol = abs_tol if same_currency else rel_tol
            amount_match = abs(proc_amt_crm - crm_amt) <= tol
            if not amount_match:
                continue

            candidates.append({
                'index': i,
                'proc_amount_crm_currency': proc_amt_crm,
                'rate': rate,
                'row': row,
                'email_prefix': email_prefix,
                'first_prefix': first_prefix,
                'last_prefix': last_prefix,
                'email_score': email_score
            })

        if not candidates:
            return None, {'failure_reason': 'No valid Shift4 candidates'}

        candidates.sort(key=lambda c: (
            -int(c['email_prefix']),
            -int(c['first_prefix']),
            -int(c['last_prefix'])
        ))

        best = candidates[0]
        received = round(best['proc_amount_crm_currency'], 4)
        payment_status = 1  # Or better calculate based on tolerance (amount_match)
        comment = ""

        match = {
            'crm_date': crm_row.get('crm_date'),
            'crm_email': crm_email,
            'crm_firstname': crm_row.get('crm_firstname', ''),
            'crm_lastname': crm_row.get('crm_lastname', ''),
            'crm_last4': crm_last4,
            'crm_currency': crm_cur,
            'crm_amount': crm_amt,
            'crm_processor_name': 'shift4',
            'proc_date': best['row'].get('proc_date'),
            'proc_email': best['row'].get('proc_email'),
            'proc_firstname': best['row'].get('proc_firstname', ''),
            'proc_lastname': best['row'].get('proc_lastname', ''),
            'proc_last4': best['row'].get('proc_last4'),
            'proc_currency': best['row'].get('proc_currency'),
            'proc_amount': best['row'].get('proc_amount'),
            'proc_amount_crm_currency': best['proc_amount_crm_currency'],
            'proc_processor_name': best['row'].get('proc_processor_name'),
            'exchange_rate': best['rate'],
            'email_similarity_avg': best['email_score'],
            'last4_match': True,
            'name_fallback_used': False,
            'exact_match_used': True,
            'match_status': 1,
            'payment_status': payment_status,
            'comment': comment,
            'matched_proc_indices': [best['index']]
        }
        return match, {}

    def _match_paypal_row(self, crm_row, proc_dict, last4_map, used, proc_config):
        crm_cur = crm_row['crm_currency']
        crm_amt = abs(crm_row['crm_amount'])
        crm_email = (crm_row.get('crm_email') or '').lower()
        crm_first = str(crm_row.get('crm_firstname', '')).lower().strip()
        crm_last = str(crm_row.get('crm_lastname', '')).lower().strip()
        crm_last4 = str(crm_row.get('crm_last4', '')).strip()

        candidates = []
        indices = [i for i in proc_dict if i not in used]

        abs_tol = 0.1
        rel_tol = proc_config.tolerance * crm_amt

        for i in indices:
            row = proc_dict[i]

            proc_amt = row.get('proc_amount')
            proc_cur = row.get('proc_currency')
            if proc_amt is None or proc_cur is None:
                continue

            # Convert amount to CRM currency
            proc_amt_crm_cur, rate = self.convert_amount(proc_amt, proc_cur, crm_cur)
            if proc_amt_crm_cur is None:
                continue

            email = str(row.get('proc_email', '')).lower()
            email_sim = self.enhanced_email_similarity(crm_email, email)
            email_match = email_sim >= proc_config.email_threshold

            proc_first = str(row.get('proc_firstname', '')).lower().strip()
            proc_last = str(row.get('proc_lastname', '')).lower().strip()
            name_match = (
                    crm_first and crm_last and
                    SequenceMatcher(None, crm_first, proc_first).ratio() >= proc_config.name_match_threshold and
                    SequenceMatcher(None, crm_last, proc_last).ratio() >= proc_config.name_match_threshold
            )

            name_in_email_match = self.name_in_email(crm_first, email) or self.name_in_email(crm_last, email)

            if not (email_match or name_match or name_in_email_match):
                continue

            same_currency = crm_cur == proc_cur
            tol = abs_tol if same_currency else rel_tol
            amount_match = abs(proc_amt_crm_cur - crm_amt) <= tol
            if not amount_match:
                continue

            proc_last4 = str(row.get('proc_last4', '')).strip()
            last4_match = (crm_last4 == proc_last4) and crm_last4 not in ("0", "0000", "", "nan")

            tier = 1 if email_match else (2 if name_match else 3)

            candidates.append({
                'index': i,
                'proc_amount_crm_currency': proc_amt_crm_cur,
                'email_score': email_sim,
                'currency': proc_cur,
                'rate': rate,
                'row': row,
                'match_tier': tier,
                'name_fallback': tier > 1,
                'last4_match': last4_match,
                'exact_match': amount_match and last4_match
            })

        if not candidates:
            return None, {'failure_reason': 'No valid PayPal candidate'}

        # Pick best candidate by tier then email similarity
        candidates.sort(key=lambda c: (c['match_tier'], -c['email_score']))
        best = candidates[0]

        received_amount = round(best['proc_amount_crm_currency'], 4)
        diff = abs(received_amount - crm_amt)
        comment = ""
        if diff > (abs_tol if best['currency'] == crm_cur else rel_tol):
            comment = f"Amount mismatch of {diff:.2f} {crm_cur}. "
        if best['currency'] != crm_cur:
            comment += "Mixed currencies. "
        if best['name_fallback']:
            comment += "Matched by name fallback."

        payment_status = 1 if abs(best['proc_amount_crm_currency'] - crm_amt) <= (abs_tol if best['currency'] == crm_cur else rel_tol) else 0


        return {
            'crm_date': crm_row.get('crm_date'),
            'crm_email': crm_email,
            'crm_firstname': crm_row.get('crm_firstname', ''),
            'crm_lastname': crm_row.get('crm_lastname', ''),
            'crm_last4': crm_last4,
            'crm_currency': crm_cur,
            'crm_amount': crm_amt,
            'crm_processor_name': crm_row.get('crm_processor_name'),
            'proc_date': best['row'].get('proc_date'),
            'proc_email': best['row'].get('proc_email'),
            'proc_firstname': best['row'].get('proc_firstname', ''),
            'proc_lastname': best['row'].get('proc_lastname', ''),
            'proc_last4': best['row'].get('proc_last4'),
            'proc_currency': best['currency'],
            'proc_amount': best['row'].get('proc_amount'),
            'proc_amount_crm_currency': best['proc_amount_crm_currency'],
            'proc_processor_name': best['row'].get('proc_processor_name'),
            'exchange_rate': best['rate'],
            'email_similarity_avg': best['email_score'],
            'last4_match': best['last4_match'],
            'name_fallback_used': best['name_fallback'],
            'exact_match_used': best['exact_match'],
            'match_status': 1,
            'payment_status': payment_status,
            'comment': comment,
            'matched_proc_indices': [best['index']]
        }, {}

    def _match_skrill_neteller_row(self, crm_row, proc_dict, last4_map, used, proc_config, processor_name):
        crm_tp = str(crm_row.get('crm_tp', '')).strip()
        crm_amt = abs(crm_row['crm_amount'])
        crm_cur = str(crm_row['crm_currency']).strip().upper()
        crm_email = str(crm_row.get('crm_email', '')).strip().lower()
        crm_last4 = str(crm_row.get('crm_last4', '')).strip()

        # Exact 1:1 match on TP + email + amount + currency
        for idx, row in proc_dict.items():
            if idx in used:
                continue

            proc_tp = str(row.get('proc_tp', '')).strip()
            proc_email = str(row.get('proc_email', '')).strip().lower()
            proc_currency = str(row.get('proc_currency', '')).strip().upper()
            proc_amount_raw = row.get('proc_amount')

            if proc_amount_raw is None:
                continue

            # Convert amount to CRM currency
            proc_amount_crm_cur, rate = self.convert_amount(proc_amount_raw, proc_currency, crm_cur)
            if proc_amount_crm_cur is None:
                continue

            if (proc_tp == crm_tp and
                    proc_email == crm_email and
                    proc_currency == crm_cur and
                    abs(proc_amount_crm_cur - crm_amt) < 0.01):
                return {
                    'crm_date': crm_row.get('crm_date'),
                    'crm_email': crm_email,
                    'crm_firstname': crm_row.get('crm_firstname', ''),
                    'crm_lastname': crm_row.get('crm_lastname', ''),
                    'crm_last4': crm_last4,
                    'crm_currency': crm_cur,
                    'crm_amount': crm_amt,
                    'crm_processor_name': crm_row.get('crm_processor_name'),
                    'proc_date': row.get('proc_date'),
                    'proc_email': proc_email,
                    'proc_firstname': row.get('proc_firstname', ''),
                    'proc_lastname': row.get('proc_lastname', ''),
                    'proc_last4': row.get('proc_last4'),
                    'proc_currency': proc_currency,
                    'proc_amount': proc_amount_raw,
                    'proc_amount_crm_currency': proc_amount_crm_cur,
                    'proc_processor_name': row.get('proc_processor_name'),
                    'exchange_rate': 1.0,
                    'email_similarity_avg': 1.0,
                    'last4_match': False,
                    'name_fallback_used': False,
                    'exact_match_used': True,
                    'converted': False,
                    'proc_combo_len': 1,
                    'crm_combo_len': 1,
                    'match_status': 1,
                    'payment_status': 1,
                    'comment': "",
                    'matched_proc_indices': [idx]
                }, {}

        best_candidate = None
        best_score = -1

        for idx, row in proc_dict.items():
            if idx in used:
                continue

            proc_tp = str(row.get('proc_tp', '')).strip()
            proc_email = str(row.get('proc_email', '')).strip().lower()
            proc_currency = str(row.get('proc_currency', '')).strip().upper()
            proc_amount_raw = row.get('proc_amount')

            if proc_amount_raw is None:
                continue

            # Convert amount to CRM currency
            proc_amount_crm_cur, rate = self.convert_amount(proc_amount_raw, proc_currency, crm_cur)
            if proc_amount_crm_cur is None:
                continue

            email_sim = self.enhanced_email_similarity(crm_email, proc_email)
            last4_valid = crm_last4 not in ("0", "0000", "", "nan")
            last4_match = last4_valid and crm_last4 == str(row.get('proc_last4', ''))
            exact_amt = abs(proc_amount_crm_cur - crm_amt) < 0.01
            full_exact = last4_match and exact_amt

            if not (proc_tp == crm_tp or email_sim >= proc_config.email_threshold):
                continue

            score = (int(proc_tp == crm_tp) * 3) + (int(full_exact) * 2) + email_sim + (int(last4_match) * 0.5)

            if score > best_score:
                best_score = score
                best_candidate = {
                    'index': idx,
                    'proc_amount_crm_currency': proc_amount_crm_cur,
                    'email_score': email_sim,
                    'currency': proc_currency,
                    'rate': rate,
                    'row': row,
                    'exact_match': full_exact,
                    'last4_match': last4_match,
                    'primary': proc_tp == crm_tp
                }

        if not best_candidate:
            return None, {'failure_reason': f'No {processor_name} candidate found'}

        proc = best_candidate
        received = proc['proc_amount_crm_currency']
        abs_tol = 0.1
        rel_tol = proc_config.tolerance * crm_amt
        same_cur = proc['currency'] == crm_cur
        tol = abs_tol if same_cur else rel_tol
        err = abs(received - crm_amt)
        payment_status = int(err <= tol)

        comment = ""
        if not payment_status:
            comment = f"Client received {'less' if received < crm_amt else 'more'} {err:.2f} {crm_cur}"
        if not same_cur:
            comment += " (Converted)"
        if proc['exact_match']:
            comment += ""

        row = proc['row']
        return {
            'crm_date': crm_row.get('crm_date'),
            'crm_email': crm_email,
            'crm_firstname': crm_row.get('crm_firstname', ''),
            'crm_lastname': crm_row.get('crm_lastname', ''),
            'crm_last4': crm_last4,
            'crm_currency': crm_cur,
            'crm_amount': crm_amt,
            'crm_processor_name': crm_row.get('crm_processor_name'),
            'proc_date': row.get('proc_date'),
            'proc_email': row.get('proc_email'),
            'proc_firstname': row.get('proc_firstname', ''),
            'proc_lastname': row.get('proc_lastname', ''),
            'proc_last4': row.get('proc_last4'),
            'proc_currency': proc['currency'],
            'proc_amount': proc_amount_raw,
            'proc_amount_crm_currency': proc['proc_amount_crm_currency'],
            'proc_processor_name': row.get('proc_processor_name'),
            'exchange_rate': proc['rate'],
            'email_similarity_avg': proc['email_score'],
            'last4_match': proc['last4_match'],
            'name_fallback_used': False,
            'exact_match_used': proc['exact_match'],
            'converted': not same_cur,
            'proc_combo_len': 1,
            'crm_combo_len': 1,
            'match_status': 1,
            'payment_status': payment_status,
            'comment': comment,
            'matched_proc_indices': [proc['index']]
        }, {}

    def _match_trustpayments_row(self, crm_row, proc_dict, last4_map, used, proc_config):
        def normalize_tp(tp):
            if pd.isna(tp):
                return ''
            if isinstance(tp, float) and tp.is_integer():
                return str(int(tp))
            tp_str = str(tp).strip()
            if tp_str.endswith('.0'):
                tp_str = tp_str[:-2]
            return tp_str

        crm_tp = normalize_tp(crm_row.get("crm_tp", ""))
        crm_last4 = str(crm_row.get("crm_last4", "")).strip()
        crm_email = str(crm_row.get("crm_email", "")).strip().lower()
        crm_first = str(crm_row.get("crm_firstname", "")).strip().lower()
        crm_last = str(crm_row.get("crm_lastname", "")).strip().lower()
        crm_amt = abs(float(crm_row.get("crm_amount", 0)))
        crm_cur = str(crm_row.get("crm_currency", "")).strip().upper()
        abs_tol = 0.1
        rel_tol = proc_config.tolerance * crm_amt

        tier_candidates = {1: [], 2: [], 3: [], 4: [], 5: []}  # added tier 5

        for idx, proc_row in proc_dict.items():
            if idx in used:
                continue

            proc_tp = normalize_tp(proc_row.get("proc_tp", ""))
            proc_last4 = str(proc_row.get("proc_last4", "")).strip()
            proc_email = str(proc_row.get("proc_email", "")).strip().lower()
            proc_first = str(proc_row.get("proc_firstname", "")).strip().lower()
            proc_last = str(proc_row.get("proc_lastname", "")).strip().lower()
            proc_amt_raw = proc_row.get("proc_amount")
            proc_cur = str(proc_row.get("proc_currency", "")).strip().upper()

            if proc_amt_raw is None:
                continue

            proc_amt_crm_cur, rate = self.convert_amount(proc_amt_raw, proc_cur, crm_cur)
            if proc_amt_crm_cur is None:
                continue

            email_sim = self.enhanced_email_similarity(crm_email, proc_email)
            full_name_match = (crm_first == proc_first and crm_last == proc_last)
            first_or_last_name_match = (crm_first == proc_first or crm_last == proc_last)

            # Match tiers logic:
            if crm_tp and proc_tp and crm_tp == proc_tp and crm_last4 and proc_last4 and crm_last4 == proc_last4:
                # TP + last4 exact
                tier_candidates[1].append((idx, proc_row, proc_amt_crm_cur, email_sim))
            elif crm_tp and proc_tp and crm_tp == proc_tp and email_sim > 0.75:
                # TP + email similarity
                tier_candidates[2].append((idx, proc_row, proc_amt_crm_cur, email_sim))
            elif crm_last4 and proc_last4 and crm_last4 == proc_last4 and email_sim > 0.75:
                # last4 + email similarity
                tier_candidates[3].append((idx, proc_row, proc_amt_crm_cur, email_sim))
            elif crm_last4 and proc_last4 and crm_last4 == proc_last4 and first_or_last_name_match:
                # last4 + (first or last name) match, even if TP differs
                tier_candidates[4].append((idx, proc_row, proc_amt_crm_cur, email_sim))
            elif email_sim > 0.75 and full_name_match:
                # email similarity + full name match
                tier_candidates[5].append((idx, proc_row, proc_amt_crm_cur, email_sim))

        # Iterate tiers in order of priority
        for tier in range(1, 6):
            candidates = tier_candidates[tier]
            if not candidates:
                continue

            best_candidate = None
            best_err = float('inf')

            for idx, proc_row, proc_amt_crm_cur, email_sim in candidates:
                err = abs(proc_amt_crm_cur - crm_amt)
                tol = abs_tol if crm_cur == proc_row.get("proc_currency", "").strip().upper() else rel_tol
                if err <= tol and err < best_err:
                    best_candidate = (idx, proc_row, proc_amt_crm_cur, email_sim, err)
                    best_err = err

            if best_candidate:
                idx, proc_row, proc_amt_crm_cur, email_sim, err = best_candidate
                received = round(proc_amt_crm_cur, 4)
                payment_status = int(err <= tol)
                comment = ""
                if not payment_status:
                    comment = f"Amount {'under' if received < crm_amt else 'over'} by {abs(received - crm_amt):.2f} {crm_cur}"

                # Mark name fallback used if tier 4 or 5
                name_fallback_used = (tier in [4, 5])

                return {
                    'crm_date': crm_row.get('crm_date'),
                    'crm_email': crm_email,
                    'crm_firstname': crm_first,
                    'crm_lastname': crm_last,
                    'crm_last4': crm_last4,
                    'crm_currency': crm_cur,
                    'crm_amount': crm_amt,
                    'crm_processor_name': "trustpayments",
                    'proc_date': proc_row.get('proc_date'),
                    'proc_email': proc_row.get('proc_email'),
                    'proc_firstname': proc_row.get('proc_firstname', ''),
                    'proc_lastname': proc_row.get('proc_lastname', ''),
                    'proc_last4': proc_row.get('proc_last4'),
                    'proc_currency': proc_row.get('proc_currency'),
                    'proc_amount': proc_row.get('proc_amount'),
                    'proc_amount_crm_currency': proc_amt_crm_cur,
                    'proc_processor_name': proc_row.get('proc_processor_name'),
                    'email_similarity_avg': round(email_sim, 4),
                    'last4_match': tier in [1, 3, 4],
                    'name_fallback_used': name_fallback_used,
                    'exact_match_used': (tier == 1),
                    'proc_combo_len': 1,
                    'crm_combo_len': 1,
                    'match_status': 1,
                    'payment_status': payment_status,
                    'comment': comment,
                    'matched_proc_indices': [idx]
                }, {'trustpayments_candidate': best_candidate}

        return None, {'failure_reason': 'No TrustPayments match'}

    def _create_unmatched_crm_record(self, crm_row):
        return {
            'crm_date': crm_row.get('crm_date'),
            'crm_email': crm_row.get('crm_email'),
            'crm_firstname': crm_row.get('crm_firstname'),
            'crm_lastname': crm_row.get('crm_lastname'),
            'crm_last4': crm_row.get('crm_last4'),
            'crm_currency': crm_row.get('crm_currency'),
            'crm_amount': crm_row.get('crm_amount'),
            'crm_processor_name': crm_row.get('crm_processor_name'),
            'proc_date': [],
            'proc_email': [],
            'proc_last4': [],
            'proc_currency': [],
            'proc_amount': [],
            'proc_processor_name': None,
            'email_similarity_avg': None,
            'last4_match': False,
            'name_fallback_used': False,
            'exact_match_used': False,
            'match_status': 0,
            'payment_status': 0,
            'comment': "No matching processor row found",
            'matched_proc_indices': []
        }

    def _estimate_runtime(self, crm_df, proc_dict, last4_map):
        samples = list(crm_df.iterrows())[:min(5, len(crm_df))]
        if not samples:
            return

        self.logger.info(f"Estimating runtime with {len(samples)} sample rows...")
        times = []
        for _, row in samples:
            t0 = time.time()
            # Use the appropriate matching method based on processor
            proc_name = row.get('crm_processor_name', '').lower()
            proc_config = self.get_processor_config(proc_name)
            if proc_config.matching_logic == "paypal":
                self._match_paypal_row(row, proc_dict, last4_map, set(), proc_config)
            else:
                self._match_standard_row(row, proc_dict, last4_map, set(), proc_config)
            times.append(time.time() - t0)

        self.estimated_time = sum(times) / len(times) * len(crm_df)
        self.logger.info(f"Estimated total runtime: {timedelta(seconds=self.estimated_time)}")

        if (self.config['auto_adjust'] and
                self.config['timeout'] and
                self.estimated_time > self.config['timeout'] * 0.8):
            self._adjust_parameters()

    def _adjust_parameters(self):
        self.logger.warning("High runtime estimated! Adjusting parameters...")
        self.config.update({
            'top_candidates': min(15, self.config['top_candidates'])
        })
        self.parameter_adjusted = True

    def _check_timeout(self):
        return (self.config['timeout'] and
                (datetime.now() - self.start_time).total_seconds() > self.config['timeout'])

    def _update_eta(self, total, done):
        elapsed = (datetime.now() - self.start_time).total_seconds()
        if done == 0:
            return

        avg_time_per_row = elapsed / done
        remaining_rows = total - done
        eta_seconds = avg_time_per_row * remaining_rows
        self.logger.info(f"Processed {done}/{total} rows. ETA: {timedelta(seconds=eta_seconds)}")
