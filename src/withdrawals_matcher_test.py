import numpy as np
from difflib import SequenceMatcher
from itertools import combinations
from datetime import datetime, timedelta
from functools import lru_cache
import pandas as pd
import logging, threading, time
from concurrent.futures import ThreadPoolExecutor, as_completed


class ProcessorConfig:
    def __init__(self,
                 email_threshold=0.65,
                 name_match_threshold=0.7,
                 require_last4=True,
                 require_email=True,
                 enable_name_fallback=True,
                 enable_exact_match=True,
                 max_combo=20,
                 tolerance=0.05,
                 matching_logic="standard"):
        self.email_threshold = email_threshold
        self.name_match_threshold = name_match_threshold
        self.require_last4 = require_last4
        self.require_email = require_email
        self.enable_name_fallback = enable_name_fallback
        self.enable_exact_match = enable_exact_match
        self.max_combo = max_combo
        self.tolerance = tolerance
        self.matching_logic = matching_logic  # "standard" or "paypal"


# Processor-specific configurations
PROCESSOR_CONFIGS = {
    'safecharge': ProcessorConfig(
        email_threshold=0.75,
        require_last4=True,
        require_email=True,
        tolerance=0.01,
    ),
    'powercash': ProcessorConfig(
        email_threshold=0.75,
        require_last4=True,
        require_email=True,
        tolerance=0.01,
    ),
    'paypal': ProcessorConfig(
        email_threshold=0.8,
        name_match_threshold=0.75,
        require_last4=False,
        require_email=False,
        enable_name_fallback=True,
        matching_logic="paypal"
    ),
    'shift4': ProcessorConfig(
        email_threshold=0.75,
        name_match_threshold=0.70,
        require_last4=True,
        require_email=True,
        enable_name_fallback=True,
        enable_exact_match=True,
        max_combo=20,
        tolerance=0.01,
        matching_logic="shift4"
    ),
    'skrill': ProcessorConfig(
        email_threshold=0.75,
        require_last4=False,
        require_email=True,
        enable_name_fallback=False,
        enable_exact_match=False,
        max_combo=5,
        tolerance=0.02,
        matching_logic="skrill"
    ),
    'neteller': ProcessorConfig(
        email_threshold=0.75,
        require_last4=False,
        require_email=True,
        enable_name_fallback=False,
        enable_exact_match=False,
        max_combo=5,
        tolerance=0.02,
        matching_logic="skrill"   # we'll just reuse the Skrill logic
    ),
    'bitpay': ProcessorConfig(
        email_threshold=0.75,
        require_last4=False,
        require_email=True,
        enable_name_fallback=True,
        enable_exact_match=False,
        max_combo=5,
        tolerance=0.02,
        matching_logic="bitpay"
    ),
    'zotapay_paymentasia': ProcessorConfig(  # Add this new configuration
        email_threshold=0.75,  # Not critical since we're matching by TP
        require_last4=False,
        require_email=False,
        enable_name_fallback=False,
        enable_exact_match=True,
        max_combo=1,  # Since we're doing 1:1 matching
        tolerance=0.02,  # 2% tolerance
        matching_logic="zotapay_paymentasia"
    ),
    "trustpayments": ProcessorConfig(
        email_threshold=0.75,
        name_match_threshold=0.75,
        require_last4=False,
        require_email=False,
        enable_name_fallback=False,
        enable_exact_match=False,
        max_combo=5,
        tolerance=0.02,
        matching_logic="trustpayments"
    ),
    # ... other processors ...
}


def load_exchange_rates(csv_path):
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
        if not e1 or not e2:
            return 0.0
        l1 = str(e1).lower().split('@')[0] if '@' in e1 else str(e1).lower()
        l2 = str(e2).lower().split('@')[0] if '@' in e2 else str(e2).lower()
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

    def generate_report(self):
        return {
            'metrics': self.metrics,
            'diagnostics': self.diagnostics if self.config['enable_diagnostics'] else None,
            'estimated_time': self.estimated_time,
            'parameters_adjusted': self.parameter_adjusted
        }

    def _cross_processor_last_chance(self, crm_df, processor_df, used_crm, used_proc, matches):
        # Step 1: Find relevant processors
        cross_processors = {"shift4", "safecharge", "powercash", "paypal", "trustpayments"}
        # Step 2: Unmatched CRM rows for these processors
        unmatched_key_crm = [
            (idx, row)
            for idx, row in crm_df.iterrows()
            if str(row.get('crm_processor_name', '')).strip().lower() in cross_processors and idx not in used_crm
        ]
        if not unmatched_key_crm:
            return
        # Step 3: Unused processor rows in these processors
        cross_proc_rows = processor_df[
            processor_df['proc_processor_name'].str.lower().str.strip().isin(cross_processors)
            & (~processor_df.index.isin(used_proc))
            ]
        if cross_proc_rows.empty:
            return
        cross_proc_dict = cross_proc_rows.to_dict('index')
        cross_proc_last4_map = cross_proc_rows.groupby('proc_last4_digits').indices

        for idx, crm_row in unmatched_key_crm:
            # Use standard matching logic (last resort)
            result = self._match_standard_row(
                crm_row, cross_proc_dict, cross_proc_last4_map, used_proc, self.get_processor_config('safecharge')
            )
            if result and result[0]:
                match, diag = result
                match['comment'] = (match.get('comment', '') + " [Cross-processor fallback]").strip()
                match['cross_processor_fallback'] = True
                matches.append(match)
                used_crm.add(idx)
                used_proc.update(match['matched_proc_indices'])
                self.metrics['matched_fallback'] += 1

    def match_withdrawals(self, crm_df, processor_df):
        self.start_time = datetime.now()
        self.metrics['total_crm'] = len(crm_df)
        used_proc, used_crm, matches = set(), set(), []
        last4_map = processor_df.groupby('proc_last4_digits').indices
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
                    if k == 'proc_emails':
                        ordered['proc_tp'] = proc_tp_vals
                    if k == 'crm_lastname':
                        ordered['crm_tp'] = crm_tp_val
                match = ordered

                matches.append(match)
                used_crm.add(idx)
                used_proc.update(match['matched_proc_indices'])
                self.metrics['matched_main'] += 1
                self.metrics['combo_distribution'][(match.get('crm_combo_len', 1), match.get('proc_combo_len', 1))] = \
                    self.metrics['combo_distribution'].get(
                        (match.get('crm_combo_len', 1), match.get('proc_combo_len', 1)), 0
                    ) + 1
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
                    if k == 'proc_emails':
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
                    'proc_dates': [row.get('proc_date')],
                    'proc_emails': [row.get('proc_emails')],
                    'proc_last4_digits': [row.get('proc_last4_digits')],
                    'proc_currencies': [row.get('proc_currency')],
                    'proc_total_amounts': [row.get('proc_total_amount')],
                    'proc_processor_name': row.get('proc_processor_name'),
                    'proc_firstnames': [row.get('proc_firstname')],
                    'proc_lastnames': [row.get('proc_lastname')],
                    'converted_amount_total': None,
                    'exchange_rates': None,
                    'email_similarity_avg': None,
                    'last4_match': None,
                    'name_fallback_used': False,
                    'exact_match_used': False,
                    'converted': False,
                    'crm_combo_len': 0,
                    'proc_combo_len': 1,
                    'match_status': 0,
                    'payment_status': 0,
                    'comment': "No matching CRM row found",
                    'matched_proc_indices': [idx],
                }
                # build in order so that proc_tp follows proc_emails
                entry = {}
                for k, v in base.items():
                    entry[k] = v
                    if k == 'proc_emails':
                        entry['proc_tp'] = [row.get('proc_tp', '')]
                matches.append(entry)

        self.metrics['processing_time'] = (datetime.now() - self.start_time).total_seconds()
        self.logger.info(f"Total processing time: {timedelta(seconds=self.metrics['processing_time'])}")
        self.merge_crm_batch_payments(matches, tolerance=0.01)
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
        if (crm_last4 not in ("0", "0000", "", "nan") and
                crm_last4 in last4_map and proc_config.require_last4):
            indices = last4_map[crm_last4]

        for i in indices:
            if i in used:
                continue
            row = proc_dict[i]
            conv, rate = self.convert_amount(row['proc_total_amount'], row['proc_currency'], crm_cur)
            if conv is None:
                continue

            email_sim = self.enhanced_email_similarity(crm_email, row['proc_emails'])
            last4_match = (crm_last4 == str(row['proc_last4_digits'])) and (crm_last4 not in ("0", "0000", "", "nan"))
            # Instead of using raw amount & currency, we now compare the converted amount.
            converted_amount_match = abs(conv - crm_amt) < 0.01

            full_exact_match = last4_match and converted_amount_match

            name_fallback = False
            if proc_config.enable_name_fallback:
                if crm_first:
                    name_fallback = self.name_in_email(crm_first, row['proc_emails'])
                if not name_fallback and crm_last:
                    name_fallback = self.name_in_email(crm_last, row['proc_emails'])

            if proc_config.enable_exact_match and full_exact_match:
                candidates.append({
                    'index': i, 'converted_amount': conv, 'email_score': email_sim,
                    'currency': row['proc_currency'], 'rate': rate, 'row_data': row,
                    'last4_match': last4_match, 'name_fallback': name_fallback,
                    'exact_match': True
                })
                continue

            # Relaxed candidate filtering:
            if proc_config.require_email and email_sim < proc_config.email_threshold and not name_fallback:
                continue
            # NEW: Accept if email is very strong even if last4 doesn't match.
            # Removed the converted_amount_match condition here.
            if proc_config.require_last4:
                if not last4_match:
                    if email_sim >= 0.75:
                        pass  # allow override of last4 mismatch based on strong email similarity
                    else:
                        continue

            candidates.append({
                'index': i, 'converted_amount': conv, 'email_score': email_sim,
                'currency': row['proc_currency'], 'rate': rate, 'row_data': row,
                'last4_match': last4_match, 'name_fallback': name_fallback,
                'exact_match': False
            })

        candidates.sort(key=lambda x: (
            -x['exact_match'],
            -x['email_score'],
            -x['last4_match'],
            -x['name_fallback']
        ))
        candidates = candidates[:self.config['top_candidates']]

        # --- Combination selection with fallback ---
        best_strict_combo = None
        best_strict_score = 0.0
        best_fallback_combo = None
        best_fallback_error = None

        abs_tol = 0.1
        rel_tol = proc_config.tolerance * crm_amt
        n = len(candidates)

        for k in range(1, min(proc_config.max_combo, n) + 1):
            for combo_idxs in combinations(range(n), k):
                combo = [candidates[i] for i in combo_idxs]
                same = all(c['currency'] == crm_cur for c in combo)
                total = sum(c['converted_amount'] for c in combo)
                tol_here = abs_tol if same else rel_tol
                diff = total - crm_amt
                error = abs(diff)
                avg_score = sum(c['email_score'] for c in combo) / k
                # Strict candidate: within tolerance.
                if error <= tol_here:
                    if avg_score > best_strict_score:
                        best_strict_score = avg_score
                        best_strict_combo = {'combo': combo, 'k': k, 'total_amount': total, 'exact_currency': same}
                        if avg_score >= 0.99:
                            break
                else:
                    # Record fallback candidate (the one with the minimal error).
                    if best_fallback_combo is None or error < best_fallback_error:
                        best_fallback_error = error
                        best_fallback_combo = {'combo': combo, 'k': k, 'total_amount': total, 'exact_currency': same,
                                               'diff': diff, 'avg_score': avg_score}
            if best_strict_combo is not None and best_strict_score >= 0.99:
                break

        if best_strict_combo is not None:
            best_combo = best_strict_combo
            strict_match = True
            best_score = best_strict_score
        elif best_fallback_combo is not None:
            best_combo = best_fallback_combo
            strict_match = False
            best_score = best_fallback_combo['avg_score']
        else:
            best_combo = None

        if best_combo:
            c = best_combo['combo']
            received_amount = round(best_combo['total_amount'], 4)
            tol_used = abs_tol if best_combo.get('exact_currency') else rel_tol
            diff = received_amount - crm_amt
            abs_diff = abs(diff)
            if strict_match:
                payment_status = 1
                comment = ""
            else:
                payment_status = 0
                if diff < 0:
                    comment = f"Client received less {abs_diff:.2f} {crm_cur}"
                else:
                    comment = f"Client received more {abs_diff:.2f} {crm_cur}"
            return {
                'crm_date': crm_row.get('crm_date'),
                'crm_email': crm_email,
                'crm_firstname': crm_row.get('crm_firstname', ''),
                'crm_lastname': crm_row.get('crm_lastname', ''),
                'crm_last4': crm_last4,
                'crm_currency': crm_cur,
                'crm_amount': crm_amt,
                'crm_processor_name': crm_row.get('crm_processor_name'),
                'proc_dates': [r['row_data']['proc_date'] for r in c],
                'proc_emails': [r['row_data']['proc_emails'] for r in c],
                'proc_firstnames': [r['row_data'].get('proc_firstname', '') for r in c],
                'proc_lastnames': [r['row_data'].get('proc_lastname', '') for r in c],
                'proc_last4_digits': [r['row_data']['proc_last4_digits'] for r in c],
                'proc_currencies': [r['currency'] for r in c],
                'proc_total_amounts': [r['row_data']['proc_total_amount'] for r in c],
                'proc_processor_name': next(iter({r['row_data']['processor_name'] for r in c}), None),
                'converted_amount_total': received_amount,
                'exchange_rates': [r['rate'] for r in c],
                'email_similarity_avg': round(best_score, 4),
                'last4_match': any(r['last4_match'] for r in c),
                'name_fallback_used': any(r.get('name_fallback', False) for r in c),
                'exact_match_used': any(r.get('exact_match', False) for r in c),
                'converted': not best_combo['exact_currency'],
                'proc_combo_len': len([r['index'] for r in c]),  # number of proc rows matched
                'crm_combo_len': 1,  # always 1 during matching, update if batching CRM
                'match_status': 1,
                'payment_status': payment_status,
                'comment': comment,
                'matched_proc_indices': [r['index'] for r in c]
            }, {'best_combo': best_combo}

    def _match_zotapay_paymentasia_row(self, crm_row, proc_dict, last4_map, used):
        # Get processor-specific configuration
        proc_config = self.get_processor_config("zotapay_paymentasia")

        crm_tp = str(crm_row.get('crm_tp', '')).strip()
        crm_amt = abs(crm_row['crm_amount'])
        crm_cur = str(crm_row['crm_currency']).strip().upper()

        # If CRM TP is missing, skip matching
        if not crm_tp:
            return None, {'failure_reason': 'Missing CRM TP'}

        # Tolerance settings
        abs_tol = 0.1  # Absolute tolerance for same currency
        rel_tol = proc_config.tolerance * crm_amt  # Relative tolerance for converted amounts

        for idx, row in proc_dict.items():
            if idx in used:
                continue

            proc_tp = str(row.get('proc_tp', '')).strip()
            if proc_tp != crm_tp:
                continue

            # Get processor values
            proc_amt = abs(row.get('proc_total_amount', 0))
            proc_cur = str(row.get('proc_currency', '')).strip().upper()

            # Convert amount if currencies differ
            if proc_cur == crm_cur:
                converted_amt = proc_amt
                rate = 1.0
                converted = False
                tolerance = abs_tol
            else:
                converted_amt, rate = self.convert_amount(proc_amt, proc_cur, crm_cur)
                if converted_amt is None:
                    # Conversion failed - proceed with direct comparison
                    converted_amt = proc_amt
                    rate = 1.0
                    converted = False
                    tolerance = abs_tol
                else:
                    converted = True
                    tolerance = rel_tol

            # Check if amounts match with tolerance
            diff = converted_amt - crm_amt
            abs_diff = abs(diff)
            amount_match = abs_diff <= tolerance
            payment_status = 1 if amount_match else 0

            # Generate comment
            if amount_match:
                comment = ""
            else:
                if diff < 0:
                    comment = f"Client received less {abs_diff:.2f} {crm_cur}"
                else:
                    comment = f"Client received more {abs_diff:.2f} {crm_cur}"

                if converted:
                    comment += f" (Converted: {proc_amt:.2f} {proc_cur} → {converted_amt:.2f} {crm_cur})"
                elif proc_cur != crm_cur:
                    comment += " (No conversion rate available)"

            return {
                # CRM data
                'crm_date': crm_row.get('crm_date'),
                'crm_email': str(crm_row.get('crm_email', '')).strip().lower(),
                'crm_firstname': crm_row.get('crm_firstname', ''),
                'crm_lastname': crm_row.get('crm_lastname', ''),
                'crm_last4': str(crm_row.get('crm_last4', '')).strip(),
                'crm_currency': crm_cur,
                'crm_amount': crm_amt,
                'crm_processor_name': crm_row.get('crm_processor_name', 'zotapay_paymentasia'),

                # Processor data
                'proc_dates': [row.get('proc_date')],
                'proc_emails': [str(row.get('proc_emails', '')).strip().lower()],
                'proc_firstnames': [row.get('proc_firstname', '')],
                'proc_lastnames': [row.get('proc_lastname', '')],
                'proc_last4_digits': [row.get('proc_last4_digits')],
                'proc_currencies': [proc_cur],
                'proc_total_amounts': [proc_amt],
                'proc_processor_name': row.get('proc_processor_name', 'zotapay_paymentasia'),

                # Matching details
                'converted_amount_total': converted_amt,
                'exchange_rates': [rate],
                'email_similarity_avg': 1.0,
                'last4_match': False,
                'name_fallback_used': False,
                'exact_match_used': True,
                'converted': converted,
                'proc_combo_len':1,
                'crm_combo_len': 1,
                'match_status': 1,
                'payment_status': payment_status,
                'comment': comment,
                'matched_proc_indices': [idx]
            }, {}

        return None, {'failure_reason': f'No zotapay_paymentasia match for TP: {crm_tp}'}
    def _match_bitpay_row(self, crm_row, proc_dict, last4_map, used, proc_config):
        crm_cur = crm_row['crm_currency']
        crm_amt = crm_row['crm_amount']
        crm_email = crm_row['crm_email']
        crm_first = str(crm_row.get('crm_firstname', '')).lower().strip()
        crm_last = str(crm_row.get('crm_lastname', '')).lower().strip()

        candidates = []
        for i, row in proc_dict.items():
            if i in used:
                continue
            conv, rate = self.convert_amount(row['proc_total_amount'], row['proc_currency'], crm_cur)
            if conv is None:
                continue

            proc_email = str(row['proc_emails']).strip().lower()
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

            candidates.append({
                'index': i,
                'converted_amount': conv,
                'email_score': email_sim,
                'currency': row['proc_currency'],
                'rate': rate,
                'row_data': row,
                'name_fallback': name_match and not email_match
                })

        candidates.sort(key=lambda c: (-int(c['email_score'] >= 0.75), -c['email_score']))
        candidates = candidates[:self.config['top_candidates']]

        abs_tol = 0.1
        rel_tol = proc_config.tolerance * crm_amt
        best = None
        best_score = 0

        for k in range(1, min(proc_config.max_combo, len(candidates)) + 1):
            for combo in combinations(candidates, k):
                total = sum(c['converted_amount'] for c in combo)
                same_cur = all(c['currency'] == crm_cur for c in combo)
                tol = abs_tol if same_cur else rel_tol
                err = abs(total - crm_amt)
                score = sum(c['email_score'] for c in combo) / k

                if err <= tol and score > best_score:
                    best = {
                        'combo': combo,
                        'k': k,
                        'total': total,
                        'score': score,
                        'exact_currency': same_cur
                        }
                    best_score = score
            if best_score >= 0.99:
                break

        if not best:
            return None, {'failure_reason': 'No valid BitPay combo'}

        c = best['combo']
        payment_status = 1
        comment = "" if best['score'] >= 0.99 else f"Matched with lower email score: {best['score']:.2f}"

        return {
            'crm_date': crm_row.get('crm_date'),
            'crm_email': crm_email,
            'crm_firstname': crm_row.get('crm_firstname', ''),
            'crm_lastname': crm_row.get('crm_lastname', ''),
            'crm_last4': crm_row.get('crm_last4'),
            'crm_currency': crm_cur,
            'crm_amount': crm_amt,
            'crm_processor_name': 'bitpay',
            'proc_dates': [r['row_data']['proc_date'] for r in c],
            'proc_emails': [r['row_data']['proc_emails'] for r in c],
            'proc_firstnames': [r['row_data'].get('proc_firstname', '') for r in c],
            'proc_lastnames': [r['row_data'].get('proc_lastname', '') for r in c],
            'proc_last4_digits': [r['row_data']['proc_last4_digits'] for r in c],
            'proc_currencies': [r['currency'] for r in c],
            'proc_total_amounts': [r['row_data']['proc_total_amount'] for r in c],
            'proc_processor_name': 'bitpay',
            'converted_amount_total': round(best['total'], 4),
            'exchange_rates': [r['rate'] for r in c],
            'email_similarity_avg': round(best['score'], 4),
            'last4_match': False,
            'name_fallback_used': any(r.get('name_fallback') for r in c),
            'exact_match_used': False,
            'converted': not best['exact_currency'],
            'proc_combo_len': best['k'],
            'crm_combo_len': 1,
            'match_status': 1,
            'payment_status': payment_status,
            'comment': comment,
            'matched_proc_indices': [r['index'] for r in c]
        }, {'bitpay_combo': best}

    def merge_crm_batch_payments(self, matches, tolerance=0.01, max_proc_combo=6):
        """
        Robustly batch CRM and processor rows for same client+processor+currency.
        For any group with unmatched CRM, try *all* combinations of processor rows
        for that processor+currency to best explain all CRM rows as a batch.
        - Avoids batching rows that are exact 1:1 matches.
        - Leaves unmatched rows as usual.
        """
        import numpy as np
        from collections import defaultdict
        from itertools import combinations

        # Build CRM groups
        crm_group_map = defaultdict(list)
        for idx, row in enumerate(matches):
            fname = (str(row.get('crm_firstname', '') or '')).strip().lower()
            lname = (str(row.get('crm_lastname', '') or '')).strip().lower()
            proc = (str(row.get('crm_processor_name', '') or '')).strip().lower()
            currency = (str(row.get('crm_currency', '') or '')).strip().upper()
            if fname or lname:
                crm_group_map[(fname, lname, proc, currency)].append((idx, row))

        # Build processor row list (simulate processor_df in-memory for this phase)
        processor_rows = []
        for idx, row in enumerate(matches):
            # Any row with at least processor-side info and amount
            if row.get('proc_total_amounts') and len(row['proc_total_amounts']) == 1:
                proc_amt = row['proc_total_amounts'][0]
                if isinstance(proc_amt, (int, float)):
                    processor_rows.append((idx, row))

        used_crm = set()
        used_proc = set()
        new_matches = []

        for group_key, group_rows in crm_group_map.items():
            if not group_rows:
                continue
            fname, lname, proc, currency = group_key
            group_crm_indices = [idx for idx, _ in group_rows]
            group_crm_rows = [r for _, r in group_rows]
            any_unmatched = any(r.get('match_status', 0) == 0 for r in group_crm_rows)
            if not any_unmatched:
                continue

            # --- Find all candidate processor rows for this group ---
            proc_candidates = [
                (idx, row)
                for idx, row in processor_rows
                if (str(row.get('proc_processor_name', '') or '').strip().lower() == proc and
                    str(row.get('proc_currencies', [''])[0]).strip().upper() == currency and
                    idx not in used_proc)
            ]

            # If there are more processor rows than max_proc_combo, take the most recent ones
            if len(proc_candidates) > max_proc_combo:
                proc_candidates = proc_candidates[-max_proc_combo:]

            crm_sum = sum((r.get('crm_amount', 0) or 0) for r in group_crm_rows)
            crm_n = len(group_crm_rows)
            proc_amounts = [row['proc_total_amounts'][0] for _, row in proc_candidates]

            # --- Now, try all processor row combinations ---
            best_combo = None
            best_error = float('inf')
            for k in range(1, min(len(proc_candidates), crm_n * 2) + 1):
                for combo in combinations(proc_candidates, k):
                    indices = [idx for idx, _ in combo]
                    total = sum(row['proc_total_amounts'][0] for _, row in combo)
                    diff = abs(total - crm_sum)
                    if diff <= tolerance * max(abs(crm_sum), abs(total), 1):
                        if diff < best_error:
                            best_combo = combo
                            best_error = diff
                if best_combo and best_error <= tolerance * max(abs(crm_sum),
                                                                abs(best_combo[0][1]['proc_total_amounts'][0]), 1):
                    break

            if not best_combo:
                continue  # Can't batch; fallback to original logic

            # --- If any CRM row can be perfectly explained by a single processor row, treat it as 1:1 ---
            skip_batch = False
            for idx_crm, crm_row in group_rows:
                amt_crm = crm_row.get('crm_amount', 0) or 0
                for idx_proc, proc_row in proc_candidates:
                    amt_proc = proc_row['proc_total_amounts'][0]
                    if abs(amt_crm - amt_proc) <= tolerance * max(abs(amt_crm), abs(amt_proc), 1):
                        # 1:1 match exists; don't batch these, let normal logic handle them
                        skip_batch = True
                        break
                if skip_batch:
                    break
            if skip_batch:
                continue  # Let those be handled by standard logic, don't batch

            # Otherwise, create batched result
            batch_crm_indices = [idx for idx, _ in group_rows]
            batch_proc_indices = [idx for idx, _ in best_combo]
            batch_proc_amounts = [row['proc_total_amounts'][0] for _, row in best_combo]

            base_row = group_crm_rows[0].copy()
            base_row['crm_amount'] = crm_sum
            base_row['crm_combo_len'] = crm_n
            base_row['proc_combo_len'] = len(batch_proc_indices)
            base_row['converted_amount_total'] = sum(batch_proc_amounts)
            base_row['matched_proc_indices'] = batch_proc_indices
            base_row['payment_status'] = int(best_error <= tolerance * abs(crm_sum))
            base_row['match_status'] = 1
            base_row['comment'] = (
                    f"Batched: {crm_n} CRM withdrawals merged (sum={crm_sum:.2f} {currency}) "
                    f"to {sum(batch_proc_amounts):.2f} {currency} across {len(batch_proc_indices)} proc rows."
                    + (f" Amount mismatch: {best_error:.2f} {currency}" if best_error > 0 else "")
            )

            new_matches.append(base_row)
            used_crm.update(batch_crm_indices)
            used_proc.update(batch_proc_indices)

        # Final list: remove any merged CRM/processor rows, append new batches
        matches[:] = [r for idx, r in enumerate(matches) if idx not in used_crm and idx not in used_proc]
        matches.extend(new_matches)

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
            'proc_dates': [],
            'proc_emails': [],
            'proc_last4_digits': [],
            'proc_currencies': [],
            'proc_total_amounts': [],
            'proc_processor_name': None,
            'converted_amount_total': None,
            'exchange_rates': [],
            'email_similarity_avg': None,
            'last4_match': False,
            'name_fallback_used': False,
            'exact_match_used': False,
            'converted': False,
            'crm_combo_len': 1,
            'proc_combo_len': 0,
            'match_status': 0,
            'payment_status': 0,
            'comment': "No matching processor row found",
            'matched_proc_indices': []
        }

    def _match_shift4_row(self, crm_row, proc_dict, last4_map, used, proc_config):
        # --- CRM fields ---
        crm_last4 = str(crm_row['crm_last4']) if not pd.isna(crm_row['crm_last4']) else ''
        crm_cur = crm_row['crm_currency']
        crm_amt = crm_row['crm_amount']
        crm_amt = crm_row['crm_amount']
        crm_email = (crm_row.get('crm_email') or '').lower()
        crm_first = str(crm_row.get('crm_firstname', '')).lower().strip()
        crm_last = str(crm_row.get('crm_lastname', '')).lower().strip()

        # --- Candidate indices by last4 only ---
        if crm_last4 and crm_last4 in last4_map and proc_config.require_last4:
            indices = [i for i in last4_map[crm_last4] if i not in used]
        else:
            indices = [i for i in proc_dict if i not in used]

        candidates = []
        for i in indices:
            row = proc_dict[i]
            conv, rate = self.convert_amount(row['proc_total_amount'],
                                             row['proc_currency'], crm_cur)
            if conv is None:
                continue

            # must match last4
            if crm_last4 != str(row['proc_last4_digits']):
                continue

            # --- 2-letter email prefix match? ---
            loc_crm = crm_email.split('@')[0]
            loc_proc = str(row.get('proc_emails', '')).lower().split('@')[0]
            email_prefix = bool(loc_crm and loc_proc and loc_crm[:2] == loc_proc[:2])
            email_score = 1 if email_prefix else 0

            # --- 3-letter name prefix matches? ---
            proc_first = str(row.get('proc_firstname', '')).lower().strip()
            proc_last = str(row.get('proc_lastname', '')).lower().strip()
            first_prefix = bool(crm_first and proc_first and crm_first[:3] == proc_first[:3])
            last_prefix = bool(crm_last and proc_last and crm_last[:3] == proc_last[:3])

            # require at least one prefix match
            if not (email_prefix or first_prefix or last_prefix):
                continue

            candidates.append({
                'index': i,
                'amount': conv,
                'rate': rate,
                'row': row,
                'email_prefix': email_prefix,
                'first_prefix': first_prefix,
                'last_prefix': last_prefix,
                'email_score': email_score
            })

        # sort by email_prefix, first_prefix, last_prefix
        candidates.sort(key=lambda c: (
            -int(c['email_prefix']),
            -int(c['first_prefix']),
            -int(c['last_prefix'])
        ))

        # --- combination search ---
        abs_tol = 0.1
        rel_tol = proc_config.tolerance * crm_amt
        best_strict = None
        best_score = -1
        best_fallback = None
        best_err = float('inf')

        for k in range(1, min(proc_config.max_combo, len(candidates)) + 1):
            for combo_idxs in combinations(range(len(candidates)), k):
                combo = [candidates[j] for j in combo_idxs]
                same_cur = all(c['row']['proc_currency'] == crm_cur for c in combo)
                total = sum(c['amount'] for c in combo)
                err = abs(total - crm_amt)
                tol = abs_tol if same_cur else rel_tol
                avg_sim = sum(c['email_score'] for c in combo) / k

                if err <= tol:
                    if avg_sim > best_score:
                        best_score = avg_sim
                        best_strict = (combo, k, total, same_cur)
                else:
                    if err < best_err:
                        best_err = err
                        best_fallback = (combo, k, total, same_cur, err, avg_sim)

        # choose strict over fallback
        if best_strict:
            combo, k, total, same = best_strict
            strict = True
            score = best_score
        elif best_fallback:
            combo, k, total, same, err, score = best_fallback
            strict = False
        else:
            return None, {'failure_reason': 'No valid Shift4 match'}

        received = round(total, 4)
        payment_status = 1 if strict else 0
        comment = "" if strict else f"Amount {'under' if received < crm_amt else 'over'} by {abs(received - crm_amt):.2f} {crm_cur}"

        return {
            'crm_date': crm_row.get('crm_date'),
            'crm_email': crm_email,
            'crm_firstname': crm_row.get('crm_firstname', ''),
            'crm_lastname': crm_row.get('crm_lastname', ''),
            'crm_last4': crm_last4,
            'crm_currency': crm_cur,
            'crm_amount': crm_amt,
            'crm_processor_name': 'shift4',
            'proc_dates': [c['row']['proc_date'] for c in combo],
            'proc_emails': [c['row']['proc_emails'] for c in combo],
            'proc_firstnames': [c['row'].get('proc_firstname', '') for c in combo],
            'proc_lastnames': [c['row'].get('proc_lastname', '') for c in combo],
            'proc_last4_digits': [c['row']['proc_last4_digits'] for c in combo],
            'proc_currencies': [c['row']['proc_currency'] for c in combo],
            'proc_total_amounts': [c['row']['proc_total_amount'] for c in combo],
            'converted_amount_total': received,
            'exchange_rates': [c['rate'] for c in combo],
            'email_similarity_avg': score,  # now just 1 or 0
            'last4_match': True,
            'converted': not same,
            'proc_combo_len': k,
            'crm_combo_len': 1,
            'match_status': 1,
            'payment_status': payment_status,
            'comment': comment,
            'matched_proc_indices': [c['index'] for c in combo]
        }, {'best_combo': combo}

    def _match_paypal_row(self, crm_row, proc_dict, last4_map, used, proc_config):
        crm_cur = crm_row['crm_currency']
        crm_amt = crm_row['crm_amount']
        crm_email = crm_row['crm_email']
        crm_first = str(crm_row.get('crm_firstname', '')).lower().strip()
        crm_last = str(crm_row.get('crm_lastname', '')).lower().strip()

        candidates = []
        indices = [i for i in proc_dict if i not in used]

        for i in indices:
            row = proc_dict[i]
            conv, rate = self.convert_amount(row['proc_total_amount'], row['proc_currency'], crm_cur)
            if conv is None:
                continue

            email = row['proc_emails']
            email_sim = self.enhanced_email_similarity(crm_email, email)

            # Tier 1: Strong email match
            email_match = email_sim >= proc_config.email_threshold

            # Tier 2: First + last name match
            proc_first = str(row.get('proc_firstname', '')).lower().strip()
            proc_last = str(row.get('proc_lastname', '')).lower().strip()
            name_match = (
                    crm_first and crm_last and
                    SequenceMatcher(None, crm_first, proc_first).ratio() >= proc_config.name_match_threshold and
                    SequenceMatcher(None, crm_last, proc_last).ratio() >= proc_config.name_match_threshold
            )

            # Tier 3: Partial fallback (name in email)
            name_in_email_match = self.name_in_email(crm_first, email) or self.name_in_email(crm_last, email)

            # Accept if any tier matches
            if not (email_match or name_match or name_in_email_match):
                continue

            tier = 1 if email_match else (2 if name_match else 3)

            candidates.append({
                'index': i,
                'converted_amount': conv,
                'email_score': email_sim,
                'currency': row['proc_currency'],
                'rate': rate,
                'row_data': row,
                'match_tier': tier,
                'name_fallback': tier > 1
            })

        # Sort: prioritize best tier and highest email similarity
        candidates.sort(key=lambda c: (c['match_tier'], -c['email_score']))
        candidates = candidates[:self.config['top_candidates']]

        best_combo = None
        best_score = 0.0
        abs_tol = 0.1
        rel_tol = proc_config.tolerance * crm_amt
        n = len(candidates)

        for k in range(1, min(proc_config.max_combo, n) + 1):
            for combo_idxs in combinations(range(n), k):
                combo = [candidates[i] for i in combo_idxs]
                same_currency = all(c['currency'] == crm_cur for c in combo)
                total = sum(c['converted_amount'] for c in combo)
                tol = abs_tol if same_currency else rel_tol
                diff = abs(total - crm_amt)
                avg_score = sum(c['email_score'] for c in combo) / k

                if diff <= tol:
                    if avg_score > best_score:
                        best_combo = {
                            'combo': combo,
                            'k': k,
                            'total_amount': total,
                            'exact_currency': same_currency
                        }
                        best_score = avg_score
                    if avg_score >= 0.99:
                        break
            if best_score >= 0.99:
                break

        if best_combo:
            c = best_combo['combo']
            received_amount = round(best_combo['total_amount'], 4)
            diff = abs(received_amount - crm_amt)
            comment = ""
            if diff > (abs_tol if best_combo['exact_currency'] else rel_tol):
                comment = f"Amount mismatch of {diff:.2f} {crm_cur}. "
            if not best_combo['exact_currency']:
                comment += "Mixed currencies. "
            if best_combo['k'] > 1:
                comment += "Multiple transactions combined. "

            match_tiers = [cand['match_tier'] for cand in c]
            match_method = ("Email match" if all(t == 1 for t in match_tiers)
                            else "Name match" if any(t == 2 for t in match_tiers)
            else "Name in email")

            comment += f"Matched by: {match_method}"

            return {
                'crm_date': crm_row.get('crm_date'),
                'crm_email': crm_email,
                'crm_firstname': crm_row.get('crm_firstname', ''),
                'crm_lastname': crm_row.get('crm_lastname', ''),
                'crm_last4': crm_row.get('crm_last4'),
                'crm_currency': crm_cur,
                'crm_amount': crm_amt,
                'crm_processor_name': crm_row.get('crm_processor_name'),
                'proc_dates': [r['row_data']['proc_date'] for r in c],
                'proc_emails': [r['row_data']['proc_emails'] for r in c],
                'proc_firstnames': [r['row_data'].get('proc_firstname', '') for r in c],
                'proc_lastnames': [r['row_data'].get('proc_lastname', '') for r in c],
                'proc_last4_digits': [r['row_data']['proc_last4_digits'] for r in c],
                'proc_currencies': [r['currency'] for r in c],
                'proc_total_amounts': [r['row_data']['proc_total_amount'] for r in c],
                'proc_processor_name': next(iter({r['row_data']['processor_name'] for r in c}), None),
                'converted_amount_total': received_amount,
                'exchange_rates': [r['rate'] for r in c],
                'email_similarity_avg': round(best_score, 4),
                'last4_match': False,
                'name_fallback_used': any(r['name_fallback'] for r in c),
                'exact_match_used': False,
                'converted': not best_combo['exact_currency'],
                'proc_combo_len': len([r['index'] for r in c]),  # number of proc rows matched
                'crm_combo_len': 1,  # always 1 during matching, update if batching CRM
                'match_status': 1,
                'payment_status': 1 if diff <= (abs_tol if best_combo['exact_currency'] else rel_tol) else 0,
                'comment': comment,
                'matched_proc_indices': [r['index'] for r in c]
            }, {'best_combo': best_combo}

        return None, {'failure_reason': 'No valid PayPal match found'}

    from itertools import combinations

    def _match_skrill_neteller_row(self, crm_row, proc_dict, last4_map, used, proc_config, processor_name):
        """
        Skrill/Neteller matcher:
         0) if there's a perfect 1:1 on TP+email+amount+currency, take it immediately
         1) else run your exhaustive+greedy combination logic
        """
        # --- CRM fields ---
        crm_tp = str(crm_row.get('crm_tp', '')).strip()
        crm_amt = crm_row['crm_amount']
        crm_cur = str(crm_row['crm_currency']).strip().upper()
        crm_email = str(crm_row['crm_email']).strip().lower()
        crm_last4 = str(crm_row.get('crm_last4', '')).strip()

        # --- Debug: show processor-side TP values ---
        all_tps = [p.get('proc_tp', '') for p in proc_dict.values()]
        self.logger.debug(f"[{processor_name}] CRM TP={crm_tp!r} | Processor TPs={all_tps}")

        # --- 0) SHORT-CIRCUIT exact 1:1 match ---
        for idx, row in proc_dict.items():
            if idx in used:
                continue

            proc_tp = str(row.get('proc_tp', '')).strip()
            proc_email = str(row.get('proc_emails', '')).strip().lower()
            proc_currency = str(row.get('proc_currency', '')).strip().upper()
            proc_amount = row.get('proc_total_amount', 0)

            if (
                    proc_tp == crm_tp and
                    proc_email == crm_email and
                    proc_currency == crm_cur and
                    abs(proc_amount - crm_amt) < 0.01
            ):
                self.logger.debug(f"[{processor_name}] Exact match on idx={idx}")
                return {
                    'crm_date': crm_row['crm_date'],
                    'crm_email': crm_email,
                    'crm_firstname': crm_row['crm_firstname'],
                    'crm_lastname': crm_row['crm_lastname'],
                    'crm_last4': crm_last4,
                    'crm_currency': crm_cur,
                    'crm_amount': crm_amt,
                    'crm_processor_name': crm_row['crm_processor_name'],
                    'proc_dates': [row['proc_date']],
                    'proc_emails': [row['proc_emails']],
                    'proc_firstnames': [row.get('proc_firstname', '')],
                    'proc_lastnames': [row.get('proc_lastname', '')],
                    'proc_last4_digits': [row['proc_last4_digits']],
                    'proc_currencies': [row['proc_currency']],
                    'proc_total_amounts': [row['proc_total_amount']],
                    'proc_processor_name': row['proc_processor_name'],
                    'converted_amount_total': crm_amt,
                    'exchange_rates': [1.0],
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

        # --- 1) FALLBACK: gather candidates by TP or email threshold ---
        candidates = []
        for idx, row in proc_dict.items():
            if idx in used:
                continue
            conv, rate = self.convert_amount(row['proc_total_amount'], row['proc_currency'], crm_cur)
            if conv is None:
                continue
            email_sim = self.enhanced_email_similarity(crm_email, row['proc_emails'])
            last4_valid = crm_last4 not in ("0", "0000", "", "nan")
            last4_match = last4_valid and crm_last4 == str(row['proc_last4_digits'])
            exact_amt = abs(conv - crm_amt) < 0.01
            full_exact = last4_match and exact_amt

            cand = {
                'index': idx,
                'converted_amount': conv,
                'email_score': email_sim,
                'currency': row['proc_currency'],
                'rate': rate,
                'row_data': row,
                'primary': str(row.get('proc_tp', '')).strip() == crm_tp,
                'exact_match': full_exact,
                'last4_match': last4_match
            }
            if cand['primary'] or email_sim >= proc_config.email_threshold:
                candidates.append(cand)

        if not candidates:
            return None, {'failure_reason': f'No {processor_name} candidate found'}

        # --- sort & exhaustive combo search ---
        candidates.sort(key=lambda c: (
            -int(c['primary']),
            -int(c['exact_match']),
            -c['email_score'],
            -int(c['last4_match'])
        ))
        abs_tol = 0.1
        rel_tol = proc_config.tolerance * crm_amt
        best_strict, best_fallback = None, None

        for k in range(1, min(proc_config.max_combo, len(candidates)) + 1):
            for combo in combinations(candidates, k):
                total = sum(c['converted_amount'] for c in combo)
                same_cur = all(c['currency'] == crm_cur for c in combo)
                tol = abs_tol if same_cur else rel_tol
                err = abs(total - crm_amt)
                score = sum(c['email_score'] for c in combo) / k

                if err <= tol:
                    if (not best_strict) or score > best_strict['score'] or err < best_strict['error']:
                        best_strict = {
                            'combo': combo, 'k': k, 'total': total,
                            'error': err, 'score': score, 'exact': same_cur
                        }
                else:
                    if (not best_fallback) or err < best_fallback['error']:
                        best_fallback = {
                            'combo': combo, 'k': k, 'total': total,
                            'error': err, 'score': score, 'exact': same_cur
                        }
            if best_strict and best_strict['score'] >= 0.99:
                break

        chosen = best_strict or best_fallback
        if not chosen:
            return None, {'failure_reason': 'No valid combination found'}
        strict = (chosen is best_strict)

        # --- greedy append to reduce error ---
        combo = list(chosen['combo'])
        total = chosen['total']
        err = abs(total - crm_amt)
        while True:
            best_imp, best_new = err, None
            for c in candidates:
                if c in combo:
                    continue
                new_err = abs((total + c['converted_amount']) - crm_amt)
                if new_err < best_imp:
                    best_imp, best_new = new_err, c
            if best_new:
                combo.append(best_new)
                total += best_new['converted_amount']
                err = best_imp
            else:
                break

        # --- build final result ---
        final_k = len(combo)
        final_same = all(c['currency'] == crm_cur for c in combo)
        final_tol = abs_tol if final_same else rel_tol
        final_err = abs(total - crm_amt)
        payment_status = int(final_err <= final_tol)
        comment = "" if payment_status else (
            f"Client received {'less' if total < crm_amt else 'more'} {abs(total - crm_amt):.2f} {crm_cur}"
        )

        return {
            'crm_date': crm_row['crm_date'],
            'crm_email': crm_email,
            'crm_firstname': crm_row['crm_firstname'],
            'crm_lastname': crm_row['crm_lastname'],
            'crm_last4': crm_last4,
            'crm_currency': crm_cur,
            'crm_amount': crm_amt,
            'crm_processor_name': crm_row['crm_processor_name'],
            'proc_dates': [c['row_data']['proc_date'] for c in combo],
            'proc_emails': [c['row_data']['proc_emails'] for c in combo],
            'proc_last4_digits': [c['row_data']['proc_last4_digits'] for c in combo],
            'proc_currencies': [c['currency'] for c in combo],
            'proc_total_amounts': [c['row_data']['proc_total_amount'] for c in combo],
            'proc_processor_name': processor_name,
            'converted_amount_total': round(total, 4),
            'exchange_rates': [c['rate'] for c in combo],
            'email_similarity_avg': round(sum(c['email_score'] for c in combo) / final_k, 4),
            'last4_match': any(c['last4_match'] for c in combo),
            'name_fallback_used': False,
            'exact_match_used': any(c['exact_match'] for c in combo),
            'converted': not final_same,
            'proc_combo_len': final_k,
            'crm_combo_len': 1,
            'match_status': 1,
            'payment_status': payment_status,
            'comment': comment,
            'matched_proc_indices': [c['index'] for c in combo]
        }, {'strict': strict, 'chosen_error': chosen['error']}

    def _match_trustpayments_row(self, crm_row, proc_dict, last4_map, used, proc_config):
        crm_tp = str(crm_row.get("crm_tp", "")).strip()
        crm_last4 = str(crm_row.get("crm_last4", "")).strip()
        crm_email = str(crm_row.get("crm_email", "")).strip().lower()
        crm_first = str(crm_row.get("crm_firstname", "")).strip().lower()
        crm_last = str(crm_row.get("crm_lastname", "")).strip().lower()
        crm_amt = float(crm_row.get("crm_amount", 0))
        crm_cur = str(crm_row.get("crm_currency", "")).strip().upper()
        abs_tol = 0.1
        rel_tol = proc_config.tolerance * crm_amt

        # Collect processor rows by tiers
        tier_candidates = {1: [], 2: [], 3: [], 4: []}
        for i, proc_row in proc_dict.items():
            if i in used:
                continue
            proc_tp = str(proc_row.get("proc_tp", "")).strip()
            proc_last4 = str(proc_row.get("proc_last4_digits", "")).strip()
            proc_email = str(proc_row.get("proc_emails", "")).strip().lower()
            proc_first = str(proc_row.get("proc_firstname", "")).strip().lower()
            proc_last = str(proc_row.get("proc_lastname", "")).strip().lower()
            proc_amt = float(proc_row.get("proc_total_amount", 0))
            proc_cur = str(proc_row.get("proc_currency", "")).strip().upper()

            conv_amt, rate = self.convert_amount(proc_amt, proc_cur, crm_cur)
            if conv_amt is None:
                continue

            email_sim = self.enhanced_email_similarity(crm_email, proc_email)
            full_name_match = (crm_first == proc_first and crm_last == proc_last)

            if crm_tp and proc_tp and crm_tp == proc_tp and crm_last4 and proc_last4 and crm_last4 == proc_last4:
                tier_candidates[1].append((i, proc_row, conv_amt, rate, email_sim))
            elif crm_tp and proc_tp and crm_tp == proc_tp and email_sim > 0.75:
                tier_candidates[2].append((i, proc_row, conv_amt, rate, email_sim))
            elif crm_last4 and proc_last4 and crm_last4 == proc_last4 and email_sim > 0.75:
                tier_candidates[3].append((i, proc_row, conv_amt, rate, email_sim))
            elif email_sim > 0.75 and full_name_match:
                tier_candidates[4].append((i, proc_row, conv_amt, rate, email_sim))

        # Try each tier in order of priority
        for tier in range(1, 5):
            candidates = tier_candidates[tier]
            if not candidates:
                continue
            n = len(candidates)
            best_combo = None
            best_err = float('inf')
            best_combo_idxs = []

            # Try all combos up to max_combo
            for k in range(1, min(proc_config.max_combo, n) + 1):
                for combo in combinations(candidates, k):
                    indices, proc_rows, amounts, rates, sims = zip(*combo)
                    total_amt = sum(amounts)
                    err = abs(total_amt - crm_amt)
                    tol = abs_tol if crm_cur == proc_rows[0].get("proc_currency", "").strip().upper() else rel_tol
                    if err <= tol and err < best_err:
                        best_combo = (indices, proc_rows, amounts, rates, sims)
                        best_err = err
                if best_combo:
                    break  # Prefer smaller combos if they work

            if best_combo:
                indices, proc_rows, amounts, rates, sims = best_combo
                received = round(sum(amounts), 4)
                payment_status = int(abs(received - crm_amt) <= tol)
                comment = ""
                if payment_status == 0:
                    comment = f"Amount {'under' if received < crm_amt else 'over'} by {abs(received - crm_amt):.2f} {crm_cur}"
                return {
                    'crm_date': crm_row.get('crm_date'),
                    'crm_email': crm_email,
                    'crm_firstname': crm_first,
                    'crm_lastname': crm_last,
                    'crm_last4': crm_last4,
                    'crm_currency': crm_cur,
                    'crm_amount': crm_amt,
                    'crm_processor_name': "trustpayments",
                    'proc_dates': [r.get('proc_date') for r in proc_rows],
                    'proc_emails': [r.get('proc_emails') for r in proc_rows],
                    'proc_firstnames': [r.get('proc_firstname') for r in proc_rows],
                    'proc_lastnames': [r.get('proc_lastname') for r in proc_rows],
                    'proc_last4_digits': [r.get('proc_last4_digits') for r in proc_rows],
                    'proc_currencies': [r.get('proc_currency') for r in proc_rows],
                    'proc_total_amounts': [r.get('proc_total_amount') for r in proc_rows],
                    'proc_processor_name': "trustpayments",
                    'converted_amount_total': received,
                    'exchange_rates': list(rates),
                    'email_similarity_avg': round(sum(sims) / len(sims), 4),
                    'last4_match': tier in [1, 3],
                    'name_fallback_used': (tier == 4),
                    'exact_match_used': (tier == 1),
                    'converted': False,
                    'proc_combo_len': len(proc_rows),
                    'crm_combo_len': 1,
                    'match_status': 1,
                    'payment_status': payment_status,
                    'comment': f"Matched by trustpayments tier {tier}. " + comment,
                    'matched_proc_indices': list(indices)
                }, {'trustpayments_combo': best_combo}

        # If nothing matched, fallback to unmatched
        return None, {'failure_reason': 'No TrustPayments combo match'}

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

    # ... [Other helper methods remain the same] ...