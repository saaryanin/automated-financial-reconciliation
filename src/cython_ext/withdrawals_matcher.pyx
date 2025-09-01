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
        require_email=True
    ),
    'paypal': ProcessorConfig(
        email_threshold=0.8,
        name_match_threshold=0.75,
        require_last4=False,
        require_email=False,
        enable_name_fallback=True,
        matching_logic="paypal"
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
        rate = self.exchange_rate_map.get((from_cur, to_cur))

        if rate is None:
            self.logger.warning(f"Missing exchange rate: {from_cur} -> {to_cur}")
        return (amount * rate, rate) if rate else (None, None)

    def generate_report(self):
        return {
            'metrics': self.metrics,
            'diagnostics': self.diagnostics if self.config['enable_diagnostics'] else None,
            'estimated_time': self.estimated_time,
            'parameters_adjusted': self.parameter_adjusted
        }

    def match_withdrawals(self, crm_df, processor_df):
        self.start_time = datetime.now()
        self.metrics['total_crm'] = len(crm_df)
        used_proc, used_crm, matches = set(), set(), []
        last4_map = processor_df.groupby('proc_last4_digits').indices
        proc_dict = processor_df.to_dict('index')

        self._estimate_runtime(crm_df, proc_dict, last4_map)

        with ThreadPoolExecutor() as executor:
            futures = {
                executor.submit(self._match_crm_row, row, proc_dict, last4_map, used_proc): idx
                for idx, row in crm_df.iterrows() if idx not in used_crm
            }
            for future in as_completed(futures):
                idx = futures[future]
                if self._check_timeout():
                    break
                try:
                    match, diag = future.result()
                except Exception as e:
                    self.logger.error(f"Error processing row {idx}: {e}")
                    match, diag = None, {'failure_reason': str(e)}

                with self.lock:
                    if match:
                        matches.append(match)
                        used_crm.add(idx)
                        used_proc.update(match.get('matched_proc_indices', []))
                        self.metrics['matched_main'] += 1
                        self.metrics['combo_distribution'][match['combo_len']] = self.metrics['combo_distribution'].get(
                            match['combo_len'], 0) + 1
                        self.metrics['currency_matches'][match['crm_currency']] = self.metrics['currency_matches'].get(
                            match['crm_currency'], 0) + 1

                        # Track payment correctness
                        if match['payment_status'] == 1:
                            self.metrics['correct_payments'] += 1
                        else:
                            self.metrics['incorrect_payments'] += 1
                    else:
                        unmatched_record = self._create_unmatched_crm_record(crm_df.loc[idx])
                        matches.append(unmatched_record)
                        self.metrics['unmatched'] += 1
                        if self.config['enable_diagnostics']:
                            self.diagnostics.append(
                                {'crm_idx': idx, 'failure_reason': diag.get('failure_reason', 'No candidates')})
                if idx % 10 == 0:
                    self._update_eta(len(crm_df), idx + 1)

        # Add unmatched processor rows
        for idx, row in processor_df.iterrows():
            if idx not in used_proc:
                record = {
                    'crm_date': None,
                    'crm_email': None,
                    'crm_firstname': None,
                    'crm_lastname': None,
                    'crm_last4': None,
                    'crm_currency': None,
                    'crm_amount': None,
                    'crm_processor_name': None,
                    'proc_dates': [row['proc_date']],
                    'proc_emails': [row['proc_emails']],
                    'proc_last4_digits': [row['proc_last4_digits']],
                    'proc_currencies': [row['proc_currency']],
                    'proc_total_amounts': [row['proc_total_amount']],
                    'converted_amount_total': None,
                    'exchange_rates': None,
                    'email_similarity_avg': None,
                    'last4_match': None,
                    'converted': False,
                    'combo_len': 1,
                    'match_status': 0,
                    'payment_status': 0,
                    'comment': "No matching CRM row found",
                    'matched_proc_indices': [idx]
                }
                matches.append(record)

        self.metrics['processing_time'] = (datetime.now() - self.start_time).total_seconds()
        self.logger.info(f"Total processing time: {timedelta(seconds=self.metrics['processing_time'])}")
        return matches

    # ... [Estimate runtime, adjust parameters, etc. methods remain the same] ...

    def _match_crm_row(self, crm_row, proc_dict, last4_map, used):
        # Get processor-specific configuration
        proc_name = crm_row.get('crm_processor_name', '').lower()
        proc_config = self.get_processor_config(proc_name)

        # Select matching logic based on processor
        if proc_config.matching_logic == "paypal":
            return self._match_paypal_row(crm_row, proc_dict, last4_map, used, proc_config)
        else:
            return self._match_standard_row(crm_row, proc_dict, last4_map, used, proc_config)

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
                'combo_len': best_combo['k'],
                'match_status': 1,
                'payment_status': payment_status,
                'comment': comment,
                'matched_proc_indices': [r['index'] for r in c]
            }, {'best_combo': best_combo}

        return None, {'failure_reason': 'No valid combination found'}

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
            'proc_dates': [], 'proc_emails': [], 'proc_last4_digits': [],
            'proc_currencies': [], 'proc_total_amounts': [],
            'proc_processor_name': None, 'converted_amount_total': None,
            'exchange_rates': [], 'email_similarity_avg': None,
            'last4_match': False, 'name_fallback_used': False,
            'exact_match_used': False, 'converted': False,
            'combo_len': 0, 'match_status': 0, 'payment_status': 0,
            'comment': "No matching processor row found",
            'matched_proc_indices': []
        }

    def _match_paypal_row(self, crm_row, proc_dict, last4_map, used, proc_config):
        crm_last4 = str(crm_row['crm_last4']) if not pd.isna(crm_row['crm_last4']) else ''
        crm_cur = crm_row['crm_currency']
        crm_amt = crm_row['crm_amount']
        crm_email = crm_row['crm_email']
        crm_first = str(crm_row.get('crm_firstname', ''))
        crm_last = str(crm_row.get('crm_lastname', ''))

        candidates = []
        indices = [i for i in proc_dict if i not in used]

        # PayPal doesn't use last4 digits
        # We'll skip last4 filtering since PayPal doesn't provide this information

        for i in indices:
            if i in used:
                continue
            row = proc_dict[i]
            conv, rate = self.convert_amount(row['proc_total_amount'], row['proc_currency'], crm_cur)
            if conv is None:
                continue

            # Calculate email similarity
            email_sim = self.enhanced_email_similarity(crm_email, row['proc_emails'])

            # PayPal-specific matching logic
            name_fallback = False
            name_in_email = False

            # Tier 1: Email match
            email_match = email_sim >= proc_config.email_threshold

            # Tier 2: Name match (if email doesn't match)
            name_match = False
            if not email_match and crm_first and crm_last:
                # Get processor names - for PayPal, we assume firstname/lastname are in the row
                proc_first = str(row.get('proc_firstname', '')).lower()
                proc_last = str(row.get('proc_lastname', '')).lower()

                # Compare names
                crm_first_lower = crm_first.lower()
                crm_last_lower = crm_last.lower()
                name_match = (
                        SequenceMatcher(None, crm_first_lower,
                                        proc_first).ratio() >= proc_config.name_match_threshold and
                        SequenceMatcher(None, crm_last_lower, proc_last).ratio() >= proc_config.name_match_threshold
                )

            # Tier 3: Name in email (if previous tiers didn't match)
            if not email_match and not name_match:
                if crm_first:
                    name_in_email = self.name_in_email(crm_first, row['proc_emails'])
                if not name_in_email and crm_last:
                    name_in_email = self.name_in_email(crm_last, row['proc_emails'])

            # Only consider candidates that pass at least one matching tier
            if not (email_match or name_match or name_in_email):
                continue

            # Valid candidate
            candidates.append({
                'index': i,
                'converted_amount': conv,
                'email_score': email_sim,
                'currency': row['proc_currency'],
                'rate': rate,
                'row_data': row,
                'last4_match': False,  # PayPal doesn't have last4
                'name_fallback': name_match or name_in_email,
                'exact_match': False,
                'match_tier': 1 if email_match else (2 if name_match else 3)
            })

        # Prioritize by match tier (email first, then name, then name-in-email)
        candidates.sort(key=lambda x: (x['match_tier'], -x['email_score']))
        candidates = candidates[:self.config['top_candidates']]

        best_combo = None
        best_score = 0.0
        abs_tol = 0.1
        rel_tol = proc_config.tolerance * crm_amt
        n = len(candidates)

        for k in range(1, min(proc_config.max_combo, n) + 1):
            for combo_idxs in combinations(range(n), k):
                combo = [candidates[i] for i in combo_idxs]
                same = all(c['currency'] == crm_cur for c in combo)
                total = sum(c['converted_amount'] for c in combo)
                tol = abs_tol if same else rel_tol
                avg_score = sum(c['email_score'] for c in combo) / k
                score_ok = avg_score >= proc_config.email_threshold

                if not score_ok:
                    continue  # still skip bad scores

                diff = abs(total - crm_amt)
                if best_combo is None or avg_score > best_score:
                    best_combo = {'combo': combo, 'k': k, 'total_amount': total, 'exact_currency': same}
                    best_score = avg_score

                avg_score = sum(c['email_score'] for c in combo) / k
                if avg_score > best_score:
                    best_score = avg_score
                    best_combo = {'combo': combo, 'k': k, 'total_amount': total, 'exact_currency': same}
                    if avg_score >= 0.99:
                        break
            if best_score >= 0.99:
                break

        if best_combo:
            c = best_combo['combo']
            received_amount = round(best_combo['total_amount'], 4)
            abs_tol = 0.1
            rel_tol = proc_config.tolerance * crm_amt
            tol = abs_tol if best_combo['exact_currency'] else rel_tol
            diff = received_amount - crm_amt
            abs_diff = abs(diff)

            # Determine payment correctness
            payment_status = 1 if abs_diff <= tol else 0

            # Create comment explaining payment status
            comment = ""
            if payment_status == 0:
                comment = (f"Amount mismatch: Expected {crm_amt:.2f} {crm_cur}, "
                           f"received {received_amount:.2f} {crm_cur} "
                           f"(difference: {abs_diff:.2f} {crm_cur})")
            elif not best_combo['exact_currency']:
                comment = "Amount converted from multiple currencies"
            if best_combo['k'] > 1:
                comment = "Split payment across multiple transactions"

            # Add PayPal-specific matching details
            match_tiers = [cand['match_tier'] for cand in c]
            if all(tier == 1 for tier in match_tiers):
                match_method = "Email match"
            elif any(tier == 2 for tier in match_tiers):
                match_method = "Name match"
            else:
                match_method = "Name in email match"

            comment += f" | Matched by: {match_method}"

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
                'proc_last4_digits': [r['row_data']['proc_last4_digits'] for r in c],
                'proc_currencies': [r['currency'] for r in c],
                'proc_total_amounts': [r['row_data']['proc_total_amount'] for r in c],
                'proc_processor_name': next(iter({r['row_data']['processor_name'] for r in c}), None),
                'converted_amount_total': received_amount,
                'exchange_rates': [r['rate'] for r in c],
                'email_similarity_avg': round(best_score, 4),
                'last4_match': False,  # PayPal doesn't have last4
                'name_fallback_used': any(r.get('name_fallback', False) for r in c),
                'exact_match_used': any(r.get('exact_match', False) for r in c),
                'converted': not best_combo['exact_currency'],
                'combo_len': best_combo['k'],
                'match_status': 1,
                'payment_status': payment_status,
                'comment': comment,
                'matched_proc_indices': [r['index'] for r in c]
            }, {'best_combo': best_combo}

        # No match found
        return None, {'failure_reason': 'No valid combination found for PayPal transaction'}

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