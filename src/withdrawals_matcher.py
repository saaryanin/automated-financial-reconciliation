import numpy as np
from difflib import SequenceMatcher
from itertools import combinations
import logging
from datetime import datetime, timedelta
from functools import lru_cache
import pandas as pd
import threading
import time
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed


class ReconciliationEngine:
    def __init__(self, exchange_rate_map, config=None):
        self.exchange_rate_map = exchange_rate_map
        self.config = {
            'max_combo': 6,
            'tolerance': 0.02,
            'email_threshold': 0.7,
            'top_candidates': 30,
            'fallback_email_threshold': 0.6,
            'enable_fallback': True,
            'enable_diagnostics': True,
            'log_level': logging.INFO,
            'deep_search': False,
            'timeout': None,  # Overall timeout in seconds; e.g., 300 for 5 minutes.
            'auto_adjust': True
        }
        if config:
            self.config.update(config)

        # Setup logging
        self.logger = logging.getLogger('ReconciliationEngine')
        self.logger.setLevel(self.config['log_level'])
        if not self.logger.handlers:
            ch = logging.StreamHandler()
            ch.setLevel(self.config['log_level'])
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

        # Tracking metrics and diagnostics
        self.metrics = {
            'total_crm': 0,
            'matched_main': 0,
            'matched_fallback': 0,
            'unmatched': 0,
            'processing_time': 0,
            'combo_distribution': {},
            'currency_matches': {},
            'row_times': []
        }
        self.diagnostics = []
        self.lock = threading.Lock()
        self.start_time = None
        self.estimated_time = None
        self.parameter_adjusted = False

    @lru_cache(maxsize=None)
    def email_similarity(self, e1, e2):
        # Compare the part before '@'
        e1 = str(e1).split('@')[0] if e1 else ""
        e2 = str(e2).split('@')[0] if e2 else ""
        return SequenceMatcher(None, e1, e2).ratio()

    def convert_amount(self, amount, from_currency, to_currency):
        if from_currency == to_currency:
            return amount, 1.0
        rate = self.exchange_rate_map.get((from_currency, to_currency))
        if rate:
            return amount * rate, rate
        return None, None

    def generate_report(self):
        """Return metrics and diagnostics report."""
        report = {
            'metrics': self.metrics,
            'diagnostics': self.diagnostics if self.config['enable_diagnostics'] else None,
            'estimated_time': self.estimated_time,
            'parameters_adjusted': self.parameter_adjusted
        }
        return report
    def match_withdrawals(self, crm_df, processor_df):
        self.start_time = datetime.now()
        self.metrics['total_crm'] = len(crm_df)
        used_proc_indices = set()
        used_crm_indices = set()
        matches = []

        # Precompute lookup structures
        last4_index_map = processor_df.groupby('proc_last4_digits').indices
        processor_dict = processor_df.to_dict('index')

        # Estimate overall runtime based on a sample for auto-adjust
        self._estimate_runtime(crm_df, processor_dict, last4_index_map)

        # Process CRM rows concurrently. Each row is processed independently.
        with ThreadPoolExecutor() as executor:
            future_to_crm = {
                executor.submit(self._match_crm_row, crm_row, processor_dict, last4_index_map,
                                used_proc_indices): crm_idx
                for crm_idx, crm_row in crm_df.iterrows()
                if crm_idx not in used_crm_indices
            }

            for future in as_completed(future_to_crm):
                crm_idx = future_to_crm[future]
                # Check timeout at retrieval
                if self._check_timeout():
                    self.logger.warning("Timeout reached! Aborting processing.")
                    break
                try:
                    match, diag_info = future.result()
                except Exception as e:
                    self.logger.error(f"Error processing CRM row {crm_idx}: {e}")
                    match = None
                    diag_info = {'failure_reason': str(e)}

                if match:
                    with self.lock:
                        matches.append(match)
                        used_crm_indices.add(crm_idx)
                        used_proc_indices.update(match['proc_indices'])
                        self.metrics['matched_main'] += 1

                        combo_len = match['combo_len']
                        self.metrics['combo_distribution'][combo_len] = \
                            self.metrics['combo_distribution'].get(combo_len, 0) + 1

                        currency_key = match['crm_currency']
                        self.metrics['currency_matches'][currency_key] = \
                            self.metrics['currency_matches'].get(currency_key, 0) + 1
                else:
                    with self.lock:
                        matches.append(self._create_unmatched_crm_record(crm_df.loc[crm_idx]))
                        self.metrics['unmatched'] += 1
                    if self.config['enable_diagnostics']:
                        self.diagnostics.append({
                            'crm_idx': crm_idx,
                            'failure_reason': diag_info.get('failure_reason', 'No candidates')
                        })

                # Optionally update ETA every few records (or use a separate thread)
                # Here we log every 10 rows
                if crm_idx % 10 == 0:
                    self._update_eta(len(crm_df), crm_idx + 1)

        # Process any remaining unmatched processor rows
        for idx, proc_row in processor_df.iterrows():
            if idx in used_proc_indices:
                continue
            matches.append({
                'crm_date': None,
                'crm_email': None,
                'crm_firstname': None,
                'crm_lastname': None,
                'crm_last4': None,
                'crm_currency': None,
                'crm_amount': None,
                'crm_processor_name': None,
                'proc_dates': [proc_row['proc_date']],
                'proc_emails': [proc_row['proc_emails']],
                'proc_last4_digits': [proc_row['proc_last4_digits']],
                'proc_currencies': [proc_row['proc_currency']],
                'proc_total_amounts': [proc_row['proc_total_amount']],
                'converted_amount_total': None,
                'exchange_rates': None,
                'email_similarity_avg': None,
                'last4_match': None,
                'converted': False,
                'combo_len': 1,
                'label': 0
            })

        self.metrics['processing_time'] = (datetime.now() - self.start_time).total_seconds()
        self.logger.info(f"Total processing time: {timedelta(seconds=self.metrics['processing_time'])}")
        return matches

    def _estimate_runtime(self, crm_df, processor_dict, last4_index_map):
        sample_size = min(5, len(crm_df))
        if sample_size == 0:
            return
        self.logger.info(f"Estimating runtime with {sample_size} sample rows...")
        sample_times = []
        for i in range(sample_size):
            # Use next(iter(...)) to sample different rows
            crm_idx, crm_row = list(crm_df.iterrows())[i]
            t0 = time.time()
            _ = self._match_crm_row(crm_row, processor_dict, last4_index_map, set())
            sample_times.append(time.time() - t0)
        avg_time = sum(sample_times) / sample_size
        self.estimated_time = avg_time * len(crm_df)
        self.logger.info(f"Estimated total runtime: {timedelta(seconds=self.estimated_time)}")

        if self.config['auto_adjust'] and self.config['timeout'] and self.estimated_time > self.config['timeout'] * 0.8:
            self._adjust_parameters()

    def _adjust_parameters(self):
        self.logger.warning("High runtime estimated! Adjusting parameters to reduce processing time...")
        new_params = {
            'max_combo': min(4, self.config['max_combo']),
            'top_candidates': min(15, self.config['top_candidates'])
        }
        # Even more aggressive if needed
        if self.estimated_time > self.config['timeout'] * 2:
            new_params.update({
                'max_combo': min(2, self.config['max_combo']),
                'top_candidates': min(10, self.config['top_candidates']),
                'email_threshold': max(0.8, self.config['email_threshold'])
            })
        self.config.update(new_params)
        self.parameter_adjusted = True
        self.logger.warning(
            f"Adjusted parameters: max_combo={self.config['max_combo']}, top_candidates={self.config['top_candidates']}, email_threshold={self.config['email_threshold']}"
        )

    def _update_eta(self, total_rows, processed_rows):
        if not self.metrics['row_times']:
            return
        avg_time = sum(self.metrics['row_times']) / len(self.metrics['row_times'])
        remaining = total_rows - processed_rows
        eta_seconds = avg_time * remaining
        self.logger.info(f"Processed {processed_rows}/{total_rows} rows. ETA: {timedelta(seconds=eta_seconds)}")

    def _check_timeout(self):
        if not self.config['timeout']:
            return False
        elapsed = (datetime.now() - self.start_time).total_seconds()
        return elapsed > self.config['timeout']

    def _match_crm_row(self, crm_row, processor_dict, last4_index_map, used_proc_indices):
        """
        For a given CRM row, find the best candidate combination of processor rows.
        Returns (match_record, diagnostics_info).
        """
        crm_last4 = crm_row['crm_last4']
        crm_currency = crm_row['crm_currency']
        crm_amount = crm_row['crm_amount']
        crm_email = crm_row['crm_email']

        diag_info = {}

        # Get candidate processor indices for this last4
        candidate_indices = last4_index_map.get(crm_last4, [])
        candidates = []
        for idx in candidate_indices:
            if idx in used_proc_indices:
                continue
            proc_row = processor_dict[idx]

            # Currency conversion
            converted, rate = self.convert_amount(
                proc_row['proc_total_amount'],
                proc_row['proc_currency'],
                crm_currency
            )
            if converted is None:
                continue

            # Email similarity (always compute)
            sim = self.email_similarity(crm_email, proc_row['proc_emails'])

            # If email match is required, enforce threshold
            if self.config.get('require_email_match', True):
                if sim < self.config['email_threshold']:
                    continue
            # If not required, allow all (no early rejection)

            # Add candidate
            candidates.append({
                'index': idx,
                'converted_amount': converted,
                'email_score': sim,
                'currency': proc_row['proc_currency'],
                'rate': rate,
                'row_data': proc_row
            })

        # Sort candidates by descending email score
        candidates.sort(key=lambda x: x['email_score'], reverse=True)
        candidates = candidates[:self.config['top_candidates']]

        best_combo = None
        best_avg_score = 0.0
        n = len(candidates)
        abs_tol = 0.1  # For exact currency
        rel_tol = self.config['tolerance'] * crm_amount  # For converted amounts

        # Use combinations (from 1 to max_combo)
        for k in range(1, min(self.config['max_combo'], n) + 1):
            for combo_indices in combinations(range(n), k):
                combo = [candidates[i] for i in combo_indices]
                exact_currency = all(c['currency'] == crm_currency for c in combo)
                total_amount = sum(c['converted_amount'] for c in combo)
                tol = abs_tol if exact_currency else rel_tol
                if abs(total_amount - crm_amount) > tol:
                    continue
                avg_score = sum(c['email_score'] for c in combo) / k
                if avg_score > best_avg_score:
                    best_avg_score = avg_score
                    best_combo = {
                        'combo': combo,
                        'k': k,
                        'total_amount': total_amount,
                        'exact_currency': exact_currency
                    }
                    # Early exit on extremely high score
                    if avg_score >= 0.99:
                        break
            if best_avg_score >= 0.99:
                break

        if best_combo:
            combo = best_combo['combo']
            match_record = {
                'crm_date': crm_row.get('crm_date'),
                'crm_email': crm_email,
                'crm_firstname': crm_row.get('crm_firstname'),
                'crm_lastname': crm_row.get('crm_lastname'),
                'crm_last4': crm_last4,
                'crm_currency': crm_currency,
                'crm_amount': crm_amount,
                'crm_processor_name': crm_row.get('crm_processor_name'),
                'proc_dates': [c['row_data']['proc_date'] for c in combo],
                'proc_emails': [c['row_data']['proc_emails'] for c in combo],
                'proc_last4_digits': [c['row_data']['proc_last4_digits'] for c in combo],
                'proc_currencies': [c['currency'] for c in combo],
                'proc_total_amounts': [c['row_data']['proc_total_amount'] for c in combo],
                'converted_amount_total': round(best_combo['total_amount'], 4),
                'exchange_rates': [c['rate'] for c in combo],
                'email_similarity_avg': round(best_avg_score, 4),
                'last4_match': True,
                'converted': not best_combo['exact_currency'],
                'combo_len': best_combo['k'],
                'label': 1,
                'proc_indices': [c['index'] for c in combo]
            }
            diag_info['best_combo'] = best_combo
            return match_record, diag_info
        else:
            diag_info['failure_reason'] = 'No valid combination found'
            return None, diag_info

    def _create_unmatched_crm_record(self, crm_row):
        """Return a record for an unmatched CRM row."""
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
            'converted_amount_total': None,
            'exchange_rates': [],
            'email_similarity_avg': None,
            'last4_match': False,
            'converted': False,
            'combo_len': 0,
            'label': 0,
            'proc_indices': []
        }

# --- Example usage ---
# Assuming you have:
#   crm_df: DataFrame with CRM columns including 'crm_date', 'crm_email', 'crm_firstname', 'crm_lastname',
#           'crm_last4', 'crm_currency', 'crm_amount', 'crm_processor_name'
#   processor_df: DataFrame with processor columns including 'proc_date', 'proc_emails', 'proc_last4_digits',
#                 'proc_currency', 'proc_total_amount'
#   And an exchange_rate_map: dict with keys (from_currency, to_currency) -> rate
#
# Example:
#   engine = ReconciliationEngine(exchange_rate_map, config={'timeout': 300, 'log_level': logging.DEBUG})
#   results = engine.match_withdrawals(crm_df, processor_df)
#   for rec in results:
#       print(rec)
