import numpy as np
from difflib import SequenceMatcher
from itertools import combinations
from datetime import datetime, timedelta
from functools import lru_cache
import pandas as pd
import logging, threading, time
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
from pprint import pprint

def anonymize_email(email):
    return hashlib.sha256(email.encode()).hexdigest()[:8] if email else None
class ReconciliationEngine:
    def __init__(self, exchange_rate_map, config=None):
        self.exchange_rate_map = exchange_rate_map
        self.config = {
            'max_combo': 20, 'tolerance': 0.02, 'email_threshold': 0.4,
            'top_candidates': 50, 'fallback_email_threshold': 0.5,
            'enable_fallback': True, 'enable_diagnostics': True,
            'log_level': logging.INFO, 'deep_search': True,
            'require_email_match': False,
            'timeout': 300, 'auto_adjust': True,'minimum_email_similarity': 0.75,
        }
        if config: self.config.update(config)

        self.logger = logging.getLogger('ReconciliationEngine')
        self.logger.setLevel(self.config['log_level'])
        if not self.logger.handlers:
            ch = logging.StreamHandler()
            ch.setLevel(self.config['log_level'])
            ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
            self.logger.addHandler(ch)

        self.metrics = {k: 0 for k in ['total_crm', 'matched_main', 'matched_fallback', 'unmatched']}
        self.metrics.update({'processing_time': 0, 'combo_distribution': {}, 'currency_matches': {}, 'row_times': []})
        self.diagnostics, self.lock = [], threading.Lock()
        self.start_time, self.estimated_time, self.parameter_adjusted = None, None, False

    @lru_cache(maxsize=None)
    def enhanced_email_similarity(self, e1, e2):
        if not e1 or not e2: return 0.0
        l1, d1 = str(e1).lower().split('@') if '@' in e1 else ('','')
        l2, d2 = str(e2).lower().split('@') if '@' in e2 else ('','')
        return 0.85 * SequenceMatcher(None, l1, l2).ratio() + 0.15 * (1.0 if d1 == d2 and d1 else 0.0)

    def convert_amount(self, amount, from_cur, to_cur):
        if from_cur == to_cur: return amount, 1.0
        rate = self.exchange_rate_map.get((from_cur, to_cur))
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
                if self._check_timeout(): break
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
                        self.metrics['combo_distribution'][match['combo_len']] = self.metrics['combo_distribution'].get(match['combo_len'], 0) + 1
                        self.metrics['currency_matches'][match['crm_currency']] = self.metrics['currency_matches'].get(match['crm_currency'], 0) + 1
                    else:
                        matches.append(self._create_unmatched_crm_record(crm_df.loc[idx]))
                        self.metrics['unmatched'] += 1
                        if self.config['enable_diagnostics']:
                            self.diagnostics.append({
                                'crm_idx': idx,
                                'failure_reason': diag.get('failure_reason', 'No candidates'),
                                'crm_email_hash': anonymize_email(crm_df.loc[idx].get('crm_email')),
                                'email_threshold': self.config['email_threshold']
                            })
                if idx % 10 == 0: self._update_eta(len(crm_df), idx + 1)

        for idx, row in processor_df.iterrows():
            if idx not in used_proc:
                matches.append({k: None for k in ['crm_date', 'crm_email', 'crm_firstname', 'crm_lastname', 'crm_last4', 'crm_currency', 'crm_amount', 'crm_processor_name']})
                matches[-1].update({
                    'proc_dates': [row['proc_date']], 'proc_emails': [row['proc_emails']],
                    'proc_last4_digits': [row['proc_last4_digits']], 'proc_currencies': [row['proc_currency']],
                    'proc_total_amounts': [row['proc_total_amount']], 'converted_amount_total': None,
                    'exchange_rates': None, 'email_similarity_avg': None, 'last4_match': None,
                    'converted': False, 'combo_len': 1, 'label': 0
                })

        self.metrics['processing_time'] = (datetime.now() - self.start_time).total_seconds()
        self.logger.info(f"Total processing time: {timedelta(seconds=self.metrics['processing_time'])}")
        return matches

    def _estimate_runtime(self, crm_df, proc_dict, last4_map):
        samples = list(crm_df.iterrows())[:min(5, len(crm_df))]
        if not samples:
            return

        self.logger.info(f"Estimating runtime with {len(samples)} sample rows...")
        times = []
        for _, row in samples:
            t0 = time.time()
            self._match_crm_row(row, proc_dict, last4_map, set())
            times.append(time.time() - t0)

        self.estimated_time = sum(times) / len(times) * len(crm_df)
        self.logger.info(f"Estimated total runtime: {timedelta(seconds=self.estimated_time)}")

        if self.config['auto_adjust'] and self.config['timeout'] and self.estimated_time > self.config['timeout'] * 0.8:
            self._adjust_parameters()

    def _adjust_parameters(self):
        self.logger.warning("High runtime estimated! Adjusting parameters...")
        self.config.update({
            'max_combo': min(4, self.config['max_combo']),
            'top_candidates': min(15, self.config['top_candidates'])
        })
        if self.estimated_time > self.config['timeout'] * 2:
            self.config.update({
                'max_combo': min(2, self.config['max_combo']),
                'top_candidates': min(10, self.config['top_candidates']),
                'email_threshold': max(0.8, self.config['email_threshold'])
            })
        self.parameter_adjusted = True

    def _check_timeout(self):
        return self.config['timeout'] and (datetime.now() - self.start_time).total_seconds() > self.config['timeout']

    def _update_eta(self, total, done):
        if not self.metrics['row_times']: return
        avg = sum(self.metrics['row_times']) / len(self.metrics['row_times'])
        eta = avg * (total - done)
        self.logger.info(f"Processed {done}/{total} rows. ETA: {timedelta(seconds=eta)}")

    def _match_crm_row(self, crm_row, proc_dict, last4_map, used):
        crm_last4, crm_cur, crm_amt, crm_email = crm_row['crm_last4'], crm_row['crm_currency'], crm_row['crm_amount'], crm_row['crm_email']
        candidates, diag_info = [], {}
        indices = [i for i in proc_dict if i not in used] if crm_last4 in ("0", "0000", "", None) else last4_map.get(crm_last4, [])

        for i in indices:
            if i in used: continue
            row = proc_dict[i]
            conv, rate = self.convert_amount(row['proc_total_amount'], row['proc_currency'], crm_cur)
            if conv is None: continue
            sim = self.enhanced_email_similarity(crm_email, row['proc_emails'])
            min_sim = self.config.get('minimum_email_similarity', 0.75)
            if sim < min_sim and abs(conv - crm_amt) > self.config['tolerance'] * crm_amt:
                if self.config['enable_diagnostics']:
                    diag_info.setdefault('all_candidates', []).append({
                        'email_score': round(sim, 3),
                        'converted_amount': round(conv, 2),
                        'currency': row['proc_currency'],
                        'proc_last4': row['proc_last4_digits'],
                        'amount_diff': round(abs(conv - crm_amt), 4),
                    })
                continue
            sim = self.enhanced_email_similarity(crm_email, row['proc_emails'])
            conv, rate = self.convert_amount(row['proc_total_amount'], row['proc_currency'], crm_cur)
            if conv is None: continue

            # ⬇️ NEW: Always log diagnostics for all attempted candidates
            diag_info.setdefault('all_candidates', []).append({
                'email_score': round(sim, 3),
                'converted_amount': round(conv, 2),
                'currency': row['proc_currency'],
                'proc_last4': row['proc_last4_digits'],
                'amount_diff': round(abs(conv - crm_amt), 4),
                'reason': 'Filtered out by email/amount threshold' if sim < 0.75 and abs(conv - crm_amt) > self.config[
                    'tolerance'] * crm_amt else 'Included'
            })

            candidates.append({
                'index': i, 'converted_amount': conv, 'email_score': sim, 'currency': row['proc_currency'],
                'rate': rate, 'row_data': row
            })

        candidates.sort(key=lambda x: x['email_score'], reverse=True)
        candidates = candidates[:self.config['top_candidates']]

        diag_info['crm_amount'] = crm_amt
        diag_info['converted_amounts'] = [round(c['converted_amount'], 2) for c in candidates[:5]]

        best_combo, best_score = None, 0.0
        abs_tol, rel_tol, n = 0.1, self.config['tolerance'] * crm_amt, len(candidates)

        for k in range(1, min(self.config['max_combo'], n) + 1):
            for combo_idxs in combinations(range(n), k):
                combo = [candidates[i] for i in combo_idxs]
                same = all(c['currency'] == crm_cur for c in combo)
                total = sum(c['converted_amount'] for c in combo)
                tol = abs_tol if same else rel_tol
                if abs(total - crm_amt) > tol: continue
                avg_score = sum(c['email_score'] for c in combo) / k
                if avg_score > best_score:
                    best_score = avg_score
                    best_combo = {'combo': combo, 'k': k, 'total_amount': total, 'exact_currency': same}
                    if avg_score >= 0.99: break
            if best_score >= 0.99: break

        if best_combo:
            c = best_combo['combo']
            return {
                'crm_date': crm_row.get('crm_date'), 'crm_email': crm_email,
                'crm_firstname': crm_row.get('crm_firstname'), 'crm_lastname': crm_row.get('crm_lastname'),
                'crm_last4': crm_last4, 'crm_currency': crm_cur, 'crm_amount': crm_amt,
                'crm_processor_name': crm_row.get('crm_processor_name'),
                'proc_dates': [r['row_data']['proc_date'] for r in c],
                'proc_emails': [r['row_data']['proc_emails'] for r in c],
                'proc_last4_digits': [r['row_data']['proc_last4_digits'] for r in c],
                'proc_currencies': [r['currency'] for r in c],
                'proc_total_amounts': [r['row_data']['proc_total_amount'] for r in c],
                'proc_processor_name': next(iter({r['row_data']['processor_name'] for r in c}), None),
                'converted_amount_total': round(best_combo['total_amount'], 4),
                'exchange_rates': [r['rate'] for r in c],
                'email_similarity_avg': round(best_score, 4),
                'last4_match': True, 'converted': not best_combo['exact_currency'],
                'combo_len': best_combo['k'], 'label': 1,
                'matched_proc_indices': [r['index'] for r in c]
            }, {'best_combo': best_combo}
        # Add tolerance signal for near matches (even if no combo was selected)
        diag_info['amount_within_tolerance'] = any(
            abs(c['converted_amount'] - crm_amt) <= self.config['tolerance'] * crm_amt
            for c in candidates
        )

        if not best_combo:
            diag_info.update({
                'top_email_scores': [
                    {'email_hash': anonymize_email(c['row_data']['proc_emails']), 'score': round(c['email_score'], 3)}
                    for c in candidates[:5]
                ],
                'candidate_count': len(candidates),
                'crm_amount': crm_amt,
                'converted_amounts': [round(c['converted_amount'], 2) for c in candidates[:5]],
                'crm_email_hash': anonymize_email(crm_email),
                'email_threshold': self.config['email_threshold'],
                'failure_reason': 'No valid combination found',
                'proc_currency_candidates': [c['currency'] for c in candidates[:5]],
                'last4_match_candidates': [c['row_data']['proc_last4_digits'] == crm_last4 for c in candidates[:5]]
            })
            # ✅ Now includes all diagnostic candidates
            return None, diag_info
    def _create_unmatched_crm_record(self, row):
        return {k: row.get(k) for k in ['crm_date', 'crm_email', 'crm_firstname', 'crm_lastname', 'crm_last4', 'crm_currency', 'crm_amount', 'crm_processor_name']} | {
            'proc_dates': [], 'proc_emails': [], 'proc_last4_digits': [],
            'proc_currencies': [], 'proc_total_amounts': [], 'converted_amount_total': None,
            'exchange_rates': [], 'email_similarity_avg': None, 'last4_match': False,
            'converted': False, 'combo_len': 0, 'label': 0
        }
diagnostics_path = r'C:\Users\yanin\Projects\reconciliation_project\data\training_dataset\diagnostics_2025-05-07.json'

with open(diagnostics_path, 'r') as f:
    diagnostics = json.load(f)

for entry in diagnostics:
    print(f"\n--- Unmatched CRM idx {entry['crm_idx']} ---")
    pprint({k: v for k, v in entry.items() if k != 'crm_email_hash'})

    if 'all_candidates' in entry:
        print("Candidate Diagnostics:")
        for rc in entry['all_candidates']:
            print(f"  → Email Sim: {rc['email_score']}, Amount Diff: {rc['amount_diff']}, Last4: {rc['proc_last4']}, Currency: {rc['currency']}, Reason: {rc['reason']}")
    else:
        print("No candidate data recorded.\n")