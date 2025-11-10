# withdrawals_matcher_test.py (updated with crm_idx in _match_standard_row and _match_safechargeuk_row)
from datetime import datetime, timedelta
from functools import lru_cache
import pandas as pd
import logging, threading, time
from src.utils import create_cancelled_row,normalize_string,clean_last4,clean_field
from difflib import SequenceMatcher
from collections import defaultdict
import re
from src.config import setup_dirs_for_reg
class ProcessorConfig:
    def __init__(self,
                 email_threshold=0.0,
                 name_match_threshold=0.0,
                 require_last4=True,
                 require_email=True,
                 enable_name_fallback=True,
                 enable_exact_match=True,
                 tolerance=0.0,
                 matching_logic="standard",
                 allow_last4_only_if_email_blank=False):
        self.email_threshold = email_threshold
        self.name_match_threshold = name_match_threshold
        self.require_last4 = require_last4
        self.require_email = require_email
        self.enable_name_fallback = enable_name_fallback
        self.enable_exact_match = enable_exact_match
        self.tolerance = tolerance
        self.matching_logic = matching_logic
        self.allow_last4_only_if_email_blank = allow_last4_only_if_email_blank
# Processor-specific configurations
PROCESSOR_CONFIGS = {
    'safecharge': ProcessorConfig(
        email_threshold=0.6,
        require_last4=True,
        require_email=True,
        tolerance=0.1,
        allow_last4_only_if_email_blank=True
    ),
    'safechargeuk': ProcessorConfig(
        email_threshold=0.6,
        require_last4=True,
        require_email=True,
        tolerance=0.1,
        allow_last4_only_if_email_blank=True
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
        tolerance=0.05,
        matching_logic="paypal"
    ),
    'shift4': ProcessorConfig(
        email_threshold=0.6,
        name_match_threshold=0.70,
        require_last4=True,
        require_email=True,
        enable_name_fallback=True,
        enable_exact_match=True,
        tolerance=0.05,
        matching_logic="shift4"
    ),
    'skrill': ProcessorConfig(
        email_threshold=0.6,
        require_last4=False,
        require_email=True,
        enable_name_fallback=False,
        enable_exact_match=False,
        tolerance=0.05,
        matching_logic="skrill"
    ),
    'neteller': ProcessorConfig(
        email_threshold=0.6,
        require_last4=False,
        require_email=True,
        enable_name_fallback=False,
        enable_exact_match=False,
        tolerance=0.05,
        matching_logic="skrill" # we'll just reuse the Skrill logic
    ),
    'bitpay': ProcessorConfig(
        email_threshold=0.6,
        require_last4=False,
        require_email=True,
        enable_name_fallback=True,
        enable_exact_match=False,
        tolerance=0.5,
        matching_logic="bitpay"
    ),
    'zotapay_paymentasia': ProcessorConfig( # Add this new configuration
        email_threshold=1,
        require_last4=False,
        require_email=False,
        enable_name_fallback=False,
        enable_exact_match=True,
        tolerance=0.1,
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
    'barclays': ProcessorConfig(
        email_threshold=0.0,
        require_last4=True,
        require_email=False,
        enable_name_fallback=False,
        tolerance=0.1,
        matching_logic="barclays"
    ),
    'safechargeuk': ProcessorConfig(
        email_threshold=0.6,
        require_last4=True,
        require_email=True,
        tolerance=0.1,
        allow_last4_only_if_email_blank=True,
        enable_name_fallback=False,
        matching_logic="safechargeuk"
    ),
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
            'auto_adjust': True,
            'enable_warning_flag': False,
            'force_skip_proc_check': False,
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
        # Handle multiple emails in e1 (CRM side) by splitting on comma and taking max similarity
        if ',' in e1:
            e1_list = [email.strip() for email in e1.split(',')]
        else:
            e1_list = [e1]
        l2 = e2.lower().split('@')[0] if '@' in e2 else e2.lower()
        max_sim = 0.0
        for e1_single in e1_list:
            l1 = e1_single.lower().split('@')[0] if '@' in e1_single else e1_single.lower()
            sim = SequenceMatcher(None, l1, l2).ratio()
            if sim > max_sim:
                max_sim = sim
        return max_sim
    def name_in_email(self, name, email):
        if not name or not email or pd.isna(name) or pd.isna(email):
            return False
        name = str(name).lower().strip()
        email_local = str(email).split('@')[0].lower() if '@' in email else str(email).lower()
        return name in email_local

    def convert_amount(self, amount, from_cur, to_cur):
        try:
            amount = float(amount)
        except (ValueError, TypeError):
            return None, None
        from_cur = str(from_cur).replace("['", "").replace("']", "").replace("'", "").strip().upper()
        to_cur = str(to_cur).replace("['", "").replace("']", "").replace("'", "").strip().upper()
        if from_cur == to_cur:
            return amount, 1.0
        # Direct rate
        rate = self.exchange_rate_map.get((from_cur, to_cur))
        if rate:
            return amount * rate, rate
        # Inverse rate
        inverse_rate = self.exchange_rate_map.get((to_cur, from_cur))
        if inverse_rate and inverse_rate != 0:
            inv = 1.0 / inverse_rate
            return amount * inv, inv
        # USD bridge with special cases
        if from_cur == 'USD':
            usd_rate1 = self.exchange_rate_map.get(('USD', to_cur))
            if usd_rate1:
                return amount * usd_rate1, usd_rate1
            # Inverse for USD -> to_cur
            inv_usd_rate1 = self.exchange_rate_map.get((to_cur, 'USD'))
            if inv_usd_rate1 and inv_usd_rate1 != 0:
                inv = 1.0 / inv_usd_rate1
                return amount * inv, inv
        elif to_cur == 'USD':
            usd_rate2 = self.exchange_rate_map.get((from_cur, 'USD'))
            if usd_rate2:
                return amount * usd_rate2, usd_rate2
            # Inverse for from_cur -> USD
            inv_usd_rate2 = self.exchange_rate_map.get(('USD', from_cur))
            if inv_usd_rate2 and inv_usd_rate2 != 0:
                inv = 1.0 / inv_usd_rate2
                return amount * inv, inv
        else:
            usd_rate1 = self.exchange_rate_map.get(('USD', to_cur))
            inv_usd_rate1 = self.exchange_rate_map.get((to_cur, 'USD')) if usd_rate1 is None else None
            if inv_usd_rate1 and inv_usd_rate1 != 0:
                usd_rate1 = 1.0 / inv_usd_rate1
            usd_rate2 = self.exchange_rate_map.get((from_cur, 'USD'))
            inv_usd_rate2 = self.exchange_rate_map.get(('USD', from_cur)) if usd_rate2 is None else None
            if inv_usd_rate2 and inv_usd_rate2 != 0:
                usd_rate2 = 1.0 / inv_usd_rate2
            if usd_rate1 is not None and usd_rate2 is not None:
                return amount * usd_rate2 * usd_rate1, usd_rate2 * usd_rate1
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
    def _match_processor_to_crm_row(self, proc_row, crm_row, proc_config, crm_idx):
        def safe_lower_strip(value):
            if pd.isna(value):
                return ''
            return str(value).lower().strip()
        """
        Try to match one processor row to one CRM row.
        Return (match_dict, diagnostics_dict) or None if no match.
        """
        # Extract relevant fields
        proc_amount = abs(proc_row.get('proc_amount')) # Use abs for positive comparison
        proc_currency = proc_row.get('proc_currency')
        proc_email = safe_lower_strip(proc_row.get('proc_email'))
        proc_last4 = str(proc_row.get('proc_last4') or '').strip()
        proc_tp = str(proc_row.get('proc_tp') or '').strip()
        crm_amount = abs(crm_row.get('crm_amount')) # Use abs for CRM too
        crm_currency = crm_row.get('crm_currency')
        crm_email_raw = safe_lower_strip(crm_row.get('crm_email')) # Raw for splitting
        crm_emails = [safe_lower_strip(e) for e in crm_email_raw.split(',')] # Split multiple emails
        crm_last4 = str(crm_row.get('crm_last4') or '').strip()
        crm_tp = str(crm_row.get('crm_tp') or '').strip()
        # Convert processor amount to CRM currency
        proc_amount_conv, rate = self.convert_amount(proc_amount, proc_currency, crm_currency)
        if proc_amount_conv is None:
            return None
        # Loose tolerance for acceptance (to allow potential matches)
        accept_abs_tol = 0.1
        accept_rel_tol = proc_config.tolerance * crm_amount * 2 # Looser rel tol
        accept_tol = max(accept_abs_tol, 500) if proc_currency == crm_currency else max(accept_rel_tol, 500)
        diff = abs(proc_amount_conv - crm_amount)
        if diff > accept_tol:
            return None
        # Check last4 match or email similarity
        last4_valid = crm_last4 not in ("0", "0000", "", "nan")
        last4_match = last4_valid and crm_last4 == proc_last4
        if last4_valid and crm_last4 != proc_last4:
            return None
        # Processor-specific matching logic
        proc_name = proc_row.get('proc_processor_name').lower()
        name_fallback_used = False
        if proc_name == 'shift4':
            # Use shift4 prefix logic, handling multiple CRM emails
            proc_email = proc_email.lower()
            loc_proc = proc_email.split('@')[0] if proc_email else ''
            email_prefix = any(
                (c_email.split('@')[0][:2] == loc_proc[:2]) if '@' in c_email else False
                for c_email in crm_emails
            )
            crm_first = safe_lower_strip(crm_row.get('crm_firstname', ''))
            crm_last = safe_lower_strip(crm_row.get('crm_lastname', ''))
            proc_first = safe_lower_strip(proc_row.get('proc_firstname', ''))
            proc_last = safe_lower_strip(proc_row.get('proc_lastname', ''))
            first_prefix = crm_first[:3] == proc_first[:3] if crm_first and proc_first else False
            last_prefix = crm_last[:3] == proc_last[:3] if crm_last and proc_last else False
            if not (email_prefix or first_prefix or last_prefix):
                return None
            email_sim = 1 if email_prefix or first_prefix or last_prefix else 0 # Fake sim for consistency
            name_fallback_used = first_prefix or last_prefix
        elif proc_name in ('safecharge', 'powercash'):
            # Use standard logic (assuming from _match_standard_row, similar to shift4 but full email or name)
            loc_proc = proc_email.split('@')[0] if '@' in proc_email else proc_email
            email_prefix = any(
                (c_email.split('@')[0] == loc_proc) if '@' in c_email else (c_email == loc_proc)
                for c_email in crm_emails
            )
            crm_first = safe_lower_strip(crm_row.get('crm_firstname', ''))
            crm_last = safe_lower_strip(crm_row.get('crm_lastname', ''))
            proc_first = safe_lower_strip(proc_row.get('proc_firstname', ''))
            proc_last = safe_lower_strip(proc_row.get('proc_lastname', ''))
            first_match = crm_first == proc_first if crm_first and proc_first else False
            last_match = crm_last == proc_last if crm_last and proc_last else False
            if not (email_prefix or first_match or last_match):
                return None
            email_sim = self.enhanced_email_similarity(crm_email_raw, proc_email) # Use raw to let method handle split
            name_fallback_used = first_match or last_match
        elif proc_name == 'trustpayments':
            crm_tp_norm = normalize_string(crm_tp)
            crm_last4_norm = normalize_string(crm_last4) if last4_valid else ''
            crm_first = safe_lower_strip(crm_row.get('crm_firstname', ''))
            crm_last = safe_lower_strip(crm_row.get('crm_lastname', ''))
            crm_tokens = set(crm_first.split() + crm_last.split())
            proc_tp_norm = normalize_string(proc_tp)
            proc_last4_norm = normalize_string(proc_last4)
            proc_first = safe_lower_strip(proc_row.get('proc_firstname', ''))
            proc_last = safe_lower_strip(proc_row.get('proc_lastname', ''))
            proc_tokens = set(proc_first.split() + proc_last.split())
            email_sim = self.enhanced_email_similarity(crm_email_raw, proc_email) # Use raw
            full_name_match = crm_tokens == proc_tokens
            first_or_last_name_match = bool(crm_tokens & proc_tokens)
            # Match tiers logic (adapted for single row comparison)
            tier = None
            if crm_tp_norm and proc_tp_norm and crm_tp_norm == proc_tp_norm and last4_valid and crm_last4_norm == proc_last4_norm:
                tier = 1
            elif crm_tp_norm and proc_tp_norm and crm_tp_norm == proc_tp_norm and email_sim > 0.75:
                tier = 2
            elif last4_valid and crm_last4_norm == proc_last4_norm and email_sim > 0.75:
                tier = 3
            elif last4_valid and crm_last4_norm == proc_last4_norm and first_or_last_name_match:
                tier = 4
            elif email_sim > 0.75 and full_name_match:
                tier = 5
            if tier is None:
                return None
            last4_match = tier in [1, 3, 4]
            name_fallback_used = tier in [4, 5]
        else:
            # Normal email_sim for other processors
            email_sim = self.enhanced_email_similarity(crm_email_raw, proc_email) # Use raw
        if proc_config.require_email and email_sim < 0.2 and not last4_match: # Lowered to 0.2
            return None
        # Require minimum email sim regardless of last4
        if email_sim < 0.3: # Enforce this to prevent last4-only matches
            return None
        # If required last4 and no match, fail (but make optional for cross)
        if proc_config.require_last4 and not last4_match and email_sim < 0.5: # Lowered from 0.75
            return None
        # If we reach here, it's a match!
        # Tight tolerance for payment_status
        status_abs_tol = 0.1
        status_rel_tol = proc_config.tolerance * crm_amount
        status_tol = max(status_abs_tol, status_rel_tol) if proc_currency == crm_currency else max(status_rel_tol,
                                                                                                   status_abs_tol)
        amount_diff = proc_amount_conv - crm_amount
        abs_amount_diff = abs(amount_diff)
        if abs_amount_diff > status_tol:
            payment_status = 0
            direction = "more" if amount_diff > 0 else "less"
            comment = f"Client received {abs_amount_diff:.2f} {crm_currency} {direction}"
        else:
            payment_status = 1
            comment = "Cross-processor fallback match"
        # Build match dict (similar to other match_*_row methods)
        match = {
            'crm_date': crm_row.get('crm_date'),
            'crm_email': crm_email_raw, # Keep original raw for output
            'crm_firstname': crm_row.get('crm_firstname', ''),
            'crm_lastname': crm_row.get('crm_lastname', ''),
            'crm_last4': crm_last4,
            'crm_currency': crm_currency,
            'crm_amount': -crm_amount if crm_row.get('crm_amount', 0) < 0 else crm_amount, # Restore sign if negative
            'crm_processor_name': crm_row.get('crm_processor_name'),
            'regulation': crm_row.get('regulation', ''),
            'proc_date': proc_row.get('proc_date'),
            'proc_email': proc_email,
            'proc_firstname': proc_row.get('proc_firstname', ''),
            'proc_lastname': proc_row.get('proc_lastname', ''),
            'proc_tp': proc_row.get('proc_tp', ''),
            'crm_tp': crm_row.get('crm_tp', ''),
            'proc_last4': proc_last4,
            'proc_currency': proc_currency,
            'proc_amount': proc_amount,
            'proc_amount_crm_currency': proc_amount_conv,
            'proc_processor_name': proc_row.get('proc_processor_name'),
            'email_similarity_avg': round(email_sim, 4),
            'last4_match': last4_match,
            'name_fallback_used': name_fallback_used,
            'exact_match_used': last4_match and abs_amount_diff <= status_tol,
            'converted': proc_currency != crm_currency,
            'proc_combo_len': 1,
            'crm_combo_len': 1,
            'match_status': 1,
            'payment_status': payment_status,
            'comment': comment,
            'matched_proc_indices': [proc_row.name], # proc_row.name is index in DataFrame
            'crm_row_index': crm_idx
        }
        return match, {}
    def _is_better_match(self, new_match, old_match):
        # Primary: higher email similarity
        if new_match['email_similarity_avg'] > old_match['email_similarity_avg']:
            return True
        if new_match['email_similarity_avg'] < old_match['email_similarity_avg']:
            return False
        # Tiebreaker 1: prefer last4 match
        if new_match['last4_match'] > old_match['last4_match']:
            return True
        if new_match['last4_match'] < old_match['last4_match']:
            return False
        # Tiebreaker 2: prefer exact match
        if new_match.get('exact_match_used', False) > old_match.get('exact_match_used', False):
            return True
        # Default: not better (keep old)
        return False
    #Handles matched unmatched rows of different processors
    def _cross_processor_last_chance(self, crm_df, processor_df, used_crm, used_proc, matches):
        card_group = {"safecharge", "shift4", "powercash", "trustpayments", "safechargeuk"}
        ewallet_group = {"paypal", "skrill", "neteller"}
        other_group = set()
        def get_group(proc_name):
            proc_lower = proc_name.lower()
            if proc_lower in card_group:
                return 'card'
            elif proc_lower in ewallet_group:
                return 'ewallet'
            else:
                return 'other'
        unmatched_proc_rows = processor_df[
            (~processor_df.index.isin(used_proc))
        ]
        if unmatched_proc_rows.empty:
            return
        proc_dict = processor_df.to_dict('index')
        crm_dict = crm_df.to_dict('index')
        crm_last4_map = crm_df.groupby('crm_last4').indices
        for proc_idx, proc_row in unmatched_proc_rows.iterrows():
            candidate_crm_indices = [i for i in crm_df.index if i not in used_crm]
            proc_group = get_group(proc_row['proc_processor_name'])
            best_match = None
            best_diag = None
            for crm_idx in candidate_crm_indices:
                crm_row = crm_dict[crm_idx]
                crm_group = get_group(crm_row['crm_processor_name'])
                if proc_row['proc_processor_name'].lower() in ['barclays'] and crm_row[
                    'regulation'].lower() != 'uk':
                    continue
                if crm_group != proc_group:
                    continue # Skip if different groups

                # Skip cross for safechargeuk CRM with safecharge proc
                if crm_row['crm_processor_name'].lower() == 'safechargeuk' and proc_row['proc_processor_name'].lower() == 'safecharge':
                    continue
                proc_proc_name = proc_row['proc_processor_name'].lower()
                proc_config = self.get_processor_config(proc_proc_name)
                match_result = self._match_processor_to_crm_row(
                    proc_row, crm_row, proc_config, crm_idx
                )
                if match_result is not None:
                    match, diag = match_result
                    if match and (best_match is None or self._is_better_match(match, best_match)):
                        best_match = match
                        best_diag = diag
            if best_match:
                used_proc.add(proc_idx)
                used_crm.add(best_match['crm_row_index'])
                # Fixed comment: Set directly to include processor names (CRM first, then PROC), no duplication or brackets
                fallback_comment = f"Cross-processor fallback match - {best_match.get('crm_processor_name', 'unknown')} matched {proc_row['proc_processor_name']}"
                best_match['comment'] = fallback_comment # Override any existing comment to avoid duplication
                best_match['cross_processor_fallback'] = True # Flag for Rule 3 to skip adding "differ" comment
                match['warning'] = True
                matches.append(best_match)
                self.metrics['matched_fallback'] += 1
                self.metrics['unmatched'] -= 1
                self.metrics['currency_matches'][best_match['crm_currency']] = self.metrics['currency_matches'].get(
                    best_match['crm_currency'], 0) + 1
                if best_match['payment_status'] == 1:
                    self.metrics['correct_payments'] += 1
                else:
                    self.metrics['incorrect_payments'] += 1

    def _cross_regulation_matching(self, crm_df, processor_df, used_crm, used_proc, matches):
        unmatched_crm_indices = [i for i in crm_df.index if i not in used_crm]
        if not unmatched_crm_indices:
            return
        crm_dict = crm_df.to_dict('index')
        proc_dict = processor_df.to_dict('index')
        for crm_idx in unmatched_crm_indices:
            crm_row = crm_dict[crm_idx]
            crm_reg_lower = crm_row['regulation'].lower()
            crm_proc_lower = crm_row['crm_processor_name'].lower()
            if crm_reg_lower == 'uk':
                target_proc = 'safecharge'  # UK CRM → ROW safecharge
                match_func = self._match_standard_row
            else:
                target_proc = 'safechargeuk'  # ROW CRM → UK safechargeuk
                match_func = self._match_safechargeuk_row
        proc_config = self.get_processor_config(target_proc)
        indices = [i for i in proc_dict if
                   proc_dict[i]['proc_processor_name'].lower() == target_proc and i not in used_proc]
        if not indices:
            proc_config = self.get_processor_config(target_proc)
            indices = [i for i in proc_dict if
                       proc_dict[i]['proc_processor_name'].lower() == target_proc and i not in used_proc]
            sample_crm_email = str(crm_row.get('crm_email', '')).lower()
            sample_crm_last4 = str(crm_row.get('crm_last4', ''))
            if 'bristol' in sample_crm_email or 'ronpierre' in sample_crm_email or sample_crm_last4 in ['824', '476']:
                print(
                    f"DEBUG Cross enter: crm_idx={crm_idx}, reg={crm_reg_lower}, crm_proc={crm_proc_lower}, target_proc={target_proc}, indices_len={len(indices)}")
                if indices:
                    sample_proc = proc_dict[indices[0]]
                    print(
                        f"DEBUG Cross sample proc: last4={sample_proc.get('proc_last4')}, email={sample_proc.get('proc_email')}, proc_name={sample_proc.get('proc_processor_name')}")
            temp_proc_dict = {k: proc_dict[k] for k in indices}
            temp_last4_map = defaultdict(list)
            for ii in indices:
                l4 = normalize_string(str(proc_dict[ii].get('proc_last4', '')), is_last4=True)
                if l4:
                    temp_last4_map[l4].append(ii)
            result = match_func(crm_row, temp_proc_dict, temp_last4_map, used_proc, proc_config, skip_proc_check=True,
                                cross_reg=True, crm_idx=crm_idx)
            if result:
                match, diag = result
                if match:
                    fallback_comment = f"Cross-regulation fallback match - {crm_row['crm_processor_name']} matched {match['proc_processor_name']}"
                    match['comment'] = fallback_comment
                    match['cross_regulation_fallback'] = True
                    matches.append(match)

                    used_crm.add(crm_idx)
                    used_proc.update(match['matched_proc_indices'])
                    self.metrics['matched_fallback'] += 1
                    self.metrics['unmatched'] -= 1
                    self.metrics['currency_matches'][match['crm_currency']] = self.metrics['currency_matches'].get(
                        match['crm_currency'], 0) + 1
                    if match['payment_status'] == 1:
                        self.metrics['correct_payments'] += 1
                    else:
                        self.metrics['incorrect_payments'] += 1
            sample_crm_email = str(crm_row.get('crm_email', '')).lower()
            sample_crm_last4 = str(crm_row.get('crm_last4', ''))
            if 'bristol' in sample_crm_email or 'ronpierre' in sample_crm_email or sample_crm_last4 in ['824', '476']:
                if result and result[0]:
                    print(
                        f"DEBUG Cross match success for {crm_idx}: last4_match={result[0].get('last4_match')}, email_sim={result[0].get('email_similarity_avg')}")
                else:
                    print(f"DEBUG Cross no match for {crm_idx}")
    def _flag_warning(self, matches, processor_df):
        used_real = {
            idx
            for m in matches
            if m.get('match_status') == 1
            for idx in m.get('matched_proc_indices', [])
        }
        unmatched_emails = processor_df.loc[
            ~processor_df.index.isin(used_real),
            'proc_email'
        ].dropna().unique()
        unmatched_last4s = processor_df.loc[
            ~processor_df.index.isin(used_real),
            'proc_last4'
        ].dropna().astype(str)
        for m in matches:
            m['warning'] = False # Default: no warning
        # Rule 1: General email similarity for unmatched rows (above 0.65)
        unmatched_crm_indices = [i for i, m in enumerate(matches) if
                                 m['match_status'] == 0 and m.get('crm_date') is not None]
        unmatched_proc_indices = [i for i, m in enumerate(matches) if
                                  m['match_status'] == 0 and m.get('crm_date') is None]
        flagged_email_sim_proc_to_crm = defaultdict(list)
        thresh_email = 0.65
        for proc_i in unmatched_proc_indices:
            proc_email = matches[proc_i].get('proc_email', '')
            if pd.isna(proc_email):
                proc_email = ''
            elif isinstance(proc_email, list):
                proc_email = proc_email[0] if proc_email else ''
            else:
                proc_email = str(proc_email)
            proc_email = re.sub(r"^\[\'|\'\]$|\[|\]|'|\"", "", proc_email).strip()
            if not proc_email:
                continue
            for crm_i in unmatched_crm_indices:
                crm_email = matches[crm_i].get('crm_email', '').lower()
                sim = self.enhanced_email_similarity(crm_email, proc_email)
                if sim > thresh_email:
                    flagged_email_sim_proc_to_crm[proc_i].append((crm_i, sim))
        for proc_i, crm_list in flagged_email_sim_proc_to_crm.items():
            if crm_list:
                matches[proc_i]['warning'] = True
                comments_proc = ' . '.join(
                    [f"Matched similar email :{matches[c[0]].get('crm_email', '')} in row {c[0] + 1} (sim {c[1]:.3f})"
                     for c in crm_list])
                current_proc = matches[proc_i].get('comment', '')
                matches[proc_i]['comment'] = current_proc + ' . ' + comments_proc if current_proc else comments_proc
                proc_email = matches[proc_i].get('proc_email', '')
                if pd.isna(proc_email):
                    proc_email = ''
                elif isinstance(proc_email, list):
                    proc_email = proc_email[0] if proc_email else ''
                else:
                    proc_email = str(proc_email)
                proc_email = re.sub(r"^\[\'|\'\]$|\[|\]|'|\"", "", proc_email).strip()
                for crm_tuple in crm_list:
                    crm_i = crm_tuple[0]
                    matches[crm_i]['warning'] = True
                    comment_crm = f"Matched similar email :{proc_email} in row {proc_i + 1}"
                    current_crm = matches[crm_i].get('comment', '')
                    matches[crm_i]['comment'] = current_crm + ' . ' + comment_crm if current_crm else comment_crm
                print(
                    f"Row {proc_i + 1} breaks Rule 1: General email similarity match to rows {[c[0] + 1 for c in crm_list]}")
        # Rule 2: Last4 digits matching
        crm_last4s = set()
        for m in matches:
            if m['match_status'] == 0 and m.get('crm_date') is not None:
                raw = str(m.get('crm_last4', '')).strip()
                if raw.lower() != 'nan' and raw:
                    crm_last4s.add(raw[:-2] if raw.endswith('.0') else raw)
        flagged_unmatched_last4_rows = defaultdict(list)
        flagged_matched_last4_rows = defaultdict(list)
        for i, m in enumerate(matches):
            if m.get('crm_date') is not None:
                continue
            raw = m.get('proc_last4')
            if isinstance(raw, list):
                raw = raw[0] if raw else None
            if not raw:
                continue
            raw_str = str(raw).strip()
            if not raw_str or raw_str.lower() == 'nan':
                continue
            code = raw_str[:-2] if raw_str.endswith('.0') else raw_str
            if code in crm_last4s and not m['warning']:
                m['warning'] = True
                flagged_unmatched_last4_rows[code].append(i)
                print(f"Row {i + 1} breaks Rule 2: Unmatched-processor last4 {raw_str} found in CRM last4s")
        for i, m in enumerate(matches):
            if m.get('crm_date') is None:
                continue
            if m['match_status'] != 0:
                continue
            raw = str(m.get('crm_last4', '')).strip()
            code = raw[:-2] if raw.endswith('.0') else raw
            if code in flagged_unmatched_last4_rows and not m['warning']:
                m['warning'] = True
                flagged_matched_last4_rows[code].append(i)
                print(f"Row {i + 1} breaks Rule 2 propagation: Matching last4 {code}")
        for code in flagged_unmatched_last4_rows:
            unmatched_rows = flagged_unmatched_last4_rows[code]
            matched_rows = flagged_matched_last4_rows.get(code, [])
            if matched_rows:
                matched_str = ', '.join([f"row {r + 1}" for r in matched_rows]) if len(
                    matched_rows) > 1 else f"row {matched_rows[0] + 1}"
                comment_u = f"Matched the same last4 :{code} in {matched_str}"
                for u_i in unmatched_rows:
                    current_comment = matches[u_i].get('comment', '')
                    matches[u_i]['comment'] = current_comment + ' . ' + comment_u if current_comment else comment_u
                unmatched_str = ', '.join([f"row {r + 1}" for r in unmatched_rows]) if len(
                    unmatched_rows) > 1 else f"row {unmatched_rows[0] + 1}"
                comment_m = f"Matched the same last4 :{code} in {unmatched_str}"
                for m_i in matched_rows:
                    current_comment = matches[m_i].get('comment', '')
                    matches[m_i]['comment'] = current_comment + ' . ' + comment_m if current_comment else comment_m
            # Rule 3: Cross processors (modified to handle fallback without duplicate comment)
            for i, m in enumerate(matches):
                if m.get('match_status') == 1:
                    crm_pname = str(m.get('crm_processor_name', '')).lower()
                    proc_pname = str(m.get('proc_processor_name', '')).lower()
                    if (crm_pname == 'safecharge' and proc_pname == 'safechargeuk') or (
                            crm_pname == 'safechargeuk' and proc_pname == 'safecharge'):
                        continue # Treat as same processor, no warning
                    if crm_pname != proc_pname:
                        m['warning'] = True # Still flag warning for cross-processor
                        if not (m.get('cross_processor_fallback', False) or m.get('cross_regulation_fallback', False)):
                            # Only add differ comment if not fallback (avoids duplication with fallback comment)
                            comment = f"Processor names differ ({crm_pname} matched {proc_pname})"
                            current_comment = m.get('comment', '')
                            m['comment'] = current_comment + ' . ' + comment if current_comment else comment
                            print(
                                f"Row {i + 1} breaks Rule 3: Processor names differ ({crm_pname} matched {proc_pname})")
                        else:
                            # For fallback, comment is already set; just log without adding
                            print(f"Row {i + 1} is cross-processor fallback (warning flagged, comment preset)")
        # Rule 4: Partial email matching for shift4, only for rows where crm_processor_name is shift4
        unmatched_crm_indices = [i for i, m in enumerate(matches) if
                                 m['match_status'] == 0 and m.get('crm_date') is not None and
                                 str(m.get('crm_processor_name', '')).lower() == 'shift4']
        unmatched_proc_shift4_indices = [i for i, m in enumerate(matches) if
                                         m['match_status'] == 0 and m.get('crm_date') is None and
                                         str(m.get('proc_processor_name', '')).lower() == 'shift4']
        flagged_shift4_proc_to_crm = defaultdict(list)
        for proc_i in unmatched_proc_shift4_indices:
            proc_email = matches[proc_i].get('proc_email', '')
            if pd.isna(proc_email):
                proc_email = ''
            elif isinstance(proc_email, list):
                proc_email = proc_email[0] if proc_email else ''
            else:
                proc_email = str(proc_email)
            proc_email = re.sub(r"^\[\'|\'\]$|\[|\]|'|\"", "", proc_email).strip()
            if not proc_email or not re.match(r'^[a-z]{2}\*+', proc_email, re.IGNORECASE):
                continue
            prefix = proc_email[:2].lower()
            for crm_i in unmatched_crm_indices:
                crm_email_raw = matches[crm_i].get('crm_email', '').lower()
                crm_emails = [e.strip() for e in crm_email_raw.split(',')] # Handle multiple CRM emails
                if any(e.startswith(prefix) for e in crm_emails):
                    flagged_shift4_proc_to_crm[proc_i].append(crm_i)
        for proc_i, crm_list in flagged_shift4_proc_to_crm.items():
            if crm_list:
                matches[proc_i]['warning'] = True
                comments_proc = ' . '.join(
                    [f"Matched similar email :{matches[c].get('crm_email', '')} in row {c + 1}" for c in crm_list])
                current_proc = matches[proc_i].get('comment', '')
                matches[proc_i]['comment'] = current_proc + ' . ' + comments_proc if current_proc else comments_proc
                proc_email = matches[proc_i].get('proc_email', '')
                if pd.isna(proc_email):
                    proc_email = ''
                elif isinstance(proc_email, list):
                    proc_email = proc_email[0] if proc_email else ''
                else:
                    proc_email = str(proc_email)
                proc_email = re.sub(r"^\[\'|\'\]$|\[|\]|'|\"", "", proc_email).strip()
                for crm_i in crm_list:
                    matches[crm_i]['warning'] = True
                    comment_crm = f"Matched similar email :{proc_email} in row {proc_i + 1}"
                    current_crm = matches[crm_i].get('comment', '')
                    matches[crm_i]['comment'] = current_crm + ' . ' + comment_crm if current_crm else comment_crm
                print(
                    f"Row {proc_i + 1} breaks Rule 4: Shift4 partial email match with prefix {prefix} to rows {[c + 1 for c in crm_list]}")
        # Rule 5: TP matching for unmatched rows of same processor
        crm_tps = defaultdict(set)
        for i, m in enumerate(matches):
            if m['match_status'] == 0 and m.get('crm_date') is not None:
                raw_tp = str(m.get('crm_tp', '')).strip()
                proc_name = str(m.get('crm_processor_name', '')).lower()
                if raw_tp.lower() != 'nan' and raw_tp:
                    crm_tps[proc_name].add(raw_tp)
        flagged_unmatched_tp_rows = defaultdict(list)
        flagged_matched_tp_rows = defaultdict(list)
        for i, m in enumerate(matches):
            if m.get('crm_date') is not None:
                continue
            raw_tp = m.get('proc_tp')
            if isinstance(raw_tp, list):
                raw_tp = raw_tp[0] if raw_tp else None
            if not raw_tp:
                continue
            raw_tp_str = str(raw_tp).strip()
            if not raw_tp_str or raw_tp_str.lower() == 'nan':
                continue
            proc_name = str(m.get('proc_processor_name', '')).lower()
            if raw_tp_str in crm_tps.get(proc_name, set()) and not m['warning']:
                m['warning'] = True
                flagged_unmatched_tp_rows[raw_tp_str].append(i)
                print(
                    f"Row {i + 1} breaks Rule 5: Unmatched-processor TP {raw_tp_str} found in CRM TPs for processor {proc_name}")
        for i, m in enumerate(matches):
            if m.get('crm_date') is None:
                continue
            if m['match_status'] != 0:
                continue
            raw_tp = str(m.get('crm_tp', '')).strip()
            proc_name = str(m.get('crm_processor_name', '')).lower()
            if raw_tp in flagged_unmatched_tp_rows and not m['warning']:
                m['warning'] = True
                flagged_matched_tp_rows[raw_tp].append(i)
                print(f"Row {i + 1} breaks Rule 5 propagation: Matching TP {raw_tp} for processor {proc_name}")
        for tp_code in flagged_unmatched_tp_rows:
            unmatched_rows = flagged_unmatched_tp_rows[tp_code]
            matched_rows = flagged_matched_tp_rows.get(tp_code, [])
            if matched_rows:
                matched_str = ', '.join([f"row {r + 1}" for r in matched_rows]) if len(
                    matched_rows) > 1 else f"row {matched_rows[0] + 1}"
                comment_u = f"Matched the same TP :{tp_code} in {matched_str}"
                for u_i in unmatched_rows:
                    current_comment = matches[u_i].get('comment', '')
                    matches[u_i]['comment'] = current_comment + ' . ' + comment_u if current_comment else comment_u
                unmatched_str = ', '.join([f"row {r + 1}" for r in unmatched_rows]) if len(
                    unmatched_rows) > 1 else f"row {unmatched_rows[0] + 1}"
                comment_m = f"Matched the same TP :{tp_code} in {unmatched_str}"
                for m_i in matched_rows:
                    current_comment = matches[m_i].get('comment', '')
                    matches[m_i]['comment'] = current_comment + ' . ' + comment_m if current_comment else comment_m
    def make_cancelled_rows(self, full_crm_df):
        """
        Return a list of “cancelled” rows (in the same schema as matches) for any
        crm_type == 'withdrawal cancelled', *including* the crm_tp.
        """
        cancelled = []
        mask = full_crm_df['crm_type'].str.lower() == 'withdrawal cancelled'
        for _, row in full_crm_df.loc[mask].iterrows():
            out = create_cancelled_row(row)
            # Carry through regulation column
            out['regulation'] = row.get('regulation', '') # Added
            # Carry through your TP column
            out['crm_tp'] = row.get('crm_tp')
            out['warning'] = False # Explicitly set for cancelled rows
            cancelled.append(out)
        return cancelled

    def match_withdrawals(self, crm_df, processor_df, add_unmatched_proc=True, add_unmatched_crm=True):
        """
        Main reconciliation loop: matches CRM withdrawals to processor withdrawals,
        then flags logic correctness on each match.
        """
        # start timing and metrics
        self.start_time = datetime.now()
        self.metrics['total_crm'] = len(crm_df)
        used_proc, used_crm, matches = set(), set(), []
        last4_map = processor_df.groupby('proc_last4').indices
        proc_dict = processor_df.to_dict('index')
        # estimate runtime
        self._estimate_runtime(crm_df, proc_dict, last4_map)
        # match CRM rows
        for idx, row in crm_df.iterrows():
            if idx in used_crm:
                continue
            if self._check_timeout():
                break
            proc_name = str(row.get('crm_processor_name', '')).strip().lower()
            try:
                result = self._match_crm_row(row, proc_dict, last4_map, used_proc, crm_idx=idx)
                if result is None or not isinstance(result, tuple) or len(result) != 2:
                    raise ValueError(f"_match_crm_row() returned invalid result: {result}")
                match, diag = result
            except Exception as e:
                self.logger.error(f"Error processing row {idx}: {e}")
                match, diag = None, {'failure_reason': str(e)}
            crm_tp_val = row.get('crm_tp')
            if match:
                # insert proc_tp list after proc_email
                proc_tp_vals = [proc_dict[i].get('proc_tp', '') for i in match['matched_proc_indices']]
                ordered = {}
                for k, v in match.items():
                    ordered[k] = v
                    if k == 'proc_email': ordered['proc_tp'] = proc_tp_vals
                    if k == 'crm_lastname': ordered['crm_tp'] = crm_tp_val
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
                print("Matched CRM idx: " + str(idx) + ", crm_email: " + match['crm_email'])
            if idx % 10 == 0:
                self._update_eta(len(crm_df), idx + 1)
            # cross-check no processor data for this CRM
            proc_rows_for = processor_df[processor_df['proc_processor_name'].str.lower().str.strip() == proc_name]
            if proc_rows_for.empty:
                self.diagnostics.append(
                    {'crm_idx': idx, 'failure_reason': 'No processor data found for this CRM processor'})
                continue
        # Cross-regulation matching
        self._cross_regulation_matching(crm_df, processor_df, used_crm, used_proc, matches)
        # last-chance cross-processor matching
        # if self.config.get('enable_cross_processor', False):
        #     self._cross_processor_last_chance(crm_df, processor_df, used_crm, used_proc, matches)
        # Add unmatched CRM rows after all matching (moved outside if)
        for idx in crm_df.index:
            if idx not in used_crm:
                if add_unmatched_crm:
                    unmatched = self._create_unmatched_crm_record(crm_df.loc[idx])
                    ordered = {}
                    for k, v in unmatched.items():
                        ordered[k] = v
                        if k == 'crm_lastname': ordered['crm_tp'] = crm_df.loc[idx].get('crm_tp', '')
                        if k == 'proc_email': ordered['proc_tp'] = []
                    matches.append(ordered)
                    self.metrics['unmatched'] += 1
                    if self.config['enable_diagnostics']:
                        self.diagnostics.append({'crm_idx': idx, 'failure_reason': 'No match found'})
        # any processor-only rows left => unmatched processor
        if add_unmatched_proc:
            for pidx, prow in processor_df.iterrows():
                if pidx not in used_proc:
                    base = {
                        'crm_date': None, 'crm_email': None, 'crm_firstname': None, 'crm_lastname': None,
                        'crm_tp': None, 'crm_last4': None, 'crm_currency': None, 'crm_amount': None,
                        'crm_processor_name': None, 'regulation': '', 'proc_date': [prow.get('proc_date')],
                        'proc_email': [prow.get('proc_email')], 'proc_last4': [prow.get('proc_last4')],
                        'proc_currency': [prow.get('proc_currency')], 'proc_amount': [prow.get('proc_amount')],
                        'proc_processor_name': prow.get('proc_processor_name'),
                        'proc_firstname': [prow.get('proc_firstname')], 'proc_lastname': [prow.get('proc_lastname')],
                        'email_similarity_avg': None, 'last4_match': None, 'name_fallback_used': False,
                        'exact_match_used': False, 'match_status': 0, 'payment_status': 0,
                        'comment': "No matching CRM row found", 'matched_proc_indices': [pidx]
                    }
                    entry = {}
                    for k, v in base.items():
                        entry[k] = v
                        if k == 'proc_email': entry['proc_tp'] = [prow.get('proc_tp', '')]
                    matches.append(entry)
        # finalize timing
        self.metrics['processing_time'] = (datetime.now() - self.start_time).total_seconds()
        self.logger.info(f"Total processing time: {timedelta(seconds=self.metrics['processing_time'])}")
        # ——— FLAG WARNING CORRECTNESS —————————————————————————————————
        if self.config.get('enable_warning_flag', False):
            self._flag_warning(matches, processor_df)
        else:
            for m in matches:
                m['warning'] = ''
        return matches

    def _match_crm_row(self, crm_row, proc_dict, last4_map, used, crm_idx=None):
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
        if proc in ('barclays'):
            return self._match_barclays_row(crm_row, proc_dict, last4_map, used,
                                            self.get_processor_config(proc), crm_idx=crm_idx)
        if proc == 'safecharge' and crm_row.get('regulation', '').lower() == 'uk':
            return self._match_safechargeuk_row(
                crm_row, proc_dict, last4_map, used,
                self.get_processor_config('safechargeuk'), crm_idx=crm_idx
            )
        # alias PowerCash → SafeCharge
        if proc == 'powercash':
            proc = 'safecharge'
        if proc == 'paypal':
            return self._match_paypal_row(
                crm_row, proc_dict, last4_map, used,
                self.get_processor_config('paypal'), crm_idx=crm_idx
            )
        if proc == 'shift4':
            return self._match_shift4_row(
                crm_row, proc_dict, last4_map, used,
                self.get_processor_config('shift4'), crm_idx=crm_idx
            )
        if proc in ('skrill', 'neteller'):
            return self._match_skrill_neteller_row(
                crm_row, proc_dict, last4_map, used,
                self.get_processor_config(proc),
                processor_name=proc, crm_idx=crm_idx
            )
        if proc == 'bitpay':
            return self._match_bitpay_row(
                crm_row, proc_dict, last4_map, used,
                self.get_processor_config('bitpay'), crm_idx=crm_idx
            )
        if proc == "zotapay_paymentasia":
            return self._match_zotapay_paymentasia_row(crm_row, proc_dict, last4_map, used,
                                                       self.get_processor_config(proc), crm_idx=crm_idx)
        if proc == "trustpayments":
            return self._match_trustpayments_row(
                crm_row, proc_dict, last4_map, used,
                self.get_processor_config("trustpayments"), crm_idx=crm_idx
            )
        # Fallback to default standard matcher
        return self._match_standard_row(
            crm_row, proc_dict, last4_map, used,
            self.get_processor_config(proc), crm_idx=crm_idx
        )

    def _match_standard_row(self, crm_row, proc_dict, last4_map, used, proc_config, skip_proc_check=False,
                            cross_reg=False, crm_idx=None):
        crm_last4 = str(crm_row['crm_last4']) if not pd.isna(crm_row['crm_last4']) else ''
        crm_last4_normalized = normalize_string(crm_last4, is_last4=True)
        crm_cur = crm_row['crm_currency']
        crm_amt = crm_row['crm_amount']
        crm_email = crm_row['crm_email']
        if crm_last4_normalized in ['0824', '0476'] or 'bristol' in str(crm_email).lower() or 'ronpierre' in str(
                crm_email).lower():
            print(
                f"DEBUG Match std: crm_last4_norm={crm_last4_normalized}, in_last4_map={crm_last4_normalized in last4_map}")
        crm_first = str(crm_row.get('crm_firstname', ''))
        crm_last = str(crm_row.get('crm_lastname', ''))
        crm_proc_name = crm_row.get('crm_processor_name', '').lower()
        is_apple_pay = 'apple pay' in str(crm_row.get('payment_method', '')).lower() and crm_row.get('regulation',
                                                                                                     '').lower() == 'uk'
        candidates = []
        if self.config.get('force_skip_proc_check', False) or skip_proc_check:
            indices = [i for i in proc_dict if i not in used]
        else:
            indices = [i for i in proc_dict if
                       i not in used and proc_dict[i].get('proc_processor_name', '').lower() == crm_proc_name]
        if crm_last4 and crm_last4_normalized in last4_map and proc_config.require_last4:
            print("crm_last4_normalized: " + crm_last4_normalized)
            print("in_last4_map: " + str(crm_last4_normalized in last4_map))
            if crm_last4_normalized in last4_map:
                map_indices = last4_map[crm_last4_normalized]
                print("map_indices type: " + str(type(map_indices)))
                print("map_indices: " + str(list(map_indices)))
                print("current indices type: " + str(type(indices)))
                print("current indices: " + str(indices))
                map_indices = list(map_indices)  # Convert to list if np.ndarray
                indices = [i for i in map_indices if i in indices]
                print("after intersection: " + str(indices))
            else:
                print("Not in last4_map")
        if '0824' in crm_last4_normalized or '0476' in crm_last4_normalized:
            proc_last4_in_indices = [normalize_string(proc_dict.get(i, {}).get('proc_last4', ''), is_last4=True) for i
                                     in indices]
            print(f"Proc last4 in filtered indices ({len(indices)} total): {proc_last4_in_indices}")
        # if crm_last4_normalized in ['0824', '0476'] or 'bristol' in str(crm_email).lower() or 'ronpierre' in str(
        #         crm_email).lower():
        #     print(f"DEBUG Match std filtered indices_len={len(indices)}")
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
            proc_amt_crm_cur, rate = self.convert_amount(proc_amt, proc_cur, crm_cur)
            if proc_amt_crm_cur is None:
                continue
            proc_email = str(row.get('proc_email', '')) if not pd.isna(row.get('proc_email')) else ''
            proc_email = proc_email.strip()
            email_sim = self.enhanced_email_similarity(crm_email, proc_email)
            proc_last4_str = normalize_string(row.get('proc_last4', ''), is_last4=True)
            # inside _match_standard_row  (same change for _match_safechargeuk_row)
            valid_last4 = crm_last4 not in ("", "0", "0000", "nan")
            last4_match = False  # <-- NEW
            if valid_last4:
                last4_match = crm_last4_normalized == proc_last4_str
            if proc_last4_str in ['0824', '0476'] or 'bristol' in str(proc_email).lower() or 'ronpierre' in str(
                    proc_email).lower():
                print(
                    f"DEBUG Match std check i={i}, proc_last4={proc_last4_str}, last4_match={last4_match}, email_sim={email_sim}")
            name_fallback = False
            # Special Apple Pay logic for UK safecharge
            if is_apple_pay:
                if email_sim < 0.75:
                    if crm_first and crm_last and self.name_in_email(crm_first, proc_email) and self.name_in_email(
                            crm_last, proc_email):
                        name_fallback = True
                    else:
                        continue
            # SafeCharge-specific logic: If proc_email is blank, rely only on last4
            if getattr(proc_config, 'allow_last4_only_if_email_blank', False) and not proc_email:
                if not last4_match:
                    continue
                # Skip all email and name fallback checks below
            else:
                # Normal logic for other cases/processors
                if proc_config.require_last4 and valid_last4 and not last4_match:
                    continue
                if proc_config.enable_name_fallback:
                    if crm_first:
                        name_fallback = self.name_in_email(crm_first, row.get('proc_email', ''))
                    if not name_fallback and crm_last:
                        name_fallback = self.name_in_email(crm_last, row.get('proc_email', ''))
                if proc_config.require_last4 and valid_last4:
                    if not last4_match:
                        continue
                    if proc_config.require_email and email_sim < proc_config.email_threshold and not name_fallback and not (
                            cross_reg and last4_match):
                        continue
                else:
                    if proc_config.require_email and email_sim < proc_config.email_threshold and not name_fallback and not (
                            cross_reg and last4_match):
                        continue
            if proc_last4_str in ['0824', '0476'] or 'bristol' in str(proc_email).lower() or 'ronpierre' in str(
                    proc_email).lower():
                print(f"DEBUG Match std candidate added: i={i}")
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
        if crm_last4_normalized in ['0824', '0476'] or 'bristol' in str(crm_email).lower() or 'ronpierre' in str(
                crm_email).lower():
            if not candidates:
                print("DEBUG Match std: No candidates found")
            else:
                print(f"DEBUG Match std: Candidates len={len(candidates)}")
        if not candidates:
            print("Not returning match for crm_email: " + str(crm_email) + " - reason: No candidates found")
            return None, {'failure_reason': 'No candidates found'}
        candidates.sort(key=lambda c: (
            -c['email_score'],
            -int(c['last4_match']),
            -int(c['name_fallback'])
        ))
        best = candidates[0]
        crm_amt_abs = abs(crm_amt)
        tolerance = max(0.1, proc_config.tolerance * crm_amt_abs)
        print("Best candidate for crm_email: " + crm_email + ", proc_email: " + str(best['row_data'].get(
            'proc_email')) + ", amount_diff: " + str(
            abs(best['proc_amount_crm_currency'] - crm_amt_abs)) + ", tolerance: " + str(tolerance))
        try:
            proc_amt = best['proc_amount_crm_currency']
            proc_amt_abs = abs(proc_amt)
            diff = proc_amt_abs - crm_amt_abs
            abs_diff = abs(diff)
            payment_status = 1 if abs_diff <= tolerance else 0
            comment = ""
            if payment_status == 0:
                if diff > 0:
                    comment = f"Overpaid by {round(diff, 2)} {crm_cur}"
                elif diff < 0:
                    comment = f"Underpaid by {round(-diff, 2)} {crm_cur}"
                else:
                    comment = "Amount mismatch"
            if cross_reg:
                comment = f"Cross-regulation match ({crm_row['regulation']} CRM to {best['row_data'].get('proc_processor_name')} PROC)" if not comment else comment + f"; Cross-regulation match ({crm_row['regulation']} CRM to {best['row_data'].get('proc_processor_name')} PROC)"
            proc_date_raw = best['row_data'].get('proc_date')
            proc_date_raw = clean_field(proc_date_raw)
            proc_date_ts = pd.to_datetime(proc_date_raw, errors='coerce')
            if pd.isna(proc_date_ts):
                proc_date_ts = None
            else:
                proc_date_ts = proc_date_ts.normalize()
            # Wrap the match dict building in try-except as:
            try:
                match = {
                    'crm_date': crm_row.get('crm_date'),
                    'crm_email': crm_email,
                    'crm_firstname': crm_first,
                    'crm_lastname': crm_last,
                    'crm_last4': crm_last4,
                    'crm_currency': crm_cur,
                    'crm_amount': crm_amt,
                    'crm_processor_name': crm_row.get('crm_processor_name'),
                    'regulation': crm_row.get('regulation', ''),
                    'proc_date': proc_date_ts,
                    'proc_email': clean_field(best['row_data'].get('proc_email')),
                    'proc_firstname': clean_field(best['row_data'].get('proc_firstname', '')),
                    'proc_lastname': clean_field(best['row_data'].get('proc_lastname', '')),
                    'proc_last4': clean_field(best['row_data'].get('proc_last4')),
                    'proc_currency': clean_field(best['row_data'].get('proc_currency')),
                    'proc_amount': clean_field(best['row_data'].get('proc_amount')),
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
                if crm_idx is not None:
                    match['crm_row_index'] = crm_idx
                print("Returning match for crm_email: " + str(crm_email))
            except Exception as e:
                print(f"Error building match for crm_email {crm_email}: {str(e)}")
                print("Not returning match for crm_email: " + str(crm_email) + " - reason: " + str(e))
                return None, {'failure_reason': str(e)}
            return match, {}
        except Exception as e:
            print(f"Error building match for crm_email {crm_email}: {str(e)}")
            print("Not returning match for crm_email: " + str(crm_email) + " - reason: " + str(e))
            return None, {'failure_reason': str(e)}

    def _match_zotapay_paymentasia_row(self, crm_row, proc_dict, last4_map, used, proc_config,crm_idx=None):
        crm_tp = str(crm_row.get('crm_tp', '')).strip()
        crm_amt = abs(crm_row['crm_amount'])
        crm_cur = str(crm_row['crm_currency']).strip().upper()
        crm_email = str(crm_row.get('crm_email', '')).strip().lower()
        crm_last4 = str(crm_row.get('crm_last4', '')).strip()
        crm_first = str(crm_row.get('crm_firstname', '')).strip().lower()
        crm_last = str(crm_row.get('crm_lastname', '')).strip().lower()
        crm_full_name = (crm_first + " " + crm_last).strip()
        crm_proc_name = crm_row.get('crm_processor_name', '')
        if not crm_tp:
            return None, {'failure_reason': 'Missing CRM TP'}
        abs_tol = 0.1
        rel_tol = proc_config.tolerance * crm_amt
        tol = max(abs_tol, rel_tol) # Apply relative tolerance universally
        candidates = []
        # Relax processor name matching for zotapay_paymentasia to include variants
        if crm_proc_name.lower() == 'zotapay_paymentasia':
            allowed_names = ['zotapay_paymentasia', 'zotapay', 'paymentasia']
            indices = [i for i in proc_dict if
                       i not in used and proc_dict[i].get('proc_processor_name', '').lower() in allowed_names]
        else:
            indices = [i for i in proc_dict if
                       i not in used and proc_dict[i].get('proc_processor_name', '').lower() == crm_proc_name.lower()]
        for idx in indices:
            row = proc_dict[idx]
            proc_tp = str(row.get('proc_tp', '')).strip()
            if proc_tp != crm_tp:
                continue
            proc_amt_raw = row.get('proc_amount')
            proc_cur = str(row.get('proc_currency', '')).strip().upper()
            if proc_amt_raw is None or proc_cur is None:
                continue
            proc_amt_crm_cur, rate = self.convert_amount(proc_amt_raw, proc_cur, crm_cur)
            if proc_amt_crm_cur is None:
                continue
            proc_email = str(row.get('proc_email', '')).strip().lower()
            proc_last4 = str(row.get('proc_last4', '')).strip()
            proc_first = str(row.get('proc_firstname', '')).strip().lower()
            proc_last = str(row.get('proc_lastname', '')).strip().lower()
            proc_full_name = (proc_first + " " + proc_last).strip()
            email_sim = self.enhanced_email_similarity(crm_email, proc_email)
            last4_valid = crm_last4 not in ("0", "0000", "", "nan")
            last4_match = last4_valid and (crm_last4 == proc_last4)
            name_sim = SequenceMatcher(None, crm_full_name, proc_full_name).ratio()
            name_match = name_sim >= proc_config.name_match_threshold if hasattr(proc_config,
                                                                                 'name_match_threshold') else name_sim >= 0.8
            if not (last4_match or email_sim >= proc_config.email_threshold or name_match):
                continue
            diff = proc_amt_crm_cur - crm_amt
            abs_diff = abs(diff)
            amount_match = abs_diff <= tol
            candidates.append({
                'index': idx,
                'proc_tp': proc_tp,
                'proc_date': row.get('proc_date'),
                'proc_email': proc_email,
                'proc_firstname': proc_first,
                'proc_lastname': proc_last,
                'proc_last4': proc_last4,
                'proc_currency': proc_cur,
                'proc_amount': proc_amt_raw,
                'proc_amount_crm_currency': round(proc_amt_crm_cur, 4),
                'email_similarity': email_sim,
                'last4_match': last4_match,
                'exact_match': last4_match and amount_match,
                'name_fallback': name_match and not (last4_match or email_sim >= proc_config.email_threshold),
                'amount_difference': diff,
                'amount_match': amount_match,
                'rate': rate,
                'name_sim': name_sim
            })
        if not candidates:
            return None, {'failure_reason': f'No zotapay_paymentasia match for TP: {crm_tp}'}
        # Sort candidates by exact match, email similarity, last4 match, name sim
        candidates.sort(key=lambda c: (
            -int(c['exact_match']),
            -c['email_similarity'],
            -int(c['last4_match']),
            -c['name_sim']
        ))
        best_candidate = candidates[0]
        amount_diff = best_candidate['amount_difference']
        payment_status = 1 if best_candidate['amount_match'] else 0
        comment = ""
        if not payment_status:
            if amount_diff > 0:
                comment = f"Overpaid by {round(amount_diff, 2)} {crm_cur}"
            elif amount_diff < 0:
                comment = f"Underpaid by {round(-amount_diff, 2)} {crm_cur}"
            else:
                comment = "Amount mismatch"
        name_fallback_used = best_candidate['name_fallback']
        try:
            match = {
                'crm_date': crm_row.get('crm_date'),
                'crm_email': crm_email,
                'crm_firstname': crm_row.get('crm_firstname', ''),
                'crm_lastname': crm_row.get('crm_lastname', ''),
                'crm_last4': crm_last4,
                'crm_currency': crm_cur,
                'crm_amount': crm_amt,
                'crm_processor_name': crm_row.get('crm_processor_name', 'zotapay_paymentasia'),
                'regulation': crm_row.get('regulation', ''),
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
                'name_fallback_used': name_fallback_used,
                'exact_match_used': best_candidate['exact_match'],
                'converted': best_candidate['proc_currency'] != crm_cur,
                'proc_combo_len': 1,
                'crm_combo_len': 1,
                'match_status': 1,
                'payment_status': payment_status,
                'comment': comment,
                'matched_proc_indices': [best_candidate['index']],
                'amount_difference': round(amount_diff, 2)
            }
            if crm_idx is not None:
                match['crm_row_index'] = crm_idx
            return match, {}
        except Exception as e:
            print(f"Error building zotapay_paymentasia match for crm_email {crm_email}: {str(e)}")
            return None, {'failure_reason': str(e)}
    def _match_bitpay_row(self, crm_row, proc_dict, last4_map, used, proc_config,crm_idx=None):
        crm_cur = crm_row['crm_currency']
        crm_amt = abs(crm_row['crm_amount'])
        crm_email = (crm_row.get('crm_email') or '').lower().strip()
        crm_first = str(crm_row.get('crm_firstname', '')).lower().strip()
        crm_last = str(crm_row.get('crm_lastname', '')).lower().strip()
        crm_full_name = (crm_first + " " + crm_last).strip()
        crm_last4 = str(crm_row.get('crm_last4', '')).strip()
        crm_proc_name = crm_row.get('crm_processor_name', '').lower()
        abs_tol = 0.1
        rel_tol = proc_config.tolerance * crm_amt
        tol = max(abs_tol, rel_tol) # Apply relative tolerance universally
        candidates = []
        indices = [i for i in proc_dict if
                   i not in used and proc_dict[i].get('proc_processor_name', '').lower() == crm_proc_name]
        for i in indices:
            row = proc_dict[i]
            proc_amt_raw = row.get('proc_amount')
            proc_cur = str(row.get('proc_currency', '')).strip()
            if proc_amt_raw is None or proc_cur is None:
                continue
            proc_amt_crm, rate = self.convert_amount(proc_amt_raw, proc_cur, crm_cur)
            if proc_amt_crm is None:
                continue
            proc_email = str(row.get('proc_email', '')).lower().strip()
            email_sim = self.enhanced_email_similarity(crm_email, proc_email)
            email_match = email_sim >= proc_config.email_threshold
            proc_first = str(row.get('proc_firstname', '')).lower().strip()
            proc_last = str(row.get('proc_lastname', '')).lower().strip()
            proc_full_name = (proc_first + " " + proc_last).strip()
            name_match = (
                    SequenceMatcher(None, crm_full_name, proc_full_name).ratio() >= proc_config.name_match_threshold
            )
            if not (email_match or name_match):
                continue
            proc_last4 = str(row.get('proc_last4', '')).strip()
            last4_valid = crm_last4 not in ("0", "0000", "", "nan")
            last4_match = last4_valid and (crm_last4 == proc_last4)
            diff = proc_amt_crm - crm_amt
            abs_diff = abs(diff)
            same_currency = crm_cur == proc_cur
            amount_match = abs_diff <= tol
            candidates.append({
                'index': i,
                'proc_amount_crm_currency': proc_amt_crm,
                'email_score': email_sim,
                'currency': proc_cur,
                'rate': rate,
                'row_data': row,
                'name_fallback': name_match and not email_match,
                'last4_match': last4_match,
                'exact_match': amount_match and last4_match,
                'amount_difference': diff,
                'amount_match': amount_match
            })
        if not candidates:
            return None, {'failure_reason': 'No valid BitPay candidate'}
        # Sort candidates: prioritize exact match, higher email similarity, then name fallback
        candidates.sort(key=lambda c: (
            -int(c['exact_match']),
            -c['email_score'],
            -int(c['name_fallback'])
        ))
        best = candidates[0]
        received = best['proc_amount_crm_currency']
        amount_diff = best['amount_difference']
        if best['amount_match']:
            payment_status = 1
            comment = ""
        else:
            payment_status = 0 # Did not pay correctly (under or over)
            if amount_diff > 0:
                comment = f"Overpaid by {round(amount_diff, 2)} {crm_cur}"
            elif amount_diff < 0:
                comment = f"Underpaid by {round(-amount_diff, 2)} {crm_cur}"
            else:
                comment = "Amount mismatch"
        try:
            match = {
                'crm_date': crm_row.get('crm_date'),
                'crm_email': crm_email,
                'crm_firstname': crm_row.get('crm_firstname', ''),
                'crm_lastname': crm_row.get('crm_lastname', ''),
                'crm_last4': crm_last4,
                'crm_currency': crm_cur,
                'crm_amount': crm_amt,
                'crm_processor_name': crm_row.get('crm_processor_name'),
                'regulation': crm_row.get('regulation', ''),
                'proc_date': best['row_data'].get('proc_date'),
                'proc_email': best['row_data'].get('proc_email'),
                'proc_firstname': best['row_data'].get('proc_firstname', ''),
                'proc_lastname': best['row_data'].get('proc_lastname', ''),
                'proc_last4': best['row_data'].get('proc_last4'),
                'proc_currency': best['row_data'].get('proc_currency'),
                'proc_amount': best['row_data'].get('proc_amount'),
                'proc_amount_crm_currency': best['proc_amount_crm_currency'],
                'proc_processor_name': best['row_data'].get('proc_processor_name'),
                'exchange_rate': best['row_data'].get('exchange_rate', 1.0),
                'email_similarity_avg': round(best['email_score'], 4),
                'last4_match': best['last4_match'],
                'name_fallback_used': best['name_fallback'],
                'exact_match_used': best['exact_match'],
                'match_status': 1,
                'payment_status': payment_status,
                'comment': comment,
                'matched_proc_indices': [best['index']],
                'amount_difference': round(amount_diff, 2)
            }
            if crm_idx is not None:
                match['crm_row_index'] = crm_idx
            return match, {}
        except Exception as e:
            print(f"Error building bitpay match for crm_email {crm_email}: {str(e)}")
            return None, {'failure_reason': str(e)}
    def _match_shift4_row(self, crm_row, proc_dict, last4_map, used, proc_config,crm_idx=None):
        crm_last4_raw = str(crm_row.get('crm_last4', '')).strip()
        crm_last4 = normalize_string(crm_last4_raw)
        crm_cur = crm_row['crm_currency']
        crm_amt = abs(crm_row['crm_amount'])
        crm_email = (crm_row.get('crm_email') or '').lower()
        crm_first = str(crm_row.get('crm_firstname', '')).lower().strip()
        crm_last = str(crm_row.get('crm_lastname', '')).lower().strip()
        crm_proc_name = crm_row.get('crm_processor_name', '').lower()
        if crm_last4 and crm_last4 in last4_map and proc_config.require_last4:
            # Normalize keys in last4_map? (Assuming last4_map keys already normalized)
            indices = [i for i in last4_map.get(crm_last4, []) if
                       i not in used and proc_dict[i].get('proc_processor_name', '').lower() == crm_proc_name]
        else:
            indices = [i for i in proc_dict if
                       i not in used and proc_dict[i].get('proc_processor_name', '').lower() == crm_proc_name]
        candidates = []
        abs_tol = 0.1
        rel_tol = proc_config.tolerance * crm_amt
        tol = max(abs_tol, rel_tol) # Apply relative tolerance universally
        for i in indices:
            row = proc_dict[i]
            proc_amt_raw = row.get('proc_amount')
            proc_cur = row.get('proc_currency')
            if proc_amt_raw is None or proc_cur is None:
                continue
            proc_amt_crm, rate = self.convert_amount(proc_amt_raw, proc_cur, crm_cur)
            if proc_amt_crm is None:
                continue
            proc_last4_raw = str(row.get('proc_last4', '')).strip()
            proc_last4 = normalize_string(proc_last4_raw)
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
                if proc_last4 == crm_last4: # Relax: allow last4-only match
                    email_score = 0.3 # Assign low score for sorting
                else:
                    continue
            amount_diff = proc_amt_crm - crm_amt
            amount_match = abs(amount_diff) <= tol
            candidates.append({
                'index': i,
                'proc_amount_crm_currency': proc_amt_crm,
                'rate': rate,
                'row': row,
                'email_prefix': email_prefix,
                'first_prefix': first_prefix,
                'last_prefix': last_prefix,
                'email_score': email_score,
                'amount_difference': amount_diff,
                'amount_match': amount_match
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
        amount_diff = best['amount_difference']
        if best['amount_match']:
            payment_status = 1
            comment = ""
        else:
            payment_status = 0
            if amount_diff > 0:
                comment = f"Client Overpaid by {round(amount_diff, 2)} {crm_cur}"
            elif amount_diff < 0:
                comment = f"Client Underpaid by {round(-amount_diff, 2)} {crm_cur}"
            else:
                comment = "Amount mismatch"
        try:
            match = {
                'crm_date': crm_row.get('crm_date'),
                'crm_email': crm_email,
                'crm_firstname': crm_row.get('crm_firstname', ''),
                'crm_lastname': crm_row.get('crm_lastname', ''),
                'crm_last4': crm_last4_raw,
                'crm_currency': crm_cur,
                'crm_amount': crm_amt,
                'crm_processor_name': crm_row.get('crm_processor_name'),
                'regulation': crm_row.get('regulation', ''),
                'proc_date': best['row'].get('proc_date'),
                'proc_email': best['row'].get('proc_email'),
                'proc_firstname': best['row'].get('proc_firstname', ''),
                'proc_lastname': best['row'].get('proc_lastname', ''),
                'proc_last4': best['row'].get('proc_last4'),
                'proc_currency': best['row'].get('proc_currency'),
                'proc_amount': best['row'].get('proc_amount'),
                'proc_amount_crm_currency': best['proc_amount_crm_currency'],
                'proc_processor_name': best['row'].get('proc_processor_name'),
                'exchange_rate': best['row'].get('exchange_rate', 1.0),
                'email_similarity_avg': best['email_score'],
                'last4_match': True,
                'name_fallback_used': False,
                'exact_match_used': True,
                'match_status': 1,
                'payment_status': payment_status,
                'comment': comment,
                'matched_proc_indices': [best['index']],
                'amount_difference': round(amount_diff, 2)
            }
            if crm_idx is not None:
                match['crm_row_index'] = crm_idx
            return match, {}
        except Exception as e:
            print(f"Error building shift4 match for crm_email {crm_email}: {str(e)}")
            return None, {'failure_reason': str(e)}

    def _match_paypal_row(self, crm_row, proc_dict, last4_map, used, proc_config,crm_idx=None):
        crm_cur = crm_row['crm_currency']
        crm_amt = abs(crm_row['crm_amount'])
        crm_email = (crm_row.get('crm_email') or '').lower()
        crm_first = str(crm_row.get('crm_firstname', '')).lower().strip()
        crm_last = str(crm_row.get('crm_lastname', '')).lower().strip()
        crm_last4 = str(crm_row.get('crm_last4', '')).strip()
        crm_proc_name = crm_row.get('crm_processor_name', '').lower()  # Add this
        crm_reg = crm_row.get('regulation', '').lower()
        candidates = []
        indices = [i for i in proc_dict if
                   i not in used and proc_dict[i].get('proc_processor_name', '').lower() == crm_proc_name]
        abs_tol = 0.1
        rel_tol = proc_config.tolerance * crm_amt
        tol = max(abs_tol, rel_tol)  # Apply relative tolerance universally
        for i in indices:
            row = proc_dict[i]
            proc_amt = row.get('proc_amount')
            proc_cur = row.get('proc_currency')
            if proc_amt is None or proc_cur is None:
                continue
            # Skip if UK regulation and proc_currency != 'GBP' (to treat as potential cross)
            if crm_reg == 'uk' and proc_cur != 'GBP':
                continue
            # Convert amount to CRM currency
            proc_amt_crm_currency, rate = self.convert_amount(proc_amt, proc_cur, crm_cur)
            if proc_amt_crm_currency is None:
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
            amount_diff = proc_amt_crm_currency - crm_amt
            amount_match = abs(amount_diff) <= tol
            proc_last4 = str(row.get('proc_last4', '')).strip()
            last4_match = (crm_last4 == proc_last4) and crm_last4 not in ("0", "0000", "", "nan")
            tier = 1 if email_match else (2 if name_match else 3)
            candidates.append({
                'index': i,
                'proc_amount_crm_currency': proc_amt_crm_currency,
                'email_score': email_sim,
                'currency': proc_cur,
                'rate': rate,
                'row': row,
                'match_tier': tier,
                'name_fallback': tier > 1,
                'last4_match': last4_match,
                'exact_match': amount_match and last4_match,
                'amount_difference': amount_diff,
                'amount_match': amount_match
            })
        if not candidates:
            return None, {'failure_reason': 'No valid PayPal candidate'}
        # Pick best candidate by tier then email similarity
        candidates.sort(key=lambda c: (c['match_tier'], -c['email_score']))
        best = candidates[0]
        received_amount = round(best['proc_amount_crm_currency'], 4)
        amount_diff = best['amount_difference']
        if best['amount_match']:
            payment_status = 1
            comment = ""
        else:
            payment_status = 0
            if amount_diff > 0:
                comment = f"Overpaid by {round(amount_diff, 2)} {crm_cur}. "
            elif amount_diff < 0:
                comment = f"Underpaid by {round(-amount_diff, 2)} {crm_cur}. "
            else:
                comment = "Amount mismatch. "
        if best['currency'] != crm_cur:
            comment += "Mixed currencies. "
        if best['name_fallback']:
            comment += "Matched by name fallback."
        try:
            match = {
                'crm_date': crm_row.get('crm_date'),
                'crm_email': crm_email,
                'crm_firstname': crm_row.get('crm_firstname', ''),
                'crm_lastname': crm_row.get('crm_lastname', ''),
                'crm_last4': crm_last4,
                'crm_currency': crm_cur,
                'crm_amount': crm_amt,
                'crm_processor_name': crm_row.get('crm_processor_name'),
                'regulation': crm_row.get('regulation', ''),
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
                'matched_proc_indices': [best['index']],
                'amount_difference': round(amount_diff, 2)
            }
            if crm_idx is not None:
                match['crm_row_index'] = crm_idx
            return match, {}
        except Exception as e:
            print(f"Error building paypal match for crm_email {crm_email}: {str(e)}")
            return None, {'failure_reason': str(e)}
    def _match_skrill_neteller_row(self, crm_row, proc_dict, last4_map, used, proc_config, processor_name,crm_idx=None):
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
                try:
                    match = {
                        'crm_date': crm_row.get('crm_date'),
                        'crm_email': crm_email,
                        'crm_firstname': crm_row.get('crm_firstname', ''),
                        'crm_lastname': crm_row.get('crm_lastname', ''),
                        'crm_last4': crm_last4,
                        'crm_currency': crm_cur,
                        'crm_amount': crm_amt,
                        'crm_processor_name': crm_row.get('crm_processor_name'),
                        'regulation': crm_row.get('regulation', ''),
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
                    }
                    if crm_idx is not None:
                        match['crm_row_index'] = crm_idx
                    return match, {}
                except Exception as e:
                    print(f"Error building skrill/neteller exact match for crm_email {crm_email}: {str(e)}")
                    return None, {'failure_reason': str(e)}
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
        try:
            match = {
                'crm_date': crm_row.get('crm_date'),
                'crm_email': crm_email,
                'crm_firstname': crm_row.get('crm_firstname', ''),
                'crm_lastname': crm_row.get('crm_lastname', ''),
                'crm_last4': crm_last4,
                'crm_currency': crm_cur,
                'crm_amount': crm_amt,
                'crm_processor_name': crm_row.get('crm_processor_name'),
                'regulation': crm_row.get('regulation', ''),
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
            }
            if crm_idx is not None:
                match['crm_row_index'] = crm_idx
            return match, {}
        except Exception as e:
            print(f"Error building skrill/neteller fallback match for crm_email {crm_email}: {str(e)}")
            return None, {'failure_reason': str(e)}
    def _match_trustpayments_row(self, crm_row, proc_dict, last4_map, used, proc_config,crm_idx=None):
        def name_similarity(name1, name2):
            return SequenceMatcher(None, name1.lower(), name2.lower()).ratio() > 0.8
        crm_tp = normalize_string(crm_row.get("crm_tp", ""))
        crm_last4_raw = str(crm_row.get("crm_last4", "")).strip()
        crm_last4 = normalize_string(crm_last4_raw)
        crm_email = str(crm_row.get("crm_email", "")).strip().lower()
        crm_first = str(crm_row.get("crm_firstname", "")).strip().lower()
        crm_last = str(crm_row.get("crm_lastname", "")).strip().lower()
        crm_amt = abs(float(crm_row.get("crm_amount", 0)))
        crm_cur = str(crm_row.get("crm_currency", "")).strip().upper()
        crm_tokens = set(crm_first.split() + crm_last.split())
        crm_proc_name = crm_row.get('crm_processor_name', '').lower()
        abs_tol = 0.1
        rel_tol = proc_config.tolerance * crm_amt
        tol = max(abs_tol, rel_tol) # Apply relative tolerance universally
        tier_candidates = {1: [], 2: [], 3: [], 4: [], 5: [], 6: []} # added tier 6
        for idx, proc_row in proc_dict.items():
            if idx in used:
                continue
            if proc_row.get('proc_processor_name', '').lower() != crm_proc_name:
                continue
            proc_tp = normalize_string(proc_row.get("proc_tp", ""))
            proc_last4_raw = str(proc_row.get("proc_last4", "")).strip()
            proc_last4 = normalize_string(proc_last4_raw)
            proc_email = str(proc_row.get("proc_email", "")).strip().lower()
            proc_first = str(proc_row.get("proc_firstname", "")).strip().lower()
            proc_last = str(proc_row.get("proc_lastname", "")).strip().lower()
            proc_amt_raw = proc_row.get("proc_amount")
            proc_cur = str(proc_row.get("proc_currency", "")).strip().upper()
            proc_tokens = set(proc_first.split() + proc_last.split())
            if proc_amt_raw is None:
                continue
            proc_amt_crm_cur, rate = self.convert_amount(proc_amt_raw, proc_cur, crm_cur)
            if proc_amt_crm_cur is None:
                continue
            email_sim = self.enhanced_email_similarity(crm_email, proc_email)
            full_name_match = crm_tokens == proc_tokens
            first_or_last_name_match = bool(crm_tokens & proc_tokens)
            # Match tiers logic:
            if crm_tp and proc_tp and crm_tp == proc_tp and crm_last4 and proc_last4 and crm_last4 == proc_last4:
                # TP + last4 exact
                tier_candidates[1].append((idx, proc_row, proc_amt_crm_cur, email_sim, rate))
            elif crm_tp and proc_tp and crm_tp == proc_tp and email_sim > 0.75:
                # TP + email similarity
                tier_candidates[2].append((idx, proc_row, proc_amt_crm_cur, email_sim, rate))
            elif crm_last4 and proc_last4 and crm_last4 == proc_last4 and email_sim > 0.75:
                # last4 + email similarity
                tier_candidates[3].append((idx, proc_row, proc_amt_crm_cur, email_sim, rate))
            elif crm_last4 and proc_last4 and crm_last4 == proc_last4 and first_or_last_name_match:
                # last4 + (first or last name) match, even if TP differs
                tier_candidates[4].append((idx, proc_row, proc_amt_crm_cur, email_sim, rate))
            elif email_sim > 0.75 and full_name_match:
                # email similarity + full name match
                tier_candidates[5].append((idx, proc_row, proc_amt_crm_cur, email_sim, rate))
            elif crm_last4 and proc_last4 and crm_last4 == proc_last4 and (
                    name_similarity(crm_first, proc_first) or name_similarity(crm_last, proc_last)):
                # last4 + similar firstname or lastname
                tier_candidates[6].append((idx, proc_row, proc_amt_crm_cur, email_sim, rate))
        # Iterate tiers in order of priority
        for tier in range(1, 7):
            candidates = tier_candidates[tier]
            if not candidates:
                continue
            best_candidate = None
            best_err = float('inf')
            for idx, proc_row, proc_amt_crm_cur, email_sim, rate in candidates:
                err = abs(proc_amt_crm_cur - crm_amt)
                if err < best_err:
                    best_candidate = (idx, proc_row, proc_amt_crm_cur, email_sim, rate, err)
                    best_err = err
            if best_candidate:
                idx, proc_row, proc_amt_crm_cur, email_sim, rate, err = best_candidate
                received = round(proc_amt_crm_cur, 4)
                amount_diff = proc_amt_crm_cur - crm_amt
                payment_status = int(err <= tol)
                comment = ""
                if not payment_status:
                    if amount_diff > 0:
                        comment = f"Overpaid by {round(amount_diff, 2)} {crm_cur}"
                    elif amount_diff < 0:
                        comment = f"Underpaid by {round(-amount_diff, 2)} {crm_cur}"
                    else:
                        comment = "Amount mismatch"
                name_fallback_used = (tier in [4, 5, 6])
                try:
                    match = {
                        'crm_date': crm_row.get('crm_date'),
                        'crm_email': crm_email,
                        'crm_firstname': crm_first,
                        'crm_lastname': crm_last,
                        'crm_last4': crm_last4_raw,
                        'crm_currency': crm_cur,
                        'crm_amount': crm_amt,
                        'crm_processor_name': "trustpayments",
                        'regulation': crm_row.get('regulation', ''),
                        'proc_date': proc_row.get('proc_date'),
                        'proc_email': proc_row.get('proc_email'),
                        'proc_firstname': proc_row.get('proc_firstname', ''),
                        'proc_lastname': proc_row.get('proc_lastname', ''),
                        'proc_last4': proc_row.get('proc_last4'),
                        'proc_currency': proc_row.get('proc_currency'),
                        'proc_amount': proc_row.get('proc_amount'),
                        'proc_amount_crm_currency': proc_amt_crm_cur,
                        'proc_processor_name': proc_row.get('proc_processor_name'),
                        'exchange_rate': rate,
                        'email_similarity_avg': round(email_sim, 4),
                        'last4_match': tier in [1, 3, 4, 6],
                        'name_fallback_used': name_fallback_used,
                        'exact_match_used': (tier == 1),
                        'proc_combo_len': 1,
                        'crm_combo_len': 1,
                        'match_status': 1,
                        'payment_status': payment_status,
                        'comment': comment,
                        'matched_proc_indices': [idx],
                        'amount_difference': round(amount_diff, 2)
                    }
                    if crm_idx is not None:
                        match['crm_row_index'] = crm_idx
                    return match, {'trustpayments_candidate': best_candidate}
                except Exception as e:
                    print(f"Error building trustpayments match for crm_email {crm_email}: {str(e)}")
                    return None, {'failure_reason': str(e)}
    def _match_barclays_row(self, crm_row, proc_dict, last4_map, used, proc_config,crm_idx=None):
        crm_last4_raw = str(crm_row.get('crm_last4', '')).strip()
        crm_last4 = normalize_string(crm_last4_raw, is_last4=True)
        crm_tp_raw = str(crm_row.get('crm_tp', '')).strip()
        crm_tp = normalize_string(crm_tp_raw)
        crm_cur = crm_row['crm_currency']
        crm_amt = abs(crm_row['crm_amount'])
        crm_proc_name = crm_row.get('crm_processor_name', '').lower()
        valid_last4 = crm_last4 not in ("", "0", "0000")
        if not valid_last4 or not crm_tp:
            return None, {'failure_reason': 'Missing valid last4 or TP for Barclays match'}
        candidates = []
        indices = [i for i in proc_dict if
                   i not in used and proc_dict[i].get('proc_processor_name', '').lower() == crm_proc_name]
        abs_tol = 0.1
        rel_tol = proc_config.tolerance * crm_amt
        tol = max(abs_tol, rel_tol)
        for i in indices:
            row = proc_dict[i]
            proc_amt_raw = row.get('proc_amount')
            proc_cur = row.get('proc_currency')
            if proc_amt_raw is None or proc_cur is None:
                continue
            proc_amt_crm, rate = self.convert_amount(proc_amt_raw, proc_cur, crm_cur)
            if proc_amt_crm is None:
                continue
            proc_last4_raw = str(row.get('proc_last4', '')).strip()
            proc_last4 = normalize_string(proc_last4_raw, is_last4=True)
            if proc_last4 != crm_last4:
                continue
            proc_tp_raw = str(row.get('proc_tp', '')).strip()
            proc_tp = normalize_string(proc_tp_raw)
            if proc_tp != crm_tp:
                continue
            amount_diff = abs(proc_amt_crm - crm_amt)
            candidates.append({
                'index': i,
                'proc_amount_crm_currency': proc_amt_crm,
                'rate': rate,
                'row_data': row,
                'amount_diff': amount_diff
            })
        if not candidates:
            return None, {'failure_reason': 'No matching Barclays row found'}
        # Sort by amount difference
        candidates.sort(key=lambda c: c['amount_diff'])
        best = candidates[0]
        payment_status = 1 if best['amount_diff'] <= tol else 0
        comment = ""
        if payment_status == 0:
            diff = best['proc_amount_crm_currency'] - crm_amt
            if diff > 0:
                comment = f"Overpaid by {round(diff, 2)} {crm_cur}"
            elif diff < 0:
                comment = f"Underpaid by {round(-diff, 2)} {crm_cur}"
            else:
                comment = "Amount mismatch"
        try:
            match = {
                'crm_date': crm_row.get('crm_date'),
                'crm_email': crm_row.get('crm_email', ''),
                'crm_firstname': crm_row.get('crm_firstname', ''),
                'crm_lastname': crm_row.get('crm_lastname', ''),
                'crm_last4': crm_last4_raw,
                'crm_currency': crm_cur,
                'crm_amount': crm_amt,
                'crm_processor_name': crm_row.get('crm_processor_name'),
                'regulation': crm_row.get('regulation', ''),
                'proc_date': best['row_data'].get('proc_date'),
                'proc_email': best['row_data'].get('proc_email', ''),
                'proc_firstname': best['row_data'].get('proc_firstname', ''),
                'proc_lastname': best['row_data'].get('proc_lastname', ''),
                'proc_last4': best['row_data'].get('proc_last4'),
                'proc_currency': best['row_data'].get('proc_currency'),
                'proc_amount': best['row_data'].get('proc_amount'),
                'proc_amount_crm_currency': round(best['proc_amount_crm_currency'], 4),
                'proc_processor_name': best['row_data'].get('proc_processor_name'),
                'email_similarity_avg': None,
                'last4_match': True,
                'name_fallback_used': False,
                'exact_match_used': payment_status == 1,
                'converted': best['rate'] != 1.0,
                'proc_combo_len': 1,
                'crm_combo_len': 1,
                'match_status': 1,
                'payment_status': payment_status,
                'comment': comment,
                'matched_proc_indices': [best['index']]
            }
            if crm_idx is not None:
                match['crm_row_index'] = crm_idx
            return match, {}
        except Exception as e:
            print(f"Error building barclays match for crm_email {crm_row.get('crm_email', '')}: {str(e)}")
            return None, {'failure_reason': str(e)}

    def _match_safechargeuk_row(self, crm_row, proc_dict, last4_map, used, proc_config, skip_proc_check=False,
                                cross_reg=False, crm_idx=None):
        crm_last4 = str(crm_row['crm_last4']) if not pd.isna(crm_row['crm_last4']) else ''
        crm_last4_normalized = normalize_string(crm_last4, is_last4=True)
        crm_cur = crm_row['crm_currency']
        crm_amt = crm_row['crm_amount']
        crm_email = crm_row['crm_email']
        if crm_last4_normalized in ['0824', '0476'] or 'bristol' in str(crm_email).lower() or 'ronpierre' in str(
                crm_email).lower():
            print(
                f"DEBUG Match safechargeuk: crm_last4_norm={crm_last4_normalized}, in_last4_map={crm_last4_normalized in last4_map}")
        crm_first = str(crm_row.get('crm_firstname', ''))
        crm_last = str(crm_row.get('crm_lastname', ''))
        crm_proc_name = crm_row.get('crm_processor_name', '').lower()
        is_apple_pay = 'apple pay' in str(crm_row.get('payment_method', '')).lower() and crm_row.get('regulation',
                                                                                                     '').lower() == 'uk'
        candidates = []
        if self.config.get('force_skip_proc_check', False) or skip_proc_check:
            indices = [i for i in proc_dict if i not in used]
        else:
            indices = [i for i in proc_dict if
                       i not in used and proc_dict[i].get('proc_processor_name', '').lower() == 'safechargeuk']
        if crm_last4 and crm_last4_normalized in last4_map and proc_config.require_last4:
            map_indices = last4_map[crm_last4_normalized]
            print("map_indices: " + str(map_indices))
            print("current indices before intersection: " + str(indices))
            indices = [i for i in map_indices if i in indices]
            print("after intersection: " + str(indices))
        if '0824' in crm_last4_normalized or '0476' in crm_last4_normalized:
            proc_last4_in_indices = [normalize_string(proc_dict.get(i, {}).get('proc_last4', ''), is_last4=True) for i
                                     in indices]
            print(f"Proc last4 in filtered indices ({len(indices)} total): {proc_last4_in_indices}")
        # if crm_last4_normalized in ['0824', '0476'] or 'bristol' in str(crm_email).lower() or 'ronpierre' in str(
        #         crm_email).lower():
        #     print(f"DEBUG Match safechargeuk filtered indices_len={len(indices)}")
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
            proc_amt_crm_cur, rate = self.convert_amount(proc_amt, proc_cur, crm_cur)
            if proc_amt_crm_cur is None:
                continue
            abs_diff = abs(proc_amt_crm_cur - abs(crm_amt))
            tolerance = max(0.1, proc_config.tolerance * abs(crm_amt))
            if abs_diff > tolerance:
                continue
            proc_email = str(row.get('proc_email', '')) if not pd.isna(row.get('proc_email')) else ''
            proc_email = proc_email.strip()
            email_sim = self.enhanced_email_similarity(crm_email, proc_email)
            proc_last4_str = normalize_string(row.get('proc_last4', ''), is_last4=True)
            valid_last4 = crm_last4 not in ("", "0", "0000", "nan")
            last4_match = False
            if valid_last4:
                last4_match = crm_last4_normalized == proc_last4_str
            if proc_last4_str in ['0824', '0476'] or 'bristol' in str(proc_email).lower() or 'ronpierre' in str(
                    proc_email).lower():
                print(
                    f"DEBUG Match safechargeuk check i={i}, proc_last4={proc_last4_str}, last4_match={last4_match}, email_sim={email_sim}")
            name_fallback = False
            # Special Apple Pay logic for UK safecharge
            if is_apple_pay:
                if email_sim < 0.75:
                    if crm_first and crm_last and self.name_in_email(crm_first, proc_email) and self.name_in_email(
                            crm_last, proc_email):
                        name_fallback = True
                    else:
                        continue
            # Special logic for safechargeuk when no last4
            if not valid_last4:
                if email_sim >= 0.8:
                    last4_match = False
                else:
                    continue
            # SafeCharge-specific logic: If proc_email is blank, rely only on last4
            if getattr(proc_config, 'allow_last4_only_if_email_blank', False) and not proc_email:
                if not last4_match:
                    continue
                # Skip all email and name fallback checks below
            else:
                # Normal logic for other cases/processors
                if proc_config.require_last4 and valid_last4 and not last4_match:
                    continue
                if proc_config.enable_name_fallback:
                    if crm_first:
                        name_fallback = self.name_in_email(crm_first, row.get('proc_email', ''))
                    if not name_fallback and crm_last:
                        name_fallback = self.name_in_email(crm_last, row.get('proc_email', ''))
                if proc_config.require_last4 and valid_last4:
                    if not last4_match:
                        continue
                    if proc_config.require_email and email_sim < proc_config.email_threshold and not name_fallback and not (
                            cross_reg and last4_match):
                        continue
                else:
                    if proc_config.require_email and email_sim < proc_config.email_threshold and not name_fallback and not (
                            cross_reg and last4_match):
                        continue
            if proc_last4_str in ['0824', '0476'] or 'bristol' in str(proc_email).lower() or 'ronpierre' in str(
                    proc_email).lower():
                print(f"DEBUG Match safechargeuk candidate added: i={i}")
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
        if crm_last4_normalized in ['0824', '0476'] or 'bristol' in str(crm_email).lower() or 'ronpierre' in str(
                crm_email).lower():
            if not candidates:
                print("DEBUG Match safechargeuk: No candidates found")
            else:
                print(f"DEBUG Match safechargeuk: Candidates len={len(candidates)}")
        if not candidates:
            print("Not returning match for crm_email: " + str(crm_email) + " - reason: No candidates found")
            return None, {'failure_reason': 'No candidates found'}
        candidates.sort(key=lambda c: (
            -c['email_score'],
            -int(c['last4_match']),
            -int(c['name_fallback'])
        ))
        best = candidates[0]
        crm_amt_abs = abs(crm_amt)
        tolerance = max(0.1, proc_config.tolerance * crm_amt_abs)
        print("Best candidate for crm_email: " + crm_email + ", proc_email: " + str(best['row_data'].get(
            'proc_email')) + ", amount_diff: " + str(
            abs(best['proc_amount_crm_currency'] - crm_amt_abs)) + ", tolerance: " + str(tolerance))
        try:
            proc_amt = best['proc_amount_crm_currency']
            proc_amt_abs = abs(proc_amt)
            diff = proc_amt_abs - crm_amt_abs
            abs_diff = abs(diff)
            payment_status = 1 if abs_diff <= tolerance else 0
            comment = ""
            if payment_status == 0:
                if diff > 0:
                    comment = f"Overpaid by {round(diff, 2)} {crm_cur}"
                elif diff < 0:
                    comment = f"Underpaid by {round(-diff, 2)} {crm_cur}"
                else:
                    comment = "Amount mismatch"
            if cross_reg:
                comment = f"Cross-regulation match ({crm_row['regulation']} CRM to {best['row_data'].get('proc_processor_name')} PROC)" if not comment else comment + f"; Cross-regulation match ({crm_row['regulation']} CRM to {best['row_data'].get('proc_processor_name')} PROC)"
            proc_date_raw = best['row_data'].get('proc_date')
            proc_date_raw = clean_field(proc_date_raw)
            proc_date_ts = pd.to_datetime(proc_date_raw, errors='coerce')
            if pd.isna(proc_date_ts):
                proc_date_ts = None
            else:
                proc_date_ts = proc_date_ts.normalize()
            # Wrap the match dict building in try-except as:
            try:
                match = {
                    'crm_date': crm_row.get('crm_date'),
                    'crm_email': crm_email,
                    'crm_firstname': crm_first,
                    'crm_lastname': crm_last,
                    'crm_last4': crm_last4,
                    'crm_currency': crm_cur,
                    'crm_amount': crm_amt,
                    'crm_processor_name': crm_row.get('crm_processor_name'),
                    'regulation': crm_row.get('regulation', ''),
                    'proc_date': proc_date_ts,
                    'proc_email': clean_field(best['row_data'].get('proc_email')),
                    'proc_firstname': clean_field(best['row_data'].get('proc_firstname', '')),
                    'proc_lastname': clean_field(best['row_data'].get('proc_lastname', '')),
                    'proc_last4': clean_field(best['row_data'].get('proc_last4')),
                    'proc_currency': clean_field(best['row_data'].get('proc_currency')),
                    'proc_amount': clean_field(best['row_data'].get('proc_amount')),
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
                if crm_idx is not None:
                    match['crm_row_index'] = crm_idx
                print("Returning match for crm_email: " + str(crm_email))
                return match, {}
            except Exception as e:
                print(f"Error building match for crm_email {crm_email}: {str(e)}")
                print("Not returning match for crm_email: " + str(crm_email) + " - reason: " + str(e))
                return None, {'failure_reason': str(e)}
        except Exception as e:
            print(f"Error building match for crm_email {crm_email}: {str(e)}")
            print("Not returning match for crm_email: " + str(crm_email) + " - reason: " + str(e))
            return None, {'failure_reason': str(e)}

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
            'regulation': crm_row.get('regulation', ''),
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

import pandas as pd
import numpy as np
from src.config import setup_dirs_for_reg
from src.utils import load_excel_if_exists, drop_cols
def match_withdrawals_for_date(date_str: str, exchange_rate_map: dict):
    # Get directories for ROW and UK
    row_dirs = setup_dirs_for_reg('row', create=False)
    uk_dirs = setup_dirs_for_reg('uk', create=False)
    # Load combined CRM and processor files for UK and ROW
    uk_crm_path = uk_dirs['combined_crm_dir'] / date_str / "combined_crm_withdrawals.xlsx"
    uk_proc_path = uk_dirs[
                       'processed_processor_dir'] / "combined" / date_str / "combined_processor_withdrawals.xlsx"
    row_crm_path = row_dirs['combined_crm_dir'] / date_str / "combined_crm_withdrawals.xlsx"
    row_proc_path = row_dirs[
                        'processed_processor_dir'] / "combined" / date_str / "combined_processor_withdrawals.xlsx"
    uk_crm = pd.read_excel(uk_crm_path) if uk_crm_path.exists() else pd.DataFrame()
    uk_proc = pd.read_excel(uk_proc_path) if uk_proc_path.exists() else pd.DataFrame()
    row_crm = pd.read_excel(row_crm_path) if row_crm_path.exists() else pd.DataFrame()
    row_proc = pd.read_excel(row_proc_path) if row_proc_path.exists() else pd.DataFrame()
    # Standardize proc_last4 and crm_last4 as padded strings
    if not uk_proc.empty:
        uk_proc['proc_last4'] = uk_proc['proc_last4'].apply(clean_last4)
    if not row_proc.empty:
        row_proc['proc_last4'] = row_proc['proc_last4'].apply(clean_last4)
    if not uk_crm.empty:
        uk_crm['crm_last4'] = uk_crm['crm_last4'].apply(clean_last4)
    if not row_crm.empty:
        row_crm['crm_last4'] = row_crm['crm_last4'].apply(clean_last4)
    uk_processors_lower = [p.lower() for p in ['safechargeuk', 'barclays', 'barclaycard']]
    desired_columns = [
        'crm_type', 'crm_date', 'crm_email', 'crm_firstname', 'crm_lastname', 'crm_tp', 'crm_last4', 'crm_currency',
        'crm_amount',
        'payment_method',
        'crm_processor_name',
        'regulation',
        'proc_date', 'proc_email', 'proc_tp', 'proc_firstname', 'proc_lastname', 'proc_last4', 'proc_currency',
        'proc_amount', 'proc_amount_crm_currency', 'proc_processor_name',
        'email_similarity_avg', 'last4_match', 'name_fallback_used', 'exact_match_used', 'match_status',
        'payment_status', 'warning', 'comment'
    ]
    if uk_crm.empty or uk_proc.empty:
        print(f"Skipping UK withdrawals matching: Missing combined files for {date_str}")
        uk_matches_df = pd.DataFrame()
    else:
        engine_uk_local = ReconciliationEngine(exchange_rate_map,
                                               {'enable_cross_processor': True, 'enable_warning_flag': True})
        uk_non_cancelled = uk_crm[uk_crm['crm_type'].str.lower() != 'withdrawal cancelled']

        # Prioritize CRM rows with valid crm_last4 for matching
        def has_valid_last4(row):
            last4 = str(row['crm_last4']).strip()
            return last4 not in ('', '0', '0000', 'nan') and last4.isdigit()

        uk_non_cancelled['has_last4'] = uk_non_cancelled.apply(has_valid_last4, axis=1)
        uk_non_cancelled = uk_non_cancelled.sort_values('has_last4', ascending=False).drop(columns=['has_last4'])
        matches_uk_local = engine_uk_local.match_withdrawals(uk_non_cancelled, uk_proc, add_unmatched_proc=True,
                                                             add_unmatched_crm=True)
        # Extract matched UK proc indices
        matched_uk_proc_ids_local = set()
        for m in matches_uk_local:
            if m['match_status'] == 1:
                matched_uk_proc_ids_local.update(m.get('matched_proc_indices', []))
        # Unmatched UK CRM from local matches
        uk_local_df = pd.DataFrame(matches_uk_local)
        unmatched_uk_crm_local_mask = (uk_local_df['match_status'] == 0) & uk_local_df['crm_date'].notna()
        unmatched_uk_crm_local = uk_local_df[unmatched_uk_crm_local_mask].copy()
        # Project back to CRM columns (approximate, assuming order)
        unmatched_uk_crm_local = uk_non_cancelled.iloc[unmatched_uk_crm_local_mask.values[:len(
            uk_non_cancelled)]].copy() if not unmatched_uk_crm_local.empty else pd.DataFrame()
        # Cross match unmatched UK CRM with ROW proc
        engine_uk_cross = ReconciliationEngine(exchange_rate_map,
                                               {'enable_cross_processor': True, 'enable_warning_flag': True})
        matches_uk_cross_full = engine_uk_cross.match_withdrawals(unmatched_uk_crm_local, row_proc,
                                                                  add_unmatched_proc=False, add_unmatched_crm=False)
        # Filter only matched from cross (exclude unmatched CRM from cross)
        matches_uk_cross = [m for m in matches_uk_cross_full if m['match_status'] == 1]
        # Extract matched ROW proc indices from UK cross
        matched_row_proc_ids_from_uk = set()
        for m in matches_uk_cross:
            matched_row_proc_ids_from_uk.update(m.get('matched_proc_indices', []))
        # Combine matches for UK
        all_matches_uk = matches_uk_local + matches_uk_cross
        uk_matches_df = pd.DataFrame(all_matches_uk)
        # Add payment_method
        uk_matches_df['payment_method'] = np.nan
        uk_matches_df['payment_method'] = uk_matches_df['payment_method'].astype('object')
        non_cancelled_count = len(uk_non_cancelled)
        if len(uk_matches_df) >= non_cancelled_count and not uk_non_cancelled.empty:
            uk_matches_df.iloc[:non_cancelled_count, uk_matches_df.columns.get_loc('payment_method')] = \
            uk_non_cancelled['payment_method'].values
        # Append cancellations
        cancelled_uk = engine_uk_local.make_cancelled_rows(uk_crm)
        if cancelled_uk:
            cancelled_df = pd.DataFrame(cancelled_uk)
            if not cancelled_df.empty:
                cancelled_df['payment_method'] = np.nan
                cancelled_df['payment_method'] = cancelled_df['payment_method'].astype('object')
                cancelled_crm_mask = uk_crm['crm_type'].str.lower() == 'withdrawal cancelled'
                cancelled_crm_df = uk_crm[cancelled_crm_mask].copy()
                if len(cancelled_df) == len(cancelled_crm_df):
                    cancelled_df['payment_method'] = cancelled_crm_df['payment_method'].values
                uk_matches_df = pd.concat([uk_matches_df, cancelled_df], ignore_index=True)
        # Fix regulation if missing
        if 'crm_index' in uk_matches_df.columns and 'regulation' not in uk_matches_df.columns:
            uk_matches_df = uk_matches_df.merge(uk_crm[['regulation']], left_on='crm_index', right_index=True,
                                                how='left')
    if row_crm.empty or row_proc.empty:
        print(f"Skipping ROW withdrawals matching: Missing combined files for {date_str}")
        row_matches_df = pd.DataFrame()
    else:
        # Available ROW proc excluding those matched to UK CRM
        available_row_proc = row_proc[~row_proc.index.isin(matched_row_proc_ids_from_uk)]
        engine_row_local = ReconciliationEngine(exchange_rate_map,
                                                {'enable_cross_processor': True, 'enable_warning_flag': True})
        row_non_cancelled = row_crm[row_crm['crm_type'].str.lower() != 'withdrawal cancelled']

        # Prioritize CRM rows with valid crm_last4 for matching
        def has_valid_last4(row):
            last4 = str(row['crm_last4']).strip()
            return last4 not in ('', '0', '0000', 'nan') and last4.isdigit()

        row_non_cancelled['has_last4'] = row_non_cancelled.apply(has_valid_last4, axis=1)
        row_non_cancelled = row_non_cancelled.sort_values('has_last4', ascending=False).drop(columns=['has_last4'])
        matches_row_local = engine_row_local.match_withdrawals(row_non_cancelled, available_row_proc,
                                                               add_unmatched_proc=True, add_unmatched_crm=True)
        # Extract matched ROW proc indices local
        matched_row_proc_ids_local = set()
        for m in matches_row_local:
            if m['match_status'] == 1:
                matched_row_proc_ids_local.update(m.get('matched_proc_indices', []))
        # Unmatched ROW CRM from local
        row_local_df = pd.DataFrame(matches_row_local)
        unmatched_row_crm_local_mask = (row_local_df['match_status'] == 0) & row_local_df['crm_date'].notna()
        unmatched_row_crm_local = row_local_df[unmatched_row_crm_local_mask].copy()
        unmatched_row_crm_local = row_non_cancelled.iloc[unmatched_row_crm_local_mask.values[:len(
            row_non_cancelled)]].copy() if not unmatched_row_crm_local.empty else pd.DataFrame()
        # Available UK proc excluding those matched to UK CRM local (note: matched_uk_proc_ids_local from uk_proc)
        available_uk_proc = uk_proc[~uk_proc.index.isin(matched_uk_proc_ids_local)]
        # Cross match unmatched ROW CRM with available UK proc
        engine_row_cross = ReconciliationEngine(exchange_rate_map,
                                                {'enable_cross_processor': True, 'enable_warning_flag': True})
        matches_row_cross_full = engine_row_cross.match_withdrawals(unmatched_row_crm_local, available_uk_proc,
                                                                    add_unmatched_proc=False,
                                                                    add_unmatched_crm=False)
        # Filter only matched from cross (exclude unmatched CRM from cross)
        matches_row_cross = [m for m in matches_row_cross_full if m['match_status'] == 1]
        # Extract matched UK proc indices from ROW cross
        matched_uk_proc_ids_from_row = set()
        for m in matches_row_cross:
            matched_uk_proc_ids_from_row.update(m.get('matched_proc_indices', []))
        # Combine matches for ROW
        all_matches_row = matches_row_local + matches_row_cross
        row_matches_df = pd.DataFrame(all_matches_row)
        # Add payment_method
        row_matches_df['payment_method'] = np.nan
        row_matches_df['payment_method'] = row_matches_df['payment_method'].astype('object')
        non_cancelled_count = len(row_non_cancelled)
        if len(row_matches_df) >= non_cancelled_count and not row_non_cancelled.empty:
            row_matches_df.iloc[:non_cancelled_count, row_matches_df.columns.get_loc('payment_method')] = \
            row_non_cancelled['payment_method'].values
        # Append cancellations
        cancelled_row = engine_row_local.make_cancelled_rows(row_crm)
        if cancelled_row:
            cancelled_df = pd.DataFrame(cancelled_row)
            if not cancelled_df.empty:
                cancelled_df['payment_method'] = np.nan
                cancelled_df['payment_method'] = cancelled_df['payment_method'].astype('object')
                cancelled_crm_mask = row_crm['crm_type'].str.lower() == 'withdrawal cancelled'
                cancelled_crm_df = row_crm[cancelled_crm_mask].copy()
                if len(cancelled_df) == len(cancelled_crm_df):
                    cancelled_df['payment_method'] = cancelled_crm_df['payment_method'].values
                row_matches_df = pd.concat([row_matches_df, cancelled_df], ignore_index=True)
        # Fix regulation if missing
        if 'crm_index' in row_matches_df.columns and 'regulation' not in row_matches_df.columns:
            row_matches_df = row_matches_df.merge(row_crm[['regulation']], left_on='crm_index', right_index=True,
                                                  how='left')
        row_matches_df = row_matches_df[~((row_matches_df['match_status'] == 0) &
                                          row_matches_df['crm_date'].isna() &
                                          row_matches_df['matched_proc_indices'].apply(lambda x: bool(
                                              set(x) & matched_row_proc_ids_from_uk if 'matched_row_proc_ids_from_uk' in locals() else set())))]
        # Keep filter to exclude any UK-specific processors from row unmatched proc
        unmatched_proc_mask = (row_matches_df['match_status'] == 0) & row_matches_df['crm_date'].isna()
        row_matches_df = row_matches_df[
            ~unmatched_proc_mask | ~row_matches_df['proc_processor_name'].str.lower().isin(uk_processors_lower)]
        row_matches_df = row_matches_df[[c for c in desired_columns if c in row_matches_df.columns]]
        row_matches_df['crm_type'] = ''
        row_matches_df.loc[row_matches_df['crm_email'].notna(), 'crm_type'] = 'Withdrawal'
        row_matches_df.loc[row_matches_df[
                               'comment'] == 'Withdrawal cancelled with no matching withdrawal found', 'crm_type'] = 'Withdrawal Cancelled'
        columns = list(row_matches_df.columns)
        columns.insert(0, columns.pop(columns.index('crm_type')))
        row_matches_df = row_matches_df[columns]
        # Clean proc_last4 before save
        row_matches_df['proc_last4'] = row_matches_df['proc_last4'].astype(str).str.replace('.0$', '', regex=True)
        # Save ROW withdrawals matching
        row_report_dir = row_dirs['lists_dir'] / date_str
        row_report_dir.mkdir(parents=True, exist_ok=True)
        row_path = row_report_dir / "row_withdrawals_matching.xlsx"
        row_matches_df.to_excel(row_path, index=False)
        print(f"ROW withdrawals matching report saved to {row_path}")
    # Finalize and save UK
    if not uk_matches_df.empty:
        unmatched_proc_mask_uk = (uk_matches_df['match_status'] == 0) & uk_matches_df['crm_date'].isna()
        cross_matched_uk_proc = uk_matches_df['matched_proc_indices'].apply(lambda x: bool(
            set(x) & (matched_uk_proc_ids_from_row if 'matched_uk_proc_ids_from_row' in locals() else set())))
        uk_matches_df = uk_matches_df[~(unmatched_proc_mask_uk & cross_matched_uk_proc)]
        uk_matches_df = uk_matches_df[[c for c in desired_columns if c in uk_matches_df.columns]]
        uk_matches_df['crm_type'] = ''
        uk_matches_df.loc[uk_matches_df['crm_email'].notna(), 'crm_type'] = 'Withdrawal'
        uk_matches_df.loc[uk_matches_df[
                              'comment'] == 'Withdrawal cancelled with no matching withdrawal found', 'crm_type'] = 'Withdrawal Cancelled'
        columns = list(uk_matches_df.columns)
        columns.insert(0, columns.pop(columns.index('crm_type')))
        uk_matches_df = uk_matches_df[columns]
        # Clean proc_last4 before save
        uk_matches_df['proc_last4'] = uk_matches_df['proc_last4'].astype(str).str.replace('.0$', '', regex=True)
        uk_report_dir = uk_dirs['lists_dir'] / date_str
        uk_report_dir.mkdir(parents=True, exist_ok=True)
        uk_path = uk_report_dir / "uk_withdrawals_matching.xlsx"
        uk_matches_df.to_excel(uk_path, index=False)
        print(f"UK withdrawals matching report saved to {uk_path}")

def run_cross_processor_matching(date_str: str, exchange_rate_map: dict) -> None:
    """PROC-driven cross-processor matching across regulations on unmatched rows after cross-reg."""
    row_dirs = setup_dirs_for_reg('row', create=False)
    uk_dirs = setup_dirs_for_reg('uk', create=False)
    row_file = row_dirs['lists_dir'] / date_str / "row_withdrawals_matching.xlsx"
    uk_file = uk_dirs['lists_dir'] / date_str / "uk_withdrawals_matching.xlsx"
    if not (row_file.exists() and uk_file.exists()):
        print("[Cross-processor] One or more matching files missing – skipping.")
        return
    row_df = pd.read_excel(row_file)
    uk_df = pd.read_excel(uk_file)
    for df in [row_df, uk_df]:
        list_cols = ['proc_date', 'proc_email', 'proc_tp', 'proc_firstname', 'proc_lastname', 'proc_last4',
                     'proc_currency', 'proc_amount', 'proc_amount_crm_currency']
        for col in list_cols:
            if col in df.columns:
                df[col] = df[col].apply(clean_field)
        if 'proc_last4' in df.columns:
            df['proc_last4'] = df['proc_last4'].apply(clean_last4)
        if 'crm_last4' in df.columns:
            df['crm_last4'] = df['crm_last4'].apply(clean_last4)
    # Extract unmatched CRM/PROC per reg
    unmatched_crm_row_mask = (row_df['match_status'] == 0) & row_df['crm_date'].notna()
    unmatched_crm_row = row_df[unmatched_crm_row_mask].copy()
    unmatched_proc_row_mask = (row_df['match_status'] == 0) & row_df['crm_date'].isna()
    unmatched_proc_row = row_df[unmatched_proc_row_mask].copy()
    unmatched_crm_uk_mask = (uk_df['match_status'] == 0) & uk_df['crm_date'].notna()
    unmatched_crm_uk = uk_df[unmatched_crm_uk_mask].copy()
    unmatched_proc_uk_mask = (uk_df['match_status'] == 0) & uk_df['crm_date'].isna()
    unmatched_proc_uk = uk_df[unmatched_proc_uk_mask].copy()
    if (unmatched_crm_row.empty and unmatched_crm_uk.empty) or (unmatched_proc_row.empty and unmatched_proc_uk.empty):
        print("[Cross-processor] No unmatched CRM or PROC rows – skipping.")
        return
    # Assign original indices and reg labels
    crm_pools = []
    proc_pools = []
    if not unmatched_crm_row.empty:
        unmatched_crm_row = unmatched_crm_row.assign(orig_reg='row', orig_crm_index=unmatched_crm_row.index)
        crm_pools.append(unmatched_crm_row)
    if not unmatched_crm_uk.empty:
        unmatched_crm_uk = unmatched_crm_uk.assign(orig_reg='uk', orig_crm_index=unmatched_crm_uk.index)
        crm_pools.append(unmatched_crm_uk)
    if not unmatched_proc_row.empty:
        unmatched_proc_row = unmatched_proc_row.assign(orig_reg='row', orig_proc_index=unmatched_proc_row.index)
        proc_pools.append(unmatched_proc_row)
    if not unmatched_proc_uk.empty:
        unmatched_proc_uk = unmatched_proc_uk.assign(orig_reg='uk', orig_proc_index=unmatched_proc_uk.index)
        proc_pools.append(unmatched_proc_uk)
    all_unmatched_crm = pd.concat(crm_pools, ignore_index=True) if crm_pools else pd.DataFrame()
    all_unmatched_proc = pd.concat(proc_pools, ignore_index=True) if proc_pools else pd.DataFrame()
    # Run global PROC-driven cross-processor matching
    engine = ReconciliationEngine(exchange_rate_map, {'log_level': logging.WARNING, 'enable_diagnostics': False})
    used_crm = set()
    used_proc = set()
    cross_matches = []
    engine._cross_processor_last_chance(all_unmatched_crm, all_unmatched_proc, used_crm, used_proc, cross_matches)
    if not cross_matches:
        print("[Cross-processor] No additional matches found.")
        return
    print(f"[Cross-processor] Found {len(cross_matches)} cross-processor matches.")
    # Collect removals and group matches by CRM reg and whether cross-reg or within
    to_remove_row = set()
    to_remove_uk = set()
    row_cross_matches = []  # ROW CRM + other-reg PROC
    uk_cross_matches = []   # UK CRM + other-reg PROC
    row_within_matches = [] # ROW CRM + ROW PROC
    uk_within_matches = []  # UK CRM + UK PROC
    for match in cross_matches:
        crm_idx = match['crm_row_index']
        crm_reg = all_unmatched_crm.at[crm_idx, 'orig_reg']
        crm_orig_idx = all_unmatched_crm.at[crm_idx, 'orig_crm_index']
        proc_idxs = match['matched_proc_indices']
        proc_reg = all_unmatched_proc.at[proc_idxs[0], 'orig_reg']
        proc_orig_idxs = {all_unmatched_proc.at[p, 'orig_proc_index'] for p in proc_idxs}
        # Update comment to reflect within or cross-reg
        fallback_comment = f"Cross-processor fallback match - {match.get('crm_processor_name', 'unknown')} matched {match.get('proc_processor_name', 'unknown')}"
        if crm_reg != proc_reg:
            match['comment'] = fallback_comment + " (cross-regulation)"
        else:
            match['comment'] = fallback_comment  # No extra for within
        match['cross_processor_fallback'] = True
        # Route
        if crm_reg == proc_reg:
            if crm_reg == 'row':
                row_within_matches.append(match)
            else:
                uk_within_matches.append(match)
        else:
            if crm_reg == 'row':
                row_cross_matches.append(match)
            else:
                uk_cross_matches.append(match)
        # Removals (always remove old unmatched rows)
        if crm_reg == 'row':
            to_remove_row.add(crm_orig_idx)
        else:
            to_remove_uk.add(crm_orig_idx)
        if proc_reg == 'row':
            to_remove_row.update(proc_orig_idxs)
        else:
            to_remove_uk.update(proc_orig_idxs)
    # Remove old unmatched from originals
    row_df = row_df.drop(list(to_remove_row), errors='ignore')
    uk_df = uk_df.drop(list(to_remove_uk), errors='ignore')
    row_df.to_excel(row_file, index=False)
    uk_df.to_excel(uk_file, index=False)
    print("[Cross-processor] Removed matched rows from original *_withdrawals_matching.xlsx files.")
    # Helper to append list of matches to a file (original or cross)
    def append_matches_to_file(file_path, matches_list, desired_order):
        if not matches_list:
            return
        df_new = pd.DataFrame(matches_list)
        df_new['crm_type'] = 'Withdrawal'
        df_new = df_new.reindex(columns=[c for c in desired_order if c in df_new.columns or c == 'crm_type'])
        if 'proc_last4' in df_new.columns:
            df_new['proc_last4'] = df_new['proc_last4'].astype(str).str.replace(r'\.0$', '', regex=True)
        if file_path.exists():
            df_existing = pd.read_excel(file_path)
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        else:
            df_combined = df_new
        df_combined.to_excel(file_path, index=False)
        print(f"[Cross-processor] Appended {len(matches_list)} matches → {file_path.name}")
    # Append within-reg to original matching files
    desired_order = [
        'crm_type', 'crm_date', 'crm_email', 'crm_firstname', 'crm_lastname', 'crm_tp', 'crm_last4',
        'crm_currency', 'crm_amount', 'payment_method', 'crm_processor_name', 'regulation',
        'proc_date', 'proc_email', 'proc_tp', 'proc_firstname', 'proc_lastname', 'proc_last4',
        'proc_currency', 'proc_amount', 'proc_amount_crm_currency', 'proc_processor_name',
        'email_similarity_avg', 'last4_match', 'name_fallback_used', 'exact_match_used',
        'match_status', 'payment_status', 'warning', 'comment', 'matched_proc_indices'
    ]
    append_matches_to_file(row_file, row_within_matches, desired_order)
    append_matches_to_file(uk_file, uk_within_matches, desired_order)
    # Append cross-reg to cross files
    row_cross_path = row_dirs['lists_dir'] / date_str / "row_cross_regulation.xlsx"
    uk_cross_path = uk_dirs['lists_dir'] / date_str / "uk_cross_regulation.xlsx"
    append_matches_to_file(row_cross_path, row_cross_matches, desired_order)
    append_matches_to_file(uk_cross_path, uk_cross_matches, desired_order)
    print(f"[Cross-processor] Finished: {len(row_within_matches)} within ROW, {len(uk_within_matches)} within UK, {len(row_cross_matches)} cross to ROW cross file, {len(uk_cross_matches)} cross to UK cross file.")