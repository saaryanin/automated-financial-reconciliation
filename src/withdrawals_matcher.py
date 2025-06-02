import numpy as np
from difflib import SequenceMatcher
from itertools import combinations
from concurrent.futures import ThreadPoolExecutor

def email_similarity(e1, e2):
    e1 = str(e1).split('@')[0]
    e2 = str(e2).split('@')[0]
    return SequenceMatcher(None, e1, e2).ratio()

def match_withdrawals(crm_df, processor_df):
    used_proc_indices = set()
    used_crm_indices = set()
    matches = []

    def match_crm_row(crm_row):
        crm_email = crm_row['crm_email']
        crm_amount = crm_row['crm_amount']
        crm_last4 = crm_row['crm_last4']
        best_score = 0
        best_combo = None

        for combo_len in [2, 1]:  # Check 2-combos first
            for proc_indices in combinations(processor_df.index, combo_len):
                if any(i in used_proc_indices for i in proc_indices):
                    continue

                combo_rows = processor_df.loc[list(proc_indices)]
                total = combo_rows['proc_total_amount'].sum()
                if abs(total - crm_amount) > 0.1 * crm_amount:
                    continue

                email_scores = [email_similarity(crm_email, r['proc_emails']) for _, r in combo_rows.iterrows()]
                avg_email_score = np.mean(email_scores)

                if avg_email_score >= 0.85 and avg_email_score > best_score:
                    best_score = avg_email_score
                    best_combo = (combo_rows.copy(), proc_indices, combo_len)

        results = []
        if best_combo:
            combo_rows, indices, combo_len = best_combo
            for _, proc_row in combo_rows.iterrows():
                results.append({
                    'crm_date': crm_row['crm_date'],
                    'crm_email': crm_row['crm_email'],
                    'crm_firstname': crm_row['crm_firstname'],
                    'crm_lastname': crm_row['crm_lastname'],
                    'crm_last4': crm_row['crm_last4'],
                    'crm_currency': crm_row['crm_currency'],
                    'crm_amount': crm_row['crm_amount'],
                    'crm_processor_name': crm_row['crm_processor_name'],
                    'proc_date': proc_row['proc_date'],
                    'actual_processor': proc_row['actual_processor'],
                    'proc_emails': proc_row['proc_emails'],
                    'firstname': '',
                    'lastname': '',
                    'proc_last4_digits': proc_row['proc_last4_digits'],
                    'proc_currency': proc_row['proc_currency'],
                    'proc_total_amount': proc_row['proc_total_amount'],
                    'date_match': crm_row['crm_date'] == proc_row['proc_date'],
                    'email_similarity': round(best_score, 3),
                    'name_similarity': 0.0,
                    'last4_match': crm_row['crm_last4'] == proc_row['proc_last4_digits'],
                    'currency_match': crm_row['crm_currency'] == proc_row['proc_currency'],
                    'amount_diff': abs(crm_row['crm_amount'] - combo_rows['proc_total_amount'].sum()),
                    'amount_ratio': combo_rows['proc_total_amount'].sum() / crm_row['crm_amount'] if crm_row['crm_amount'] else 0,
                    'converted': False,
                    'combo_len': combo_len,
                    'label': ''
                })
            used_proc_indices.update(indices)
            used_crm_indices.add(crm_row.name)
        return results

    with ThreadPoolExecutor() as executor:
        results = list(executor.map(match_crm_row, [row for _, row in crm_df.iterrows()]))
        for res in results:
            matches.extend(res)

    unmatched_proc_df = processor_df.drop(index=used_proc_indices)
    for _, proc_row in unmatched_proc_df.iterrows():
        matches.append({
            'crm_date': '', 'crm_email': '', 'crm_firstname': '', 'crm_lastname': '', 'crm_last4': '',
            'crm_currency': '', 'crm_amount': '', 'crm_processor_name': '',
            'proc_date': proc_row['proc_date'],
            'actual_processor': proc_row['actual_processor'],
            'proc_emails': proc_row['proc_emails'],
            'firstname': '', 'lastname': '',
            'proc_last4_digits': proc_row['proc_last4_digits'],
            'proc_currency': proc_row['proc_currency'],
            'proc_total_amount': proc_row['proc_total_amount'],
            'date_match': '', 'email_similarity': '', 'name_similarity': '', 'last4_match': '', 'currency_match': '',
            'amount_diff': '', 'amount_ratio': '', 'converted': False,
            'combo_len': 1, 'label': 0
        })

    return matches
