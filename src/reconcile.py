import numpy as np
from difflib import SequenceMatcher
from itertools import combinations
from concurrent.futures import ThreadPoolExecutor


def email_similarity(e1, e2):
    e1 = str(e1).split('@')[0]
    e2 = str(e2).split('@')[0]
    return SequenceMatcher(None, e1, e2).ratio()


def convert_amount(amount, from_currency, to_currency, rate_map):
    if from_currency == to_currency:
        return amount, 1.0
    rate = rate_map.get((from_currency, to_currency))
    if rate:
        return amount * rate, rate
    return None, None


def match_withdrawals_with_conversion(crm_df, processor_df, exchange_rate_map, max_combo=6, tolerance=0.01):
    used_proc_indices = set()
    used_crm_indices = set()
    matches = []

    def convert_proc_row_amount(row, target_currency):
        from_curr = row['proc_currency']
        amount = row['proc_total_amount']
        return convert_amount(amount, from_curr, target_currency, exchange_rate_map)

    def match_crm_row(crm_row):
        crm_email = crm_row['crm_email']
        crm_amount = crm_row['crm_amount']
        crm_last4 = crm_row['crm_last4']
        crm_currency = crm_row['crm_currency']

        candidate_df = processor_df[processor_df['proc_last4_digits'] == crm_last4]
        candidate_df = candidate_df[~candidate_df.index.isin(used_proc_indices)]

        best_combo = None
        best_score = 0

        for combo_len in range(1, max_combo + 1):
            for proc_indices in combinations(candidate_df.index, combo_len):
                combo_rows = processor_df.loc[list(proc_indices)]
                email_scores = [email_similarity(crm_email, r['proc_emails']) for _, r in combo_rows.iterrows()]
                avg_email_score = np.mean(email_scores)

                if avg_email_score < 0.75:
                    continue

                converted_amounts = []
                rates_used = []
                for _, r in combo_rows.iterrows():
                    converted, rate = convert_proc_row_amount(r, crm_currency)
                    if converted is None:
                        break
                    converted_amounts.append(converted)
                    rates_used.append(rate)

                if len(converted_amounts) != combo_len:
                    continue

                total_converted = sum(converted_amounts)

                if abs(total_converted - crm_amount) <= tolerance * crm_amount:
                    if avg_email_score > best_score:
                        best_score = avg_email_score
                        best_combo = {
                            "combo_rows": combo_rows.copy(),
                            "indices": proc_indices,
                            "converted_amounts": converted_amounts,
                            "rates_used": rates_used,
                            "email_scores": email_scores,
                            "total_converted": total_converted,
                            "combo_len": combo_len
                        }

        if best_combo:
            combo = best_combo
            any_conversion = any(crm_currency != row['proc_currency'] for _, row in combo["combo_rows"].iterrows())
            matches.append({
                'crm_date': crm_row['crm_date'],
                'crm_email': crm_row['crm_email'],
                'crm_firstname': crm_row['crm_firstname'],
                'crm_lastname': crm_row['crm_lastname'],
                'crm_last4': crm_row['crm_last4'],
                'crm_currency': crm_currency,
                'crm_amount': crm_amount,
                'crm_processor_name': crm_row['crm_processor_name'],
                'proc_dates': list(combo["combo_rows"]['proc_date']),
                'proc_emails': list(combo["combo_rows"]['proc_emails']),
                'proc_last4_digits': list(combo["combo_rows"]['proc_last4_digits']),
                'proc_currencies': list(combo["combo_rows"]['proc_currency']),
                'proc_total_amounts': list(combo["combo_rows"]['proc_total_amount']),
                'converted_amount_total': round(combo["total_converted"], 4),
                'exchange_rates': combo["rates_used"],
                'email_similarity_avg': round(np.mean(combo["email_scores"]), 4),
                'last4_match': True,
                'converted': any_conversion,
                'combo_len': combo["combo_len"],
                'label': 1
            })
            used_proc_indices.update(combo["indices"])
            used_crm_indices.add(crm_row.name)
        else:
            matches.append({
                'crm_date': crm_row['crm_date'],
                'crm_email': crm_row['crm_email'],
                'crm_firstname': crm_row['crm_firstname'],
                'crm_lastname': crm_row['crm_lastname'],
                'crm_last4': crm_row['crm_last4'],
                'crm_currency': crm_currency,
                'crm_amount': crm_amount,
                'crm_processor_name': crm_row['crm_processor_name'],
                'proc_dates': [],
                'proc_emails': [],
                'proc_last4_digits': [],
                'proc_currencies': [],
                'proc_total_amounts': [],
                'converted_amount_total': '',
                'exchange_rates': [],
                'email_similarity_avg': '',
                'last4_match': False,
                'converted': False,
                'combo_len': 0,
                'label': 0
            })

    with ThreadPoolExecutor() as executor:
        executor.map(match_crm_row, [row for _, row in crm_df.iterrows()])

    unmatched_proc_df = processor_df.drop(index=used_proc_indices)
    for _, proc_row in unmatched_proc_df.iterrows():
        matches.append({
            'crm_date': '', 'crm_email': '', 'crm_firstname': '', 'crm_lastname': '', 'crm_last4': '',
            'crm_currency': '', 'crm_amount': '', 'crm_processor_name': '',
            'proc_dates': [proc_row['proc_date']],
            'proc_emails': [proc_row['proc_emails']],
            'proc_last4_digits': [proc_row['proc_last4_digits']],
            'proc_currencies': [proc_row['proc_currency']],
            'proc_total_amounts': [proc_row['proc_total_amount']],
            'converted_amount_total': '',
            'exchange_rates': '',
            'email_similarity_avg': '',
            'last4_match': '',
            'converted': False,
            'combo_len': 1,
            'label': 0
        })

    return matches
