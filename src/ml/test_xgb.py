import pandas as pd
import joblib
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from src.config import BASE_DIR, TEST_MODEL_DIR

AMT_TOL = 0.1
CONF_THRESHOLD = 0.6
TOP_K = 5  # how many lowest-confidence rows to show

def enhanced_name_similarity(name1, name2):
    name1 = '' if pd.isna(name1) else str(name1)
    name2 = '' if pd.isna(name2) else str(name2)
    if not name1 or not name2:
        return 0.0
    vectorizer = TfidfVectorizer(analyzer='char', ngram_range=(2, 3))
    tfidf = vectorizer.fit_transform([name1, name2])
    return cosine_similarity(tfidf[0:1], tfidf[1:2])[0][0]

def make_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Handle proc_amount if list: sum abs or 0 if empty
    def handle_amount(val):
        if isinstance(val, list):
            if val:
                return sum(abs(x) for x in val if pd.notna(x))
            return 0.0
        return pd.to_numeric(val, errors='coerce')

    df['crm_amount'] = pd.to_numeric(df.get('crm_amount', 0), errors='coerce').fillna(0)
    df['proc_amount'] = df['proc_amount'].apply(handle_amount).fillna(0)

    # Difference & ratio (positive)
    df['amount_diff'] = (abs(df['crm_amount']) - abs(df['proc_amount'])).abs()
    df['amount_ratio'] = abs(df['proc_amount_crm_currency'].fillna(0)) / abs(df['crm_amount']).replace(0, 1)

    # Comment length
    df['comment_len'] = df.get('comment', '').fillna('').str.len()

    # Date gap (days)
    df['crm_date'] = pd.to_datetime(df.get('crm_date'), errors='coerce')
    df['proc_date'] = pd.to_datetime(df.get('proc_date'), errors='coerce')
    df['date_diff'] = (df['proc_date'] - df['crm_date']).dt.days.abs().fillna(0)

    # Comment but paid
    df['comment_but_paid'] = (
        df.get('comment','').notna() &
        (df['comment']!='') &
        (df['payment_status']==1)
    ).astype(int)

    # Cast bools to int
    for flag in [
        'last4_match','name_fallback_used','exact_match_used',
        'match_status','payment_status','logic_is_correct'
    ]:
        if flag in df:
            df[flag] = df[flag].fillna(0).astype(int)

    # Validation features
    df['currency_match'] = (df.get('crm_currency', '') == df.get('proc_currency', '')).astype(int)
    df['payment_valid'] = (abs(df['crm_amount']) - df['proc_amount_crm_currency']).abs() <= 0.1 * abs(df['crm_amount'])
    df['payment_valid'] = df['payment_valid'].fillna(False).astype(int)  # Handle NaN as 0
    df['last4_actual'] = (df['crm_last4'].fillna('') == df['proc_last4'].fillna('')).astype(int)
    df['last4_disagree'] = (df['last4_actual'] != df['last4_match']).astype(int)
    df['name_sim_first'] = df.apply(lambda row: enhanced_name_similarity(row.get('crm_firstname', ''), row.get('proc_firstname', '')), axis=1)
    df['name_sim_last'] = df.apply(lambda row: enhanced_name_similarity(row.get('crm_lastname', ''), row.get('proc_lastname', '')), axis=1)
    df['match_status_valid'] = ((df['last4_match'] == 1) | (df['email_similarity_avg'] > 0.5) | (df['name_sim_first'] > 0.5) | (df['name_sim_last'] > 0.5)).astype(int)

    # is_cancel: more flexible match for "Withdrawal cancelled with no matching withdrawal found"
    df['is_cancel'] = df['comment'].str.lower().fillna('').str.contains(r'withdrawal\s*cancelled\s*with\s*no\s*matching\s*withdrawal\s*found').astype(int)
    return df


def main():
    # 1) load your full pipeline
    pipe = joblib.load(BASE_DIR / "model" / "xgb_pipeline_v1.pkl")
    print("Loaded pipeline.")

    # 2) read test file
    df = pd.read_csv(TEST_MODEL_DIR / "training_dataset_2025-04-10.csv")
    print("Read", len(df), "rows.")

    # 3) recompute ALL features
    df = make_features(df)

    # 4) select exactly the features your pipeline expects
    numeric = ['email_similarity_avg','amount_diff','amount_ratio','comment_len','date_diff', 'name_sim_first', 'name_sim_last']
    bools   = ['last4_match','name_fallback_used','exact_match_used',
               'match_status','payment_status','comment_but_paid', 'currency_match',
               'payment_valid', 'last4_actual', 'last4_disagree', 'match_status_valid', 'is_cancel']
    cats    = ['crm_processor_name','crm_currency','proc_currency']
    feats   = [c for c in numeric + bools + cats if c in df.columns]

    X      = df[feats]
    # y_true only if labeled; else None
    if df['logic_is_correct'].notna().all() and df['logic_is_correct'].nunique() > 1:
        y_true = df['logic_is_correct'].astype(int)
    else:
        y_true = None
        print("Note: Data unlabeled (all logic_is_correct 0/NaN) - skipping true metrics.")

    # 5) model predicts + confidence
    y_pred = pipe.predict(X)
    probs  = pipe.predict_proba(X).max(axis=1)

    # Fill the logic_is_correct column with predictions
    df['logic_is_correct'] = y_pred

    # Save the updated file
    updated_path = TEST_MODEL_DIR / "predicted_training_dataset_2025-04-10.csv"
    df.to_csv(updated_path, index=False)
    print(f"Saved updated file with filled logic_is_correct to {updated_path}")

    # 6) data‐driven sanity checks
    actual_l4 = (df['crm_last4'].fillna('') == df['proc_last4'].fillna('')).astype(int)
    last4_bad = actual_l4 != df['last4_match']
    pay_bad   = (df['payment_status'] == 1) & (df['amount_diff'] > AMT_TOL)

    # 7) combine all flags
    if y_true is not None:
        disagree = (y_pred != y_true)
    else:
        disagree = pd.Series([False] * len(df))
    lowconf  = (probs < CONF_THRESHOLD)
    flag     = disagree | lowconf | last4_bad | pay_bad

    print(f"Flagged rows: {flag.sum()}",
          f"(disagree={disagree.sum()}, lowconf={lowconf.sum()},",
          f"last4_bad={last4_bad.sum()}, pay_bad={pay_bad.sum()})\n")

    # 8) detail them, but only the lowest‐confidence TOP_K
    if not flag.any():
        print("✅ No anomalies detected.")
    else:
        out = df.loc[flag].copy()
        out['model_pred']   = y_pred[flag]
        out['confidence']   = probs[flag]
        out['actual_last4'] = actual_l4[flag]

        # pick the TOP_K least confident
        low_conf = out.nsmallest(TOP_K, 'confidence')

        display = [
            'crm_email','proc_email','crm_last4','proc_last4',
            'last4_match','actual_last4','payment_status','amount_diff',
            'logic_is_correct','model_pred','confidence'
        ]
        display = [c for c in display if c in low_conf.columns]

        print(f"--- Lowest {TOP_K} confidence flagged rows ---")
        print(low_conf[display].reset_index(drop=True))

    # 9) overall metrics if labeled
    if y_true is not None:
        print("\nOverall classification:")
        print(classification_report(y_true, y_pred))
        print("Confusion matrix:\n", confusion_matrix(y_true, y_pred))

if __name__ == "__main__":
    main()