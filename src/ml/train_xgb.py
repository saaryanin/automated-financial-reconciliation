# src/ml/train_xgb.py

import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
import joblib
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from src.config import FALSE_TRAINING_DIR, TRUE_TRAINING_DIR, MODEL_DIR

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
    # ── 1) Load both synthetic “false” and all true‐label files ─────
    synth_path = FALSE_TRAINING_DIR / "synthetic_candidates.csv"
    if not synth_path.exists():
        raise FileNotFoundError(f"Missing synthetic file: {synth_path}")
    dfs = [pd.read_csv(synth_path)]

    print(f"Loading true datasets from {TRUE_TRAINING_DIR}")
    for f in TRUE_TRAINING_DIR.glob("*.csv"):
        print("  -", f.name)
        dfs.append(pd.read_csv(f))

    df_all = pd.concat(dfs, ignore_index=True)
    print(f"Total rows loaded: {len(df_all)}")

    # ── 2) Feature engineering ──────────────────────────────────────
    df_all = make_features(df_all)

    # ── 3) Define feature groups ──────────────────────────────────
    numeric_feats = [
        'email_similarity_avg','amount_diff','amount_ratio',
        'comment_len','date_diff', 'name_sim_first', 'name_sim_last'
    ]
    bool_feats = [
        'last4_match','name_fallback_used','exact_match_used',
        'match_status','payment_status','comment_but_paid', 'currency_match',
        'payment_valid', 'last4_actual', 'last4_disagree', 'match_status_valid', 'is_cancel'
    ]
    cat_feats = [
        'crm_processor_name','crm_currency','proc_currency'
    ]

    # keep only those that actually exist
    numeric_feats = [c for c in numeric_feats if c in df_all]
    bool_feats    = [c for c in bool_feats    if c in df_all]
    cat_feats     = [c for c in cat_feats     if c in df_all]

    all_feats = numeric_feats + bool_feats + cat_feats

    # ── 4) Build preprocessing + model pipeline ────────────────────
    preprocessor = ColumnTransformer([
        ("num",   StandardScaler(),      numeric_feats),
        ("bool",  "passthrough",         bool_feats),
        ("cat",   OneHotEncoder(handle_unknown="ignore"), cat_feats),
    ])

    pipeline = Pipeline([
        ("prep", preprocessor),
        ("clf",  xgb.XGBClassifier(eval_metric="logloss", verbosity=1))
    ])

    # ── 5) Train/test split ────────────────────────────────────────
    X = df_all[all_feats]
    y = df_all['logic_is_correct']
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    # ── 6) Fit & evaluate ──────────────────────────────────────────
    print("Training pipeline...")
    pipeline.fit(X_train, y_train)

    print("Evaluating on test set…")
    y_pred = pipeline.predict(X_test)
    print("Accuracy:", accuracy_score(y_test, y_pred))
    print(classification_report(y_test, y_pred))

    # ── 7) Save the full pipeline ──────────────────────────────────
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    out_path = MODEL_DIR / "xgb_pipeline_v1.pkl"
    joblib.dump(pipeline, out_path)
    print("Saved pipeline to", out_path)

if __name__ == "__main__":
    main()