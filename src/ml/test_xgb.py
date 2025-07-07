import pandas as pd
import joblib
from src.config import BASE_DIR,TEST_MODEL_DIR
from sklearn.metrics import classification_report, confusion_matrix

# 1) Load your trained model
MODEL_PATH = BASE_DIR / "model" / "xgb_model_v1.pkl"
clf = joblib.load(MODEL_PATH)

# 2) Pick one of your checked datasets
test_file = TEST_MODEL_DIR / "training_dataset_2025-03-20.csv"
df = pd.read_csv(test_file)


# --- 3) Feature engineering (must mirror training) ---
# Convert amount columns to numeric
for col in ['crm_amount', 'proc_amount']:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
# Compute absolute difference
if 'crm_amount' in df.columns and 'proc_amount' in df.columns:
    df['amount_diff'] = (df['crm_amount'] - df['proc_amount']).abs()
else:
    df['amount_diff'] = 0
# Cast boolean flags
for flag in ['last4_match', 'name_fallback_used', 'exact_match_used',
             'match_status', 'payment_status', 'logic_is_correct']:
    if flag in df.columns:
        df[flag] = df[flag].fillna(0).astype(int)

# --- 4) Prepare feature matrix and labels ---
features = [
    'email_similarity_avg',
    'last4_match',
    'name_fallback_used',
    'exact_match_used',
    'match_status',
    'payment_status',
    'amount_diff'
]
# Keep only present features
features = [f for f in features if f in df.columns]
X_test = df[features].fillna(0)
y_true = df['logic_is_correct'].astype(int)

# --- 5) Predict and get probabilities ---
y_pred = clf.predict(X_test)
probs  = clf.predict_proba(X_test)

# --- 6) Identify anomalies ---
threshold     = 0.6
disagree_mask = (y_pred != y_true)
lowconf_mask  = (probs.max(axis=1) < threshold)
flag_mask     = disagree_mask | lowconf_mask

# --- 7) Print summary ---
print(f"Total rows flagged: {flag_mask.sum()}")
print(f" - Disagreements: {disagree_mask.sum()}")
print(f" - Low confidence (<{threshold}): {lowconf_mask.sum()}")

# --- 8) Detailed anomalies ---
if not flag_mask.any():
    print("✅ No anomalies detected.")
else:
    flagged = df.loc[flag_mask].copy()
    flagged['model_pred']  = y_pred[flag_mask]
    flagged['confidence']  = probs.max(axis=1)[flag_mask]

    print("\nFull flagged row data:")
    for idx, row in flagged.iterrows():
        print(f"Row {idx}: {row.to_dict()}")

    display_cols = ['crm_email', 'proc_email', 'comment'] + features + ['logic_is_correct', 'model_pred', 'confidence']
    display_cols = [c for c in display_cols if c in flagged.columns]
    print("\nAnomalous rows (selected cols):")
    print(flagged[display_cols].reset_index(drop=True))

# --- 9) Overall classification on this file ---
print("\nOverall performance:")
print(classification_report(y_true, y_pred))
print("Confusion matrix:", confusion_matrix(y_true, y_pred))




