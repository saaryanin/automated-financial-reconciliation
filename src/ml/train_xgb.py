import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score
import joblib

from src.config import FALSE_TRAINING_DIR, TRUE_TRAINING_DIR, MODEL_DIR

# ─── Paths via config.py ───────────────────────────────────────────────────
SYN_PATH   = FALSE_TRAINING_DIR / "synthetic_candidates.csv"
CHECK_DIR  = TRUE_TRAINING_DIR
MODEL_PATH = MODEL_DIR / "xgb_model_v1.pkl"

print(f"Loading synthetic data from {SYN_PATH}")
dfs = []
if SYN_PATH.exists():
    dfs.append(pd.read_csv(SYN_PATH))
else:
    raise FileNotFoundError(f"Synthetic file not found: {SYN_PATH}")

print(f"Loading version-checked datasets from {CHECK_DIR}")
if CHECK_DIR.exists():
    for csv_file in CHECK_DIR.glob("*.csv"):
        print(f"  - {csv_file.name}")
        dfs.append(pd.read_csv(csv_file))
else:
    print(f"Warning: directory not found: {CHECK_DIR}")

# ─── Concatenate ────────────────────────────────────────────────────────────
df_all = pd.concat(dfs, ignore_index=True)
print(f"Total rows loaded: {len(df_all)}")

# ─── Feature engineering ────────────────────────────────────────────────────
# Cast boolean flags to ints
bool_cols = [
    'last4_match','name_fallback_used','exact_match_used',
    'match_status','payment_status','logic_is_correct'
]
# Fill any missing with 0, then cast to int
for col in bool_cols:
    if col in df_all.columns:
        df_all[col] = df_all[col].fillna(0).astype(int)

# ensure numeric
if 'crm_amount' in df_all.columns:
    df_all['crm_amount'] = pd.to_numeric(df_all['crm_amount'], errors='coerce')
if 'proc_amount' in df_all.columns:
    df_all['proc_amount'] = pd.to_numeric(df_all['proc_amount'], errors='coerce')

# Compute absolute amount difference safely
df_all['amount_diff'] = (df_all['crm_amount'].fillna(0)
                        - df_all['proc_amount'].fillna(0)).abs()


# Compute absolute amount difference
if 'crm_amount' in df_all and 'proc_amount' in df_all:
    df_all['amount_diff'] = (df_all['crm_amount'].fillna(0)
                            - df_all['proc_amount'].fillna(0)).abs()
else:
    df_all['amount_diff'] = 0

# ─── Prepare features & label ────────────────────────────────────────────────
features = [
    'email_similarity_avg','last4_match','name_fallback_used',
    'exact_match_used','match_status','payment_status','amount_diff'
]
# drop any missing
features = [f for f in features if f in df_all]
label = 'logic_is_correct'

X = df_all[features].fillna(0)
y = df_all[label].astype(int)

# ─── Train/test split ───────────────────────────────────────────────────────
print("Splitting data into train/test…")
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

# ─── Train XGBoost ──────────────────────────────────────────────────────────
print("Training XGBoost classifier…")
clf = xgb.XGBClassifier(use_label_encoder=False, eval_metric='logloss', verbosity=1)
clf.fit(X_train, y_train)

# ─── Evaluate ───────────────────────────────────────────────────────────────
print("Evaluating on test set…")
y_pred = clf.predict(X_test)
print(f"Accuracy: {accuracy_score(y_test, y_pred):.4f}")
print(classification_report(y_test, y_pred))

# ─── Save model ─────────────────────────────────────────────────────────────
print(f"Saving model to {MODEL_PATH}")
joblib.dump(clf, MODEL_PATH)
print("Done.")
