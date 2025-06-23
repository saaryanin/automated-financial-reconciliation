import pandas as pd
from src.config import DATA_DIR, BASE_DIR
import glob
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score
import joblib

# Point to checked_datasets directory
CHECKED_DATASETS_DIR = DATA_DIR / "training_dataset" / "checked_datasets"

# Find all CSV datasets (change pattern if you use underscore instead of dash!)
files = glob.glob(str(CHECKED_DATASETS_DIR / 'training_dataset_2025-*.csv'))
print("Files found:", files)
if not files:
    raise ValueError("No CSV files found. Check path and file pattern!")

# Read and concatenate
dfs = [pd.read_csv(file) for file in files]
df_all = pd.concat(dfs, ignore_index=True)

# Fix dtype for boolean-like columns
df_all['name_fallback_used'] = df_all['name_fallback_used'].astype(str).str.lower().map({'true': 1, 'false': 0}).fillna(0).astype(int)
df_all['exact_match_used'] = df_all['exact_match_used'].astype(str).str.lower().map({'true': 1, 'false': 0}).fillna(0).astype(int)
df_all['converted'] = df_all['converted'].astype(str).str.lower().map({'true': 1, 'false': 0}).fillna(0).astype(int)



# Features & label
features = [
    'email_similarity_avg', 'name_fallback_used', 'exact_match_used',
    'converted', 'combo_len'
]
label = 'match_status'

# Data
X = df_all[features]
y = df_all[label]

# Train/test split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train model
clf = xgb.XGBClassifier(use_label_encoder=False, eval_metric='logloss')
clf.fit(X_train, y_train)

# Evaluate
y_pred = clf.predict(X_test)
print("Accuracy:", accuracy_score(y_test, y_pred))
print(classification_report(y_test, y_pred))

# Save model
MODEL_DIR = BASE_DIR / "model"
MODEL_DIR.mkdir(parents=True, exist_ok=True)
joblib.dump(clf, MODEL_DIR / "xgb_model_v1.pkl")
