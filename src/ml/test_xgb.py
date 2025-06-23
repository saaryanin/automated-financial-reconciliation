import pandas as pd
import joblib
from sklearn.metrics import classification_report, confusion_matrix

# Load the trained model
clf = joblib.load('../../model/xgb_model_v1.pkl')

# Load a new or hold-out test dataset (or use X_test from before)
test_file = '../../data/training_dataset/checked_datasets/training_dataset_2025_01_11.xlsx'
test_df = pd.read_excel(test_file)

features = [
    'email_similarity_avg', 'name_fallback_used', 'exact_match_used',
    'converted', 'combo_len'
]
X_test = test_df[features]
y_true = test_df['match_status']

# Predict
y_pred = clf.predict(X_test)

# Compare

print(classification_report(y_true, y_pred))
print(confusion_matrix(y_true, y_pred))
