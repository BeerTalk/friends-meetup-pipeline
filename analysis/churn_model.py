""" Conduct geo and social features prediction on churn
input data = analytics.churn_features, which is built by dbt from the raw data in BigQuery
first, we are gonna train on only activity
second, we will train on activity w geo w social features

Model a: activity only
Model b: activity + geo + social
Model c: Model b without n_friends (to see if social features are important)
model d: gradient boosting on Model b features (to see if non-linear effects and interactions are important)
"""
import numpy as np
import pandas as pd
from google.cloud import bigquery
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import HistGradientBoostingClassifier

# load data
client = bigquery.Client()
df = client.query("select * from analytics.churn_features").to_dataframe()
print(f"Loaded {len(df):,} users, churn rate {df['churned'].mean():.1%}")

# feature prep
# log counts and distances 
for col in ["checkins_30_days", "days_active_30_days", "unique_venues",
            "unique_cells", "radius_km", "n_friends"]:
    df[f"log_{col}"] = np.log1p(df[col])

# true/falseto 1/0
df["met_friend"] = df["met_friend_first_30d"].astype(int)
y = df["churned"].astype(int)

ACTIVITY = ["log_checkins_30_days", "log_days_active_30_days"]
GEO_SOCIAL = ["log_unique_venues", "log_unique_cells", "log_radius_km", "weekend_ratio", "log_n_friends", "met_friend"]

# split train and test data - 80/20
X_train, X_test, y_train, y_test = train_test_split(
    df, y, test_size = 0.2, random_state = 42, stratify = y
)

#train and auc 
def fit_and_score(name: str, features: list[str]):
    model = make_pipeline(StandardScaler(), LogisticRegression(max_iter=1000))
    model.fit(X_train[features], y_train)
    p_churn = model.predict_proba(X_test[features])[:, 1]
    auc = roc_auc_score(y_test, p_churn)
    print(f"{name:<32} AUC = {auc:.3f}")
    return model

model_a = fit_and_score("A: activity only", ACTIVITY)
model_b = fit_and_score("B: activity + geo + social", ACTIVITY +GEO_SOCIAL)
model_c = fit_and_score("C: B without n_friends", ACTIVITY + [f for f in GEO_SOCIAL if f != "log_n_friends"])

# use coefficient to measure feature importantce 
coefs = pd.Series(model_b[-1].coef_[0], index=ACTIVITY + GEO_SOCIAL).sort_values()
print("\nModel B coefficients (scaled):")
print(coefs.round(3).to_string())


# gradient boost, check non-linear effects and interactions on their own
 
model_d = HistGradientBoostingClassifier(max_iter=300, learning_rate=0.05, random_state=42)
model_d.fit(X_train[ACTIVITY + GEO_SOCIAL], y_train)
p_churn_d = model_d.predict_proba(X_test[ACTIVITY + GEO_SOCIAL])[:, 1]
print(f"\n{'D: gradient boosting (B features)':<32} AUC = {roc_auc_score(y_test, p_churn_d):.3f}")
 