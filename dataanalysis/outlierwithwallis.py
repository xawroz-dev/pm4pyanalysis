import pandas as pd
import numpy as np
from scipy.stats import kruskal
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import LabelEncoder, StandardScaler
import plotly.express as px
import seaborn as sns
import matplotlib.pyplot as plt

# -------------------------
# 1. Create Synthetic Dataset
# -------------------------
np.random.seed(42)
n = 1000
data = pd.DataFrame({
    'category_id': np.random.choice(['A', 'B', 'C'], size=n),
    'market_id': np.random.choice(['X', 'Y', 'Z'], size=n),
    'segment': np.random.choice(['Retail', 'Wholesale'], size=n),
    'event_time': np.random.normal(loc=30, scale=10, size=n),
    'frequency': np.random.poisson(lam=5, size=n),
    'case_id': np.arange(n)
})

# Introduce some outliers
data.loc[np.random.choice(n, 10), 'event_time'] *= 3
data.loc[np.random.choice(n, 10), 'frequency'] *= 4

# -------------------------
# 2. Kruskal-Wallis Dependency Test
# -------------------------
def kruskal_dependency(df, cat_col, num_col):
    print(f"\nH0: '{cat_col}' and '{num_col}' are independent.")
    print(f"H1: '{cat_col}' and '{num_col}' are dependent.")
    groups = [group[num_col].values for _, group in df.groupby(cat_col)]
    stat, p = kruskal(*groups)
    print(f"Kruskal-Wallis Test between '{cat_col}' and '{num_col}':")
    print(f"Statistic = {stat:.2f}, p-value = {p:.4f}")
    if p < 0.05:
        print(f"=> Reject H0: '{cat_col}' and '{num_col}' are dependent.")
    else:
        print(f"=> Fail to reject H0: '{cat_col}' and '{num_col}' are independent.")
    return p

dependency_results = []
for cat_col in ['category_id', 'market_id', 'segment']:
    for num_col in ['event_time', 'frequency']:
        p_value = kruskal_dependency(data, cat_col, num_col)
        dependency_results.append((cat_col, num_col, p_value))

# -------------------------
# 3. Global Univariate Outlier Detection
# -------------------------
iso_global = IsolationForest(contamination=0.02, random_state=42)
data['outlier_global_event_time'] = iso_global.fit_predict(data[['event_time']])
data['outlier_global_event_time'] = data['outlier_global_event_time'].map({1: 0, -1: 1})

plt.figure(figsize=(10, 6))
sns.boxplot(x=data['event_time'])
plt.title('Global Univariate Outlier Detection (event_time)')
plt.show()

# -------------------------
# 4. Filtered Univariate Outlier Detection (market_id == 'X')
# -------------------------
market_x = data[data['market_id'] == 'X'].copy()
iso_market = IsolationForest(contamination=0.02, random_state=42)
market_x['outlier_event_time'] = iso_market.fit_predict(market_x[['event_time']])
market_x['outlier_event_time'] = market_x['outlier_event_time'].map({1: 0, -1: 1})

plt.figure(figsize=(10, 6))
sns.boxplot(x=market_x['event_time'])
plt.title('Univariate Outlier Detection (event_time) for market_id = X')
plt.show()

# -------------------------
# 5. Multivariate Outlier Detection
# -------------------------
df_encoded = data.copy()
le_dict = {}
for col in ['category_id', 'market_id', 'segment']:
    le = LabelEncoder()
    df_encoded[col] = le.fit_transform(df_encoded[col])
    le_dict[col] = le

features = ['category_id', 'market_id', 'segment', 'event_time', 'frequency']
scaler = StandardScaler()
X_scaled = scaler.fit_transform(df_encoded[features])

iso_mv = IsolationForest(contamination=0.03, random_state=42)
df_encoded['outlier_multivariate'] = iso_mv.fit_predict(X_scaled)
df_encoded['outlier_multivariate'] = df_encoded['outlier_multivariate'].map({1: 0, -1: 1})

# -------------------------
# 6. Visualization with Hover for Multivariate
# -------------------------
fig = px.scatter(
    df_encoded, x='event_time', y='frequency',
    color='outlier_multivariate',
    hover_data=['category_id', 'market_id', 'segment', 'case_id'],
    title='Multivariate Outlier Detection (Hover to See Details)'
)
fig.show()

# -------------------------
# 7. Why Isolation Forest?
# -------------------------
why_iforest = '''
Why Isolation Forest is Preferred:

1. Unlike distance-based models (e.g., Mahalanobis, DBSCAN), Isolation Forest does not assume numeric distances between encoded categories.
2. It isolates points by randomly selecting features and thresholds, which works well with label-encoded categories.
3. It is robust to high dimensionality and scales linearly with data size.
4. Mahalanobis or k-NN may fail when categories are encoded numerically, because numeric codes imply order and magnitude that don’t exist.
5. Isolation Forest avoids this by using data partitioning rather than distance.
'''
print(why_iforest)
