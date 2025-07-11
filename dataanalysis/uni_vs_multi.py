# Multivariate Outlier Detection and Categorical Impact Analysis

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest
from sklearn.decomposition import PCA
from scipy.stats import kruskal, f_oneway
import plotly.express as px

# Step 1: Generate Sample Event Dataset
np.random.seed(42)
n_cases = 300
data = pd.DataFrame({
    'case_id': np.random.choice([f"case_{i}" for i in range(100)], n_cases),
    'market': np.random.choice(['US', 'EU', 'ASIA'], n_cases),
    'case_category': np.random.choice(['A', 'B', 'C'], n_cases),
    'case_type': np.random.choice(['Type1', 'Type2'], n_cases),
    'business_unit': np.random.choice(['BU1', 'BU2', 'BU3'], n_cases),
    'time_taken': np.random.gamma(shape=2, scale=3, size=n_cases),
    'event_frequency': np.random.poisson(lam=3, size=n_cases)
})

# Inject a synthetic multivariate outlier that isn't an outlier in univariate sense
median_time = data['time_taken'].median()
median_freq = data['event_frequency'].median()
data.loc[0, 'time_taken'] = median_time
data.loc[0, 'event_frequency'] = median_freq

# Step 2: Aggregate by Case ID
case_summary = data.groupby('case_id').agg({
    'market': 'first',
    'case_category': 'first',
    'case_type': 'first',
    'business_unit': 'first',
    'time_taken': 'mean',
    'event_frequency': 'sum'
}).reset_index()

# Step 3: Statistical Tests for Variable Selection
categorical_cols = ['market', 'case_category', 'case_type', 'business_unit']

kruskal_results_time = [(col, kruskal(*[grp['time_taken'] for _, grp in case_summary.groupby(col)])[1])
                        for col in categorical_cols]
kruskal_results_freq = [(col, kruskal(*[grp['event_frequency'] for _, grp in case_summary.groupby(col)])[1])
                        for col in categorical_cols]

significant_vars_time = [col for col, p in kruskal_results_time if p < 0.05]
significant_vars_freq = [col for col, p in kruskal_results_freq if p < 0.05]

# Fallback to all if none are significant
default_vars_time = significant_vars_time if significant_vars_time else categorical_cols
default_vars_freq = significant_vars_freq if significant_vars_freq else categorical_cols

# Step 4: Outlier Detection Function
def detect_outliers(data, target_columns, categorical_columns):
    encoded = pd.get_dummies(data[categorical_columns], drop_first=True)
    features = pd.concat([data[target_columns], encoded], axis=1)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(features)

    iso = IsolationForest(contamination=0.05, random_state=42)
    outlier_flags = iso.fit_predict(X_scaled)
    pca = PCA(n_components=2)
    X_pca = pca.fit_transform(X_scaled)

    result_df = data.copy()
    result_df['is_outlier'] = (outlier_flags == -1)
    result_df['pc1'], result_df['pc2'] = X_pca[:, 0], X_pca[:, 1]
    return result_df

# Time Taken Outliers
time_outliers = detect_outliers(case_summary, ['time_taken'], default_vars_time)

# Event Frequency Outliers
freq_outliers = detect_outliers(case_summary, ['event_frequency'], default_vars_freq)

# Step 5: Univariate Outlier Detection for Comparison (fixed)
univariate_flags = pd.DataFrame({'case_id': case_summary['case_id']})

for col in categorical_cols:
    for metric in ['time_taken', 'event_frequency']:
        outlier_col = f"{metric}_outlier_{col}"
        univariate_flags[outlier_col] = False  # default

        for cat_value, group in case_summary.groupby(col):
            q1 = group[metric].quantile(0.25)
            q3 = group[metric].quantile(0.75)
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr

            mask = (case_summary[col] == cat_value) & (
                (case_summary[metric] < lower_bound) | (case_summary[metric] > upper_bound)
            )
            univariate_flags.loc[mask, outlier_col] = True
# Step 6: Visualization
plt.figure(figsize=(12, 6))
sns.boxplot(x='market', y='time_taken', data=case_summary)
plt.title("Univariate Boxplot: Time Taken by Market")
plt.tight_layout()
plt.savefig('univariate_time_market.png')
plt.close()

plt.figure(figsize=(12, 6))
sns.boxplot(x='case_category', y='event_frequency', data=case_summary)
plt.title("Univariate Boxplot: Frequency by Case Category")
plt.tight_layout()
plt.savefig('univariate_frequency_category.png')
plt.close()

fig_time = px.scatter(
    time_outliers, x='pc1', y='pc2', color='is_outlier',
    hover_data=['case_id', 'market', 'case_category', 'case_type', 'business_unit', 'time_taken'],
    title="Multivariate PCA of Time Taken Outlier Detection"
)
fig_time.write_html("time_outliers_pca.html")

fig_freq = px.scatter(
    freq_outliers, x='pc1', y='pc2', color='is_outlier',
    hover_data=['case_id', 'market', 'case_category', 'case_type', 'business_unit', 'event_frequency'],
    title="Multivariate PCA of Frequency Outlier Detection"
)
fig_freq.write_html("frequency_outliers_pca.html")

# Step 7: Export Samples
print("\nSample Time Taken Outliers:\n", time_outliers[time_outliers['is_outlier']].head())
print("\nSample Frequency Outliers:\n", freq_outliers[freq_outliers['is_outlier']].head())

# Save samples
time_outliers[time_outliers['is_outlier']].to_csv("time_outliers.csv", index=False)
freq_outliers[freq_outliers['is_outlier']].to_csv("frequency_outliers.csv", index=False)

# Step 8: Outlier Analysis by Market
outliers_by_market = {}
markets = case_summary['market'].unique()

for market in markets:
    market_data = case_summary[case_summary['market'] == market]
    time_outliers_market = detect_outliers(market_data, ['time_taken'], default_vars_time)
    freq_outliers_market = detect_outliers(market_data, ['event_frequency'], default_vars_freq)

    outliers_by_market[market] = {
        "time_outliers": time_outliers_market[time_outliers_market['is_outlier']],
        "freq_outliers": freq_outliers_market[freq_outliers_market['is_outlier']]
    }

# Step 9: Demonstrate power of multivariate analysis
example_case_id = data.loc[0, 'case_id']
example_case = time_outliers[time_outliers['case_id'] == example_case_id]
univ_flags = univariate_flags[univariate_flags['case_id'] == example_case_id]

print("\n--- Demonstration of Multivariate Outlier ---")
print("Case ID:", example_case_id)
print("Multivariate Outlier Detected:", example_case['is_outlier'].values[0])
print("Univariate Outlier Flags:")
print(univ_flags.drop(columns='case_id').T)
print("\n=> This shows how a case can look typical in each category but become an outlier when combined.")
