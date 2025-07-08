import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import pearsonr, spearmanr, kendalltau

# Generate synthetic data (if not already available)
np.random.seed(42)
data = pd.DataFrame({
    'event_time': np.random.normal(loc=30, scale=10, size=1000),
    'frequency': np.random.poisson(lam=5, size=1000)
})

# Introduce some noise and correlation artificially if needed
data.loc[np.random.choice(data.index, 10), 'event_time'] *= 3
data.loc[np.random.choice(data.index, 10), 'frequency'] *= 4

# --- Pearson Correlation (Linear Relationship) ---
pearson_corr, pearson_p = pearsonr(data['event_time'], data['frequency'])
print(f"Pearson Correlation: {pearson_corr:.2f}, p-value: {pearson_p:.4f}")
if pearson_p < 0.05:
    print("=> Statistically significant linear correlation.")
else:
    print("=> No statistically significant linear correlation.")

# --- Spearman Correlation (Monotonic Relationship) ---
spearman_corr, spearman_p = spearmanr(data['event_time'], data['frequency'])
print(f"Spearman Correlation: {spearman_corr:.2f}, p-value: {spearman_p:.4f}")
if spearman_p < 0.05:
    print("=> Statistically significant monotonic correlation.")
else:
    print("=> No statistically significant monotonic correlation.")

# --- Kendall Tau Correlation (Ordinal Relationship) ---
kendall_corr, kendall_p = kendalltau(data['event_time'], data['frequency'])
print(f"Kendall Tau Correlation: {kendall_corr:.2f}, p-value: {kendall_p:.4f}")
if kendall_p < 0.05:
    print("=> Statistically significant ordinal correlation.")
else:
    print("=> No statistically significant ordinal correlation.")

# --- Visualization: Scatter with Regression Line ---
sns.set(style="whitegrid")
sns.jointplot(data=data, x='event_time', y='frequency', kind='reg', height=6)
plt.suptitle("Scatter Plot with Regression Line", y=1.02)
plt.show()

# --- Correlation Heatmap ---
corr_matrix = data[['event_time', 'frequency']].corr()
plt.figure(figsize=(5, 4))
sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', fmt=".2f")
plt.title("Correlation Heatmap")
plt.tight_layout()
plt.show()
