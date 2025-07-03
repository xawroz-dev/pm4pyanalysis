import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.covariance import MinCovDet  # For robust Mahalanobis Distance
from scipy.spatial.distance import mahalanobis
import matplotlib.pyplot as plt
import seaborn as sns

# --- 1. Generate Synthetic Data with Numerical and Categorical Features ---
np.random.seed(42)
num_samples = 500
num_features = 3  # Numerical features

# Generate numerical data for 'normal' points
data_numerical = np.random.normal(loc=[0, 0, 0], scale=[1, 1, 1], size=(num_samples, num_features))

# Generate categorical data
categories_A = np.random.choice(['Red', 'Blue', 'Green', 'Yellow'], size=num_samples, p=[0.4, 0.3, 0.2, 0.1])
categories_B = np.random.choice(['Small', 'Medium', 'Large'], size=num_samples, p=[0.5, 0.3, 0.2])
categories_C = np.random.choice(['Type1', 'Type2'], size=num_samples, p=[0.7, 0.3])

# Create a DataFrame
df = pd.DataFrame(data_numerical, columns=[f'num_feat_{i + 1}' for i in range(num_features)])
df['category_A'] = categories_A
df['category_B'] = categories_B
df['category_C'] = categories_C

print("Original Data Head:")
print(df.head())
print("\nOriginal Data Info:")
df.info()

# --- Introduce Outliers ---
num_outliers = 20

# Numerical outliers (far from the mean)
outliers_numerical = np.random.normal(loc=[10, 10, 10], scale=[1, 1, 1], size=(num_outliers, num_features))

# Categorical outliers (less common categories for the 'outlier' group)
# For category_A, 'Purple' is a new, very rare category that only appears in outliers
outliers_category_A = np.random.choice(['Yellow', 'Green', 'Purple'], size=num_outliers,
                                       p=[0.4, 0.4, 0.2])  # More yellow/green, introducing 'Purple'
outliers_category_B = np.random.choice(['Large'], size=num_outliers)  # Mostly large
outliers_category_C = np.random.choice(['Type2'], size=num_outliers)  # Mostly Type2

# Combine numerical and categorical outliers
outliers_df = pd.DataFrame(outliers_numerical, columns=[f'num_feat_{i + 1}' for i in range(num_features)])
outliers_df['category_A'] = outliers_category_A
outliers_df['category_B'] = outliers_category_B
outliers_df['category_C'] = outliers_category_C

# Concatenate outliers to the original DataFrame
df_with_outliers = pd.concat([df, outliers_df], ignore_index=True)
df_with_outliers['is_outlier_true'] = [0] * num_samples + [1] * num_outliers  # True labels for evaluation

print(f"\nData with {num_outliers} outliers added. Total samples: {len(df_with_outliers)}")
print("\nData with Outliers Head (showing some outliers):")
print(df_with_outliers.tail())

# --- 2. Data Preprocessing for Isolation Forest ---
# For Isolation Forest, Label Encoding is generally a good choice for categorical features
# as it avoids high dimensionality while allowing the tree structure to split effectively.

df_if = df_with_outliers.copy()
label_encoders = {}
for col in ['category_A', 'category_B', 'category_C']:
    # Ensure all categories from both normal and outlier data are fitted
    # This prevents errors if an outlier introduces a new category not seen in the 'normal' set
    df_if[col] = df_if[col].astype('category')  # Convert to pandas category dtype
    le = LabelEncoder()
    le.fit(df_if[col])  # Fit on all unique values across the combined dataset
    df_if[col] = le.transform(df_if[col])
    label_encoders[col] = le

print("\nData after Label Encoding for Isolation Forest Head:")
print(df_if.head())

# Scale numerical features (optional but good practice for numerical stability, though IF is less sensitive)
scaler_if = StandardScaler()
df_if_scaled = df_if.copy()
df_if_scaled[[f'num_feat_{i + 1}' for i in range(num_features)]] = scaler_if.fit_transform(
    df_if_scaled[[f'num_feat_{i + 1}' for i in range(num_features)]])

# --- 3. Apply Isolation Forest ---
# 'contamination' is the proportion of outliers in the data set.
# This is a hyperparameter that needs to be estimated or tuned.
# For this example, we know the true number of outliers.
if_model = IsolationForest(random_state=42, contamination=num_outliers / len(df_with_outliers))
df_if_scaled['if_scores'] = if_model.fit_predict(df_if_scaled.drop('is_outlier_true', axis=1))

# Convert scores to outlier labels: -1 for outlier, 1 for inlier
df_if_scaled['if_outlier_predicted'] = df_if_scaled['if_scores'].apply(lambda x: 1 if x == -1 else 0)

print("\nIsolation Forest Outlier Prediction Summary:")
print(df_if_scaled['if_outlier_predicted'].value_counts())
print(f"True Outliers in dataset: {df_if_scaled['is_outlier_true'].sum()}")

# Evaluate IF performance (simple comparison)
if_true_positives = \
df_if_scaled[(df_if_scaled['is_outlier_true'] == 1) & (df_if_scaled['if_outlier_predicted'] == 1)].shape[0]
if_false_positives = \
df_if_scaled[(df_if_scaled['is_outlier_true'] == 0) & (df_if_scaled['if_outlier_predicted'] == 1)].shape[0]
if_false_negatives = \
df_if_scaled[(df_if_scaled['is_outlier_true'] == 1) & (df_if_scaled['if_outlier_predicted'] == 0)].shape[0]

print(
    f"Isolation Forest - True Positives: {if_true_positives}, False Positives: {if_false_positives}, False Negatives: {if_false_negatives}")

# --- Analyze Categorical Features of Predicted Outliers (Isolation Forest) ---
# Get the original data for points predicted as outliers by Isolation Forest
if_predicted_outlier_indices = df_if_scaled[df_if_scaled['if_outlier_predicted'] == 1].index
if_predicted_outliers_original = df_with_outliers.loc[if_predicted_outlier_indices].copy()

print("\n--- Categorical Feature Analysis for Isolation Forest Predicted Outliers ---")
print("Top categories among Isolation Forest predicted outliers:")
for col in ['category_A', 'category_B', 'category_C']:
    print(f"\n{col}:")
    # Compare distribution in predicted outliers vs. overall data
    outlier_counts = if_predicted_outliers_original[col].value_counts(normalize=True)
    overall_counts = df_with_outliers[col].value_counts(normalize=True)

    analysis_df = pd.DataFrame({
        'Outlier_Distribution': outlier_counts,
        'Overall_Distribution': overall_counts
    }).fillna(0).sort_values(by='Outlier_Distribution', ascending=False)
    print(analysis_df)

    # Visualization for categorical features of outliers
    plt.figure(figsize=(8, 5))
    sns.countplot(data=if_predicted_outliers_original, x=col,
                  order=if_predicted_outliers_original[col].value_counts().index, palette='viridis')
    plt.title(f'Distribution of {col} for Isolation Forest Predicted Outliers')
    plt.xlabel(col)
    plt.ylabel('Count')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.show()

# --- 4. Data Preprocessing for Mahalanobis Distance ---
# For Mahalanobis Distance, One-Hot Encoding can increase dimensionality and violate normality.
# Frequency Encoding is a more robust option for nominal categories.
# We'll use Frequency Encoding for categorical features and scale numerical features.

df_md = df_with_outliers.copy()

# Apply Frequency Encoding to categorical columns
for col in ['category_A', 'category_B', 'category_C']:
    freq_map = df_md[col].value_counts(normalize=True).to_dict()
    df_md[col + '_freq'] = df_md[col].map(freq_map)
    df_md = df_md.drop(columns=[col])  # Drop original categorical column

print("\nData after Frequency Encoding for Mahalanobis Distance Head:")
print(df_md.head())

# Scale all features for Mahalanobis Distance
# It's crucial to scale for MD because it uses absolute distances and variance.
# Robust scaling is good here.
scaler_md = StandardScaler()
features_for_md = df_md.drop('is_outlier_true', axis=1)
df_md_scaled = scaler_md.fit_transform(features_for_md)

# Convert scaled data back to DataFrame for easier handling
df_md_scaled = pd.DataFrame(df_md_scaled, columns=features_for_md.columns)
df_md_scaled['is_outlier_true'] = df_md['is_outlier_true']  # Add back true labels

print("\nData after Scaling for Mahalanobis Distance Head:")
print(df_md_scaled.head())

# --- 5. Apply Mahalanobis Distance ---
# Calculate mean and robust covariance matrix using MinCovDet
# MinCovDet is less sensitive to outliers in the data when calculating mean and covariance.
robust_cov = MinCovDet(random_state=42).fit(df_md_scaled.drop('is_outlier_true', axis=1))
mean_vector = robust_cov.location_
covariance_matrix_inv = np.linalg.inv(robust_cov.covariance_)

# Calculate Mahalanobis Distance for each point
df_md_scaled['md_distance'] = [mahalanobis(x, mean_vector, covariance_matrix_inv) for x in
                               df_md_scaled.drop('is_outlier_true', axis=1).values]

# Determine threshold using chi-squared distribution (assuming multivariate normality)
# Degrees of freedom = number of features used
num_features_md = df_md_scaled.drop('is_outlier_true', axis=1).shape[1]
from scipy.stats import chi2

# Common alpha levels are 0.01 or 0.001. We use 0.001 for strict outlier detection.
alpha = 0.001
md_threshold_squared = chi2.ppf(1 - alpha, num_features_md)
md_threshold = np.sqrt(md_threshold_squared)  # Convert back to distance

print(f"\nMahalanobis Distance Threshold (alpha={alpha}): {md_threshold:.2f}")

# Predict outliers based on threshold
df_md_scaled['md_outlier_predicted'] = (df_md_scaled['md_distance'] > md_threshold).astype(int)

print("\nMahalanobis Distance Outlier Prediction Summary:")
print(df_md_scaled['md_outlier_predicted'].value_counts())

# Evaluate MD performance (simple comparison)
md_true_positives = \
df_md_scaled[(df_md_scaled['is_outlier_true'] == 1) & (df_md_scaled['md_outlier_predicted'] == 1)].shape[0]
md_false_positives = \
df_md_scaled[(df_md_scaled['is_outlier_true'] == 0) & (df_md_scaled['md_outlier_predicted'] == 1)].shape[0]
md_false_negatives = \
df_md_scaled[(df_md_scaled['is_outlier_true'] == 1) & (df_md_scaled['md_outlier_predicted'] == 0)].shape[0]

print(
    f"Mahalanobis Distance - True Positives: {md_true_positives}, False Positives: {md_false_positives}, False Negatives: {md_false_negatives}")

# --- 6. Visualize and Compare Results ---

# Convert actual outlier status to a string for plotting
df_if_scaled['True_Status'] = df_if_scaled['is_outlier_true'].map({0: 'Inlier', 1: 'True Outlier'})
df_if_scaled['IF_Prediction'] = df_if_scaled['if_outlier_predicted'].map(
    {0: 'Predicted Inlier', 1: 'Predicted Outlier'})

df_md_scaled['True_Status'] = df_md_scaled['is_outlier_true'].map({0: 'Inlier', 1: 'True Outlier'})
df_md_scaled['MD_Prediction'] = df_md_scaled['md_outlier_predicted'].map(
    {0: 'Predicted Inlier', 1: 'Predicted Outlier'})

# Plotting Isolation Forest scores vs. true outliers (using numerical features for visualization)
plt.figure(figsize=(14, 6))

plt.subplot(1, 2, 1)
sns.scatterplot(x=df_if_scaled['num_feat_1'], y=df_if_scaled['num_feat_2'], hue=df_if_scaled['True_Status'],
                style=df_if_scaled['IF_Prediction'], palette='coolwarm', s=50, alpha=0.7)
plt.title('Isolation Forest: True vs. Predicted Outliers (Numerical Feat. 1 vs 2)')
plt.xlabel('Numerical Feature 1')
plt.ylabel('Numerical Feature 2')

# Plotting Mahalanobis Distance vs. true outliers
plt.subplot(1, 2, 2)
sns.scatterplot(x=df_md_scaled['num_feat_1'], y=df_md_scaled['num_feat_2'], hue=df_md_scaled['True_Status'],
                style=df_md_scaled['MD_Prediction'], palette='coolwarm', s=50, alpha=0.7)
plt.title('Mahalanobis Distance: True vs. Predicted Outliers (Numerical Feat. 1 vs 2)')
plt.xlabel('Numerical Feature 1')
plt.ylabel('Numerical Feature 2')

plt.tight_layout()
plt.show()

# You can also visualize the distributions of scores
plt.figure(figsize=(12, 5))
plt.subplot(1, 2, 1)
sns.histplot(df_if_scaled['if_scores'], kde=True, bins=50)
plt.title('Distribution of Isolation Forest Scores')
plt.xlabel('Anomaly Score')
plt.ylabel('Count')

plt.subplot(1, 2, 2)
sns.histplot(df_md_scaled['md_distance'], kde=True, bins=50)
plt.title('Distribution of Mahalanobis Distances')
plt.xlabel('Mahalanobis Distance')
plt.ylabel('Count')
plt.tight_layout()
plt.show()

# Final Comparison of correctly identified outliers
print("\n--- Final Outlier Detection Comparison ---")
print(f"Total True Outliers: {num_outliers}")
print(f"Isolation Forest identified {if_true_positives} true outliers and {if_false_positives} false positives.")
print(f"Mahalanobis Distance identified {md_true_positives} true outliers and {md_false_positives} false positives.")