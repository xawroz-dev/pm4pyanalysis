import pandas as pd
import numpy as np
from scipy.stats import kruskal
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import OneHotEncoder
import plotly.express as px
import plotly.graph_objects as go

# --- 1. Simulate Digital Bank Activity Data ---
# This section creates a synthetic dataset that mimics your digital bank activities.
# It includes categorical data (Market, Case_Category, Line_of_Business)
# and quantitative data (Frequency_of_Events, Time_Taken_seconds).
# We also deliberately introduce some "outliers" that are contextual.

np.random.seed(42)  # for reproducibility of results

num_records = 5000  # Total number of simulated activity records

# Define concrete values for categorical variables
markets = ['USA', 'UK', 'India', 'Germany', 'Brazil']
case_categories = ['Loan Application', 'Investment Transaction', 'Account Opening', 'Customer Service Inquiry',
                   'Credit Card Payment']
lines_of_business = ['Retail Banking', 'Corporate Banking', 'Wealth Management']

# Generate categorical data
data = {
    'Market': np.random.choice(markets, num_records),
    'Case_Category': np.random.choice(case_categories, num_records),
    'Line_of_Business': np.random.choice(lines_of_business, num_records),
    'Customer_ID': [f'CUST_{i:05d}' for i in range(num_records)],
    'Transaction_ID': [f'TRANS_{i:07d}' for i in range(num_records)]
}
df = pd.DataFrame(data)

# Generate quantitative data with some realistic dependencies and noise
# Base distributions
df['Frequency_of_Events'] = np.random.normal(loc=50, scale=15, size=num_records).astype(int)
df['Time_Taken_seconds'] = np.random.normal(loc=120, scale=40, size=num_records).astype(int)

# Introduce dependencies:
# Example 1: 'Loan Application' typically has lower frequency and longer time
df.loc[df['Case_Category'] == 'Loan Application', 'Frequency_of_Events'] = np.random.normal(loc=10, scale=5, size=(
            df['Case_Category'] == 'Loan Application').sum()).astype(int)
df.loc[df['Case_Category'] == 'Loan Application', 'Time_Taken_seconds'] = np.random.normal(loc=600, scale=100, size=(
            df['Case_Category'] == 'Loan Application').sum()).astype(int)

# Example 2: 'Investment Transaction' might have moderate frequency and moderate-long time
df.loc[df['Case_Category'] == 'Investment Transaction', 'Frequency_of_Events'] = np.random.normal(loc=20, scale=8,
                                                                                                  size=(df[
                                                                                                            'Case_Category'] == 'Investment Transaction').sum()).astype(
    int)
df.loc[df['Case_Category'] == 'Investment Transaction', 'Time_Taken_seconds'] = np.random.normal(loc=300, scale=70,
                                                                                                 size=(df[
                                                                                                           'Case_Category'] == 'Investment Transaction').sum()).astype(
    int)

# Example 3: 'India' market might have higher frequency and shorter times (e.g., more mobile-first, faster processes)
df.loc[df['Market'] == 'India', 'Frequency_of_Events'] = np.random.normal(loc=60, scale=10,
                                                                          size=(df['Market'] == 'India').sum()).astype(
    int)
df.loc[df['Market'] == 'India', 'Time_Taken_seconds'] = np.random.normal(loc=90, scale=20,
                                                                         size=(df['Market'] == 'India').sum()).astype(
    int)

# Ensure no negative values for frequency or time
df['Frequency_of_Events'] = df['Frequency_of_Events'].apply(lambda x: max(1, x))
df['Time_Taken_seconds'] = df['Time_Taken_seconds'].apply(lambda x: max(1, x))

# Add some explicit, contextual outliers for demonstration purposes
# These might not be outliers globally, but are unusual within their context.
current_num_records = len(df)

# Outlier 1: Extremely high frequency, very low time for a Loan Application in USA (suspiciously fast automation/bot)
df.loc[current_num_records] = ['USA', 'Loan Application', 'Retail Banking', 'CUST_BOT1', 'TRANS_BOT001', 500, 5]
current_num_records += 1

# Outlier 2: Extremely low frequency, very high time for a simple Checking Account inquiry in UK (system freeze, complex manual intervention)
df.loc[current_num_records] = ['UK', 'Customer Service Inquiry', 'Retail Banking', 'CUST_SYSFREEZE', 'TRANS_SYS001', 2,
                               1200]
current_num_records += 1

# Outlier 3: Normal quantitative values, but an unusual categorical combination
# (e.g., Corporate Banking dealing with a standard 'Account Opening' for a retail client in Brazil)
df.loc[current_num_records] = ['Brazil', 'Account Opening', 'Corporate Banking', 'CUST_UNUSUAL', 'TRANS_UNU001', 45,
                               110]
current_num_records += 1

# Outlier 4: Very high frequency for a context that usually has low frequency (e.g., Investment in Germany)
df.loc[current_num_records] = ['Germany', 'Investment Transaction', 'Wealth Management', 'CUST_HIGHINV',
                               'TRANS_HIGHINV', 300, 250]
current_num_records += 1

print("--- Sample of the Simulated Digital Banking Data ---")
print(df.head())
print(f"\nTotal records simulated: {len(df)}")
print("\n--- Data Information ---")
df.info()

# --- 2. Kruskal-Wallis Test for Dependency ---
# This section performs non-parametric tests to see if the distribution of
# your quantitative variables ('Frequency_of_Events', 'Time_Taken_seconds')
# differs significantly across the categories of your categorical variables.

quantitative_cols = ['Frequency_of_Events', 'Time_Taken_seconds']
categorical_cols = ['Market', 'Case_Category', 'Line_of_Business']

print("\n\n--- Kruskal-Wallis Test Results (Dependency Analysis) ---")
for q_col in quantitative_cols:
    print(f"\nTesting dependency for quantitative variable: '{q_col}'")
    for c_col in categorical_cols:
        # Prepare groups for Kruskal-Wallis test
        groups = [df[q_col][df[c_col] == category] for category in df[c_col].unique()]

        # Filter out empty groups (if a category somehow has no data, though unlikely with our simulation)
        groups = [g for g in groups if not g.empty]

        # Kruskal-Wallis requires at least 2 non-empty groups
        if len(groups) < 2:
            print(f"  Skipping '{c_col}' for '{q_col}': Not enough distinct groups ({len(groups)}).")
            continue

        stat, p_value = kruskal(*groups)  # Perform the Kruskal-Wallis H-test
        print(f"  '{c_col}' vs '{q_col}': H-statistic = {stat:.2f}, p-value = {p_value:.4f}")

        # Interpret the p-value (common significance level is 0.05)
        if p_value < 0.05:
            print(
                f"    -> Significant dependency found! The distribution of '{q_col}' differs across '{c_col}' categories.")
        else:
            print(
                f"    -> No significant dependency found. The distribution of '{'{q_col}'}' is similar across '{c_col}' categories.")

# --- 3. Data Transformation for Isolation Forest ---
# Isolation Forest requires numerical input. We convert categorical features
# into a numerical format using One-Hot Encoding. This expands your dataset's
# dimensionality but allows the algorithm to capture patterns across categories.

# Create a copy to avoid modifying the original DataFrame directly
df_for_if = df.copy()

# Initialize OneHotEncoder
# handle_unknown='ignore' will convert unknown categories encountered during transform to all zeros,
# which is useful if your training set doesn't contain all possible categories.
# sparse_output=False ensures a dense NumPy array is returned.
encoder = OneHotEncoder(handle_unknown='ignore', sparse_output=False)

# Fit the encoder to the categorical columns and then transform them
encoded_features = encoder.fit_transform(df_for_if[categorical_cols])

# Get the names for the new one-hot encoded columns
encoded_feature_names = encoder.get_feature_names_out(categorical_cols)

# Create a new DataFrame for the encoded categorical features
df_encoded_categorical = pd.DataFrame(encoded_features, columns=encoded_feature_names, index=df_for_if.index)

# Combine the original quantitative features with the new encoded categorical features
# This DataFrame will be the input for the Isolation Forest model.
features_for_isolation_forest = pd.concat([df_for_if[quantitative_cols], df_encoded_categorical], axis=1)

print("\n\n--- Features Prepared for Isolation Forest (first 5 rows) ---")
print(features_for_isolation_forest.head())
print(f"\nTotal features (dimensions) for Isolation Forest: {features_for_isolation_forest.shape[1]}")

# --- 4. Isolation Forest Outlier Detection ---
# Isolation Forest is an effective algorithm for high-dimensional data and works by
# building decision trees to isolate anomalies. It's particularly good at finding
# "contextual" outliers as it considers all features together.

# Initialize Isolation Forest model
# 'contamination' is an important parameter: it's your estimate of the proportion of outliers in the dataset.
# It helps the algorithm set a decision boundary. Adjust this based on your domain knowledge.
# 'random_state' ensures reproducibility of the results.
iso_forest = IsolationForest(contamination=0.01, random_state=42)  # Assuming 1% of data are outliers

# Fit the model to your prepared features and predict outliers.
# The `fit_predict` method returns -1 for outliers and 1 for inliers.
df_for_if['is_outlier_raw'] = iso_forest.fit_predict(features_for_isolation_forest)

# Get the anomaly score for each data point. Lower scores indicate a higher likelihood of being an anomaly.
df_for_if['anomaly_score'] = iso_forest.decision_function(features_for_isolation_forest)

# Create a more readable 'Outlier_Status' column for visualization
df_for_if['Outlier_Status'] = df_for_if['is_outlier_raw'].apply(lambda x: 'Outlier' if x == -1 else 'Normal')

print("\n\n--- Isolation Forest Outlier Detection Results (first 10 rows with status and score) ---")
print(df_for_if[['Frequency_of_Events', 'Time_Taken_seconds', 'Outlier_Status', 'anomaly_score']].head(10))
print(f"\nTotal number of outliers detected: {(df_for_if['Outlier_Status'] == 'Outlier').sum()}")

# --- 5. Visual Representation with Interactive Scatter Plot ---
# Using Plotly to create an interactive scatter plot.
# Outliers are colored differently. When you hover over a point,
# all its associated details (Customer ID, Market, Category, etc.) are displayed.

fig = px.scatter(
    df_for_if,
    x='Frequency_of_Events',
    y='Time_Taken_seconds',
    color='Outlier_Status',  # Color points based on their outlier status
    hover_data={  # Data to show on hover tooltip
        'Customer_ID': True,
        'Transaction_ID': True,
        'Market': True,
        'Case_Category': True,
        'Line_of_Business': True,
        'Frequency_of_Events': ':.0f',  # Format as integer
        'Time_Taken_seconds': ':.0f',  # Format as integer
        'anomaly_score': ':.3f',  # Format to 3 decimal places
        'is_outlier_raw': False  # Hide this raw column from hover
    },
    title='Multivariate Outlier Detection in Digital Bank Activity Data',
    labels={  # Axis labels
        'Frequency_of_Events': 'Frequency of Events (Clicks/Actions)',
        'Time_Taken_seconds': 'Time Taken (seconds)'
    },
    color_discrete_map={'Normal': 'blue', 'Outlier': 'red'},  # Define colors for statuses
    template='plotly_white'  # Clean background
)

# Customize hover template for more specific control over what's displayed
# and how it's formatted. 'customdata' holds the values from df_for_if.
# Make sure the order in customdata matches the indices used in hovertemplate.
custom_hover_data = df_for_if[
    ['Customer_ID', 'Transaction_ID', 'Market', 'Case_Category', 'Line_of_Business', 'anomaly_score']].values

fig.update_traces(
    hovertemplate=(
        "<b>Customer ID:</b> %{customdata[0]}<br>"
        "<b>Transaction ID:</b> %{customdata[1]}<br>"
        "<b>Market:</b> %{customdata[2]}<br>"
        "<b>Case Category:</b> %{customdata[3]}<br>"
        "<b>Line of Business:</b> %{customdata[4]}<br>"
        "<br>"  # Add a line break for readability
        "<b>Frequency:</b> %{x:.0f}<br>"
        "<b>Time Taken:</b> %{y:.0f} seconds<br>"
        "<b>Anomaly Score:</b> %{customdata[5]:.3f}<br>"
        "<extra></extra>"  # Removes the default trace name (e.g., 'Normal' or 'Outlier')
    ),
    marker=dict(size=8, opacity=0.7),  # Customize marker appearance
    customdata=custom_hover_data
)

fig.update_layout(hovermode="closest")  # Ensures hover works well for dense plots
fig.show()

print("\n\n--- End of Code Execution ---")