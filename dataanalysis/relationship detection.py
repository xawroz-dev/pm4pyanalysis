import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
from statsmodels.formula.api import ols
from statsmodels.stats.anova import anova_lm
from statsmodels.stats.multicomp import pairwise_tukeyhsd

# Set plot style
sns.set_style("whitegrid")

# --- Step 1: Data Generation and Preprocessing ---
print("--- Step 1: Data Generation and Preprocessing ---")

# Generate synthetic event data
np.random.seed(42) # for reproducibility
num_cases = 100
events_per_case = np.random.randint(2, 10, num_cases) # Each case has 2-9 events

data = []
case_id_counter = 1
for num_events in events_per_case:
    case_id = f"Case_{case_id_counter:03d}"
    market = np.random.choice(['North', 'South', 'East', 'West'], p=[0.25, 0.25, 0.25, 0.25])
    case_category = np.random.choice(['Service', 'Product', 'Support'])
    case_type = np.random.choice(['TypeA', 'TypeB', 'TypeC', 'TypeD'])
    business_unit = np.random.choice(['BU1', 'BU2'])

    for _ in range(num_events):
        time_taken = np.random.normal(loc=50, scale=10) # Base time taken
        # Introduce some outliers conditionally
        if np.random.rand() < 0.02: # 2% chance of a high outlier
            time_taken = np.random.normal(loc=200, scale=30)
        elif np.random.rand() < 0.01: # 1% chance of a low outlier
            time_taken = np.random.normal(loc=5, scale=2)

        data.append({
            'CaseID': case_id,
            'Market': market,
            'CaseCategory': case_category,
            'CaseType': case_type,
            'BusinessUnit': business_unit,
            'TimeTaken': max(1, time_taken) # Ensure time is positive
        })
    case_id_counter += 1

event_df = pd.DataFrame(data)
print("Sample Event-level Data Head:")
print(event_df.head())
print("\nEvent-level Data Info:")
event_df.info()

# Aggregate to case-level for 'frequency' and 'total time taken'
# For 'Frequency', let's assume it's the number of events per case for simplicity
# For 'Time Taken', let's use the average time taken per event for a case to find "slow cases"
case_df = event_df.groupby('CaseID').agg(
    Market=('Market', 'first'), # Market, category etc. are constant per case
    CaseCategory=('CaseCategory', 'first'),
    CaseType=('CaseType', 'first'),
    BusinessUnit=('BusinessUnit', 'first'),
    TotalTimeTaken=('TimeTaken', 'sum'),
    AverageTimeTaken=('TimeTaken', 'mean'),
    Frequency=('CaseID', 'count')
).reset_index()

print("\nSample Case-level Data Head (Aggregated):")
print(case_df.head())
print("\nCase-level Data Info:")
case_df.info()

# --- Step 2: Exploratory Data Analysis (EDA) ---
print("\n--- Step 2: Exploratory Data Analysis (EDA) ---")

print("\nDescriptive Statistics for Numerical Variables (Event-level):")
print(event_df[['TimeTaken']].describe())

print("\nDescriptive Statistics for Numerical Variables (Case-level):")
print(case_df[['TotalTimeTaken', 'AverageTimeTaken', 'Frequency']].describe())

# Distributions of TimeTaken (Event-level)
plt.figure(figsize=(12, 5))
plt.subplot(1, 2, 1)
sns.histplot(event_df['TimeTaken'], kde=True)
plt.title('Distribution of Time Taken (Event-level)')
plt.subplot(1, 2, 2)
sns.boxplot(y=event_df['TimeTaken'])
plt.title('Box Plot of Time Taken (Event-level)')
plt.tight_layout()
plt.show()

# Distributions of AverageTimeTaken and Frequency (Case-level)
plt.figure(figsize=(12, 5))
plt.subplot(1, 2, 1)
sns.histplot(case_df['AverageTimeTaken'], kde=True)
plt.title('Distribution of Average Time Taken (Case-level)')
plt.subplot(1, 2, 2)
sns.histplot(case_df['Frequency'], kde=True)
plt.title('Distribution of Frequency (Case-level)')
plt.tight_layout()
plt.show()

# Categorical Variable Distributions
fig, axes = plt.subplots(2, 2, figsize=(14, 10))
sns.countplot(x='Market', data=case_df, ax=axes[0, 0])
axes[0, 0].set_title('Distribution of Market')
sns.countplot(x='CaseCategory', data=case_df, ax=axes[0, 1])
axes[0, 1].set_title('Distribution of Case Category')
sns.countplot(x='CaseType', data=case_df, ax=axes[1, 0])
axes[1, 0].set_title('Distribution of Case Type')
sns.countplot(x='BusinessUnit', data=case_df, ax=axes[1, 1])
axes[1, 1].set_title('Distribution of Business Unit')
plt.tight_layout()
plt.show()

# --- Step 3: Statistical Tests for Variable Relationships ---
print("\n--- Step 3: Statistical Tests for Variable Relationships ---")

# Let's check the relationship between categorical variables and 'AverageTimeTaken' (case-level)
# We will use Kruskal-Wallis H-test because 'TimeTaken' is often not normally distributed and can be skewed.
# If you confirm normality and homogeneity of variance, ANOVA would be suitable.

categorical_cols = ['Market', 'CaseCategory', 'CaseType', 'BusinessUnit']
numerical_target = 'AverageTimeTaken' # or 'Frequency'

print(f"\nStatistical Tests for {numerical_target} vs. Categorical Variables:")

for col in categorical_cols:
    groups = [case_df[numerical_target][case_df[col] == cat].dropna() for cat in case_df[col].unique()]
    if len(groups) > 1: # Need at least two groups to compare
        stat, p = stats.kruskal(*groups)
        print(f"\nKruskal-Wallis H-test for {col} vs. {numerical_target}:")
        print(f"H-statistic: {stat:.4f}, p-value: {p:.4f}")
        if p < 0.05:
            print(f"Significant relationship found between {col} and {numerical_target} (p < 0.05).")
            # Perform post-hoc test if significant (Tukey HSD for balanced groups, Dunn's for unbalanced/non-normal)
            # For simplicity, we'll use Tukey HSD on the original data for visualization purposes.
            # For formal Kruskal-Wallis post-hoc, you'd use a package like scikit-posthocs.
            # Here, we'll just show the boxplot.
            plt.figure(figsize=(8, 6))
            sns.boxplot(x=col, y=numerical_target, data=case_df)
            plt.title(f'{numerical_target} by {col}')
            plt.show()
            # If you want to do ANOVA and Tukey HSD:
            try:
                model = ols(f'{numerical_target} ~ C({col})', data=case_df).fit()
                anova_table = anova_lm(model, typ=2)
                print(f"ANOVA for {col} vs. {numerical_target}:")
                print(anova_table)
                if anova_table['PR(>F)'][0] < 0.05:
                    tukey_result = pairwise_tukeyhsd(endog=case_df[numerical_target], groups=case_df[col], alpha=0.05)
                    print(f"Tukey HSD Post-hoc Test for {col}:")
                    print(tukey_result)
            except Exception as e:
                print(f"Could not perform ANOVA/Tukey for {col}: {e}")
        else:
            print(f"No significant relationship found between {col} and {numerical_target} (p >= 0.05).")
    else:
        print(f"Skipping {col}: Not enough groups to perform test.")

# --- Step 4: Multivariate Outlier Detection (Case-based, Group-wise IQR) ---
print("\n--- Step 4: Multivariate Outlier Detection (Case-based, Group-wise IQR) ---")

outlier_target = 'AverageTimeTaken'
grouping_cols = ['Market', 'CaseCategory', 'CaseType']  # The combination for multivariate outlier

case_df['is_outlier'] = False
case_df['outlier_type'] = ''

print(f"\nDetecting outliers for '{outlier_target}' based on grouping by {grouping_cols} using IQR method.")

# Iterate through each unique combination of the grouping columns
for name, group in case_df.groupby(grouping_cols):
    Q1 = group[outlier_target].quantile(0.25)
    Q3 = group[outlier_target].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    # Identify outliers within this group
    # Get the indices of the outliers within the *original* case_df
    low_outlier_indices = group[group[outlier_target] < lower_bound].index
    high_outlier_indices = group[group[outlier_target] > upper_bound].index

    # Combine both sets of indices
    outlier_indices = low_outlier_indices.union(high_outlier_indices)

    if not outlier_indices.empty:
        # Update the original DataFrame's 'is_outlier' column using the indices
        case_df.loc[outlier_indices, 'is_outlier'] = True

        # Assign outlier type based on the specific conditions
        case_df.loc[low_outlier_indices, 'outlier_type'] = 'Low Outlier'
        case_df.loc[high_outlier_indices, 'outlier_type'] = 'High Outlier'

        print(f"\nGroup: {name}")
        print(f"  Q1: {Q1:.2f}, Q3: {Q3:.2f}, IQR: {IQR:.2f}")
        print(f"  Lower Bound: {lower_bound:.2f}, Upper Bound: {upper_bound:.2f}")
        print(f"  Outliers found: {len(outlier_indices)}")
        print(case_df.loc[outlier_indices, ['CaseID', outlier_target]])  # Show the actual outlier values

# Display cases identified as outliers
outliers_df = case_df[case_df['is_outlier']].copy()
print("\nCases identified as multivariate outliers:")
print(outliers_df[['CaseID', 'Market', 'CaseCategory', 'CaseType', 'AverageTimeTaken', 'outlier_type']])

print(f"\nTotal outliers found: {len(outliers_df)} out of {len(case_df)} cases.")
# --- Step 5: Visualization of Findings ---
print("\n--- Step 5: Visualization of Findings ---")

# Visualize 'AverageTimeTaken' by Market and CaseCategory, highlighting outliers
plt.figure(figsize=(14, 8))
sns.boxplot(x='Market', y='AverageTimeTaken', hue='CaseCategory', data=case_df)
# Overlay outliers as points
# Note: The FutureWarning about 'color=' being deprecated for gradients is a Seaborn internal warning.
# For a single color, 'color=' is still fine. If you want a specific color like red, just use 'red'.
sns.stripplot(x='Market', y='AverageTimeTaken', hue='CaseCategory', data=outliers_df,
              marker='o', color='red', edgecolor='black', linewidth=1, jitter=True, dodge=True, legend=False) # Added dodge=True for better alignment with boxplot
plt.title('Average Time Taken by Market and Case Category with Outliers Highlighted')
plt.ylabel('Average Time Taken (per Case)')
plt.show()

# You can create more specific visualizations if needed, e.g., FacetGrid
g = sns.FacetGrid(case_df, col="Market", row="CaseCategory", height=4, aspect=1.5, sharey=True)
g.map_dataframe(sns.boxplot, x="CaseType", y="AverageTimeTaken")
# Important: When using map_dataframe for stripplot, ensure the 'data' argument refers to the filtered dataframe
# and that 'x' and 'y' are consistent with the mapping function.
# Also, ensure 'hue' is not used in map_dataframe for consistency if it's not part of the FacetGrid setup.
g.map_dataframe(lambda data, color, **kwargs: sns.stripplot(x="CaseType", y="AverageTimeTaken", data=data[data['is_outlier']],
                                                              marker='o', color='red', edgecolor='black', linewidth=1, jitter=True, ax=plt.gca(), **kwargs))

# Corrected set_titles method: Use the 'template' argument with standard formatting
g.set_axis_labels("Case Type", "Average Time Taken")
g.set_titles(col_template="{col_name}", row_template="{row_name}") # Corrected: use col_template and row_template
plt.suptitle('Average Time Taken Distribution by Market, Case Category, and Case Type (Outliers in Red)', y=1.02)
plt.tight_layout(rect=[0, 0.03, 1, 0.98])
plt.show()

# --- Step 6: Other Potentially Helpful Analyses (Example: Outlier Breakdown) ---
print("\n--- Step 6: Other Potentially Helpful Analyses ---")

print("\nBreakdown of Outliers by Market:")
print(outliers_df['Market'].value_counts())

print("\nBreakdown of Outliers by CaseCategory:")
print(outliers_df['CaseCategory'].value_counts())

print("\nBreakdown of Outliers by Outlier Type:")
print(outliers_df['outlier_type'].value_counts())