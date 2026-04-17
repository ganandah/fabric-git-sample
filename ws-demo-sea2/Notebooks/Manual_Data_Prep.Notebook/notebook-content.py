# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "29b1f882-96ba-4a01-b12a-e7fe89c41f78",
# META       "default_lakehouse_name": "demo_poc_lakehouse",
# META       "default_lakehouse_workspace_id": "6f7d4bfe-f4b7-4c47-ae27-7e5580516687",
# META       "known_lakehouses": [
# META         {
# META           "id": "29b1f882-96ba-4a01-b12a-e7fe89c41f78"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# _**Import Library**_

# CELL ********************

import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
from sklearn.feature_selection import SelectKBest, f_classif
from sklearn.ensemble import RandomForestClassifier

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **_Load Data_**

# CELL ********************

import pandas as pd
# Load data into pandas DataFrame from "/lakehouse/default/Files/PoC_Dataset_for_Machine_Learning_Platform_2025-Based.csv"
df = pd.read_csv("/lakehouse/default/Files/PoC_Dataset_for_Machine_Learning_Platform_2025-Based.csv")
display(df)

print("Data types:")
print(df.dtypes)
print("\n" + "="*50)
print("Dataset Info:")
df.info()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# _**Data Preparation**_

# CELL ********************

# STEP 1: DATA PREPARATION
print("\n STEP 1: DATA PREPARATION")
print("-" * 40)

# Make a copy for processing
df_processed = df.copy()
print(f"Starting with shape: {df_processed.shape}")

# Drop ID column and other non-predictive columns
non_predictive_cols = ['id']
existing_non_predictive = [col for col in non_predictive_cols if col in df_processed.columns]
if existing_non_predictive:
    print(f"Dropping non-predictive columns: {existing_non_predictive}")
    df_processed.drop(columns=existing_non_predictive, inplace=True)

# Drop columns with too many missing values (>95% missing)
missing_before = df_processed.isnull().sum()

high_missing_cols = missing_before[missing_before > len(df_processed) * 0.95].index
if len(high_missing_cols) > 0:
    print(f"Dropping columns with >95% missing values: {list(high_missing_cols)}")
    df_processed.drop(columns=high_missing_cols, inplace=True)

# Handle remaining missing values
numeric_columns = df_processed.select_dtypes(include=[np.number]).columns
for col in numeric_columns:
    if df_processed[col].isnull().sum() > 0:
        df_processed[col].fillna(df_processed[col].median(), inplace=True)

categorical_columns = df_processed.select_dtypes(include=['object']).columns
for col in categorical_columns:
    if col != 'qualified_status' and df_processed[col].isnull().sum() > 0:
        if df_processed[col].isnull().sum() == len(df_processed):
            df_processed[col].fillna('Unknown', inplace=True)
        else:
            df_processed[col].fillna(df_processed[col].mode()[0], inplace=True)

# Encode categorical variables
for col in categorical_columns:
    if col != 'qualified_status':
        unique_vals = df_processed[col].nunique()
        if unique_vals <= 10:  # One-hot encode low cardinality
            dummies = pd.get_dummies(df_processed[col], prefix=col, drop_first=True)
            df_processed = pd.concat([df_processed, dummies], axis=1)
            df_processed.drop(col, axis=1, inplace=True)
        else:  # Label encode high cardinality
            le = LabelEncoder()
            df_processed[col] = le.fit_transform(df_processed[col])

# Handle target variable
if 'qualified_status' in df_processed.columns:
    le_target = LabelEncoder()
    df_processed['qualified_status'] = le_target.fit_transform(df_processed['qualified_status'])
    target_mapping = dict(zip(le_target.classes_, le_target.transform(le_target.classes_)))
    print(f"Target variable encoded: {target_mapping}")

print(f"Data preparation completed. Final shape: {df_processed.shape}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_processed)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/manual_prepared_dataset_full_2.csv")
# df now is a Spark DataFrame containing CSV data from "Files/manual_prepared_dataset_full_2.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# STEP 2: FEATURE SELECTION
print("\n STEP 2: FEATURE SELECTION")
print("-" * 40)

# Prepare features and target
target_col = 'qualified_status'
X = df_processed.drop(target_col, axis=1)
y = df_processed[target_col]

print(f"Total features before selection: {X.shape[1]}")

# Method 1: Statistical feature selection
k_features = min(20, X.shape[1])
selector = SelectKBest(score_func=f_classif, k=k_features)
X_selected = selector.fit_transform(X, y)
selected_features = X.columns[selector.get_support()]

print(f"selected_features: {selected_features}")

# Method 2: Random Forest feature importance
rf = RandomForestClassifier(n_estimators=100, random_state=42)
rf.fit(X, y)
feature_importance = pd.DataFrame({
    'feature': X.columns,
    'importance': rf.feature_importances_
}).sort_values('importance', ascending=False)

print(f"feature_importance: {feature_importance}")

# Combine both methods
top_statistical = set(selected_features)
top_rf = set(feature_importance.head(k_features).feature)
final_features = list(top_statistical.union(top_rf))

print(f"Final selected features ({len(final_features)}): combining statistical and RF methods")
X_final = X[final_features]

# tambahkan TargetCol
df_for_automl = pd.concat(
    [X_final.reset_index(drop=True), y.reset_index(drop=True).rename('qualified_status')],
    axis=1
)
print("Data types:")
print(df_for_automl.dtypes)
print("\n" + "="*50)
print("Dataset Info:")
df_for_automl.info()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Save the cleaned DataFrame to a new table
df_for_automl_spark = spark.createDataFrame(df_for_automl)
df_for_automl_spark.write.mode("overwrite").format("delta").saveAsTable("prepared_dataset_full")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Parquet (disarankan)
save_path = "/lakehouse/default/Files/manual_prepared_dataset_full.parquet"
df_for_automl.to_parquet(save_path, index=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
