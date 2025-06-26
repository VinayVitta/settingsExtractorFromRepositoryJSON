import pandas as pd

# Your loaded DataFrame
df = pd.read_csv(r"C:\Users\VIT\OneDrive - QlikTech Inc\QlikVit\Customers\EdwardJones\PlatformReview\filecloud-20250430192131\run_output_20250528_111756\exportRepositoryCSV_20250528_111756.csv") # or however you load your data

# Define DB types and the corresponding column name patterns
db_type_patterns = {
    "ORACLE_COMPONENT_TYPE": "src_oracle_",
    "SNOWFLAKE": "target_snowflake_",
    "SQL_SERVER": "src_sql_",
    "MONGO": "src_mongo_",
    # Add more if needed
}

results = []

for db_type, pattern in db_type_patterns.items():
    # Check columns matching pattern
    matched_cols = [col for col in df.columns if pattern in col]
    print(f"{db_type} matched columns: {matched_cols}")

    if not matched_cols:
        continue

    # Determine whether it's a source or target db type
    if pattern.startswith("src_"):
        tasks_df = df[df["source_db_type"] == db_type]
    elif pattern.startswith("target_"):
        tasks_df = df[df["target_db_type"] == db_type]
    else:
        continue

    for col in matched_cols:
        non_null_tasks = tasks_df[~tasks_df[col].isnull()]
        distinct_task_names = non_null_tasks["task_name"].nunique()

        results.append({
            "db_type": db_type,
            "setting_column": col,
            "used_in_tasks": distinct_task_names
        })

# Create summary DataFrame
if results:
    summary_df = pd.DataFrame(results).sort_values(by=["db_type", "used_in_tasks"], ascending=[True, False])
    print(summary_df)
else:
    print("⚠️ No matching settings found for the specified DB types.")
