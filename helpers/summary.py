import pandas as pd
import duckdb
from helpers.queries import tasksCounts, changeProcessTuning, handlingPolicy, logStream, tablesData
import os
import logging
from helpers.docx.docCreation import export_tables_to_word

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def read_csv(path):
    """Read CSV file into a Pandas DataFrame."""
    if not os.path.exists(path):
        logging.error(f"CSV file not found: {path}")
        raise FileNotFoundError(path)
    logging.info(f"Reading CSV from {path}")
    return pd.read_csv(path)


def run_queries(query_list):
    """Run a list of DuckDB queries and return a combined DataFrame."""
    return pd.concat([duckdb.query(q).to_df() for q in query_list], ignore_index=True)


def main():
    # Config
    csv_file_path = r"C:\Users\VIT\OneDrive - QlikTech Inc\QlikVit\Customers\EdwardJones\PlatformReview\filecloud-20250430192131\run_output_20250507_094558\exportRepositoryCSV_20250507_094558.csv"
    output_docx_path = "full_task_summary.docx"

    # Load data
    data_df = read_csv(csv_file_path)
    duckdb.register("data_df", data_df)

    # Query groups
    queries = {
        "Running Tasks Summary": {
            "queries": [
                tasksCounts.total_tasks_query,
                tasksCounts.running_tasks_query,
                tasksCounts.replication_tasks_query,
                tasksCounts.logstream_tasks_query,
                tasksCounts.apply_changes_query,
                tasksCounts.store_changes_query,
                tasksCounts.apply_store_changes_query,
                tasksCounts.no_logstream_query
            ],
            "notes": "Provides counts of all tasks grouped by type and status."
        },
        "Batch Tuning Summary": {
            "queries": [changeProcessTuning.batch_tuning],
            "notes": "Summarizes batch tuning parameters like MIN, MAX, and Memory."
        },
        "Transaction Offloading Summary": {
            "queries": [changeProcessTuning.transaction_memory],
            "notes": "Shows memory settings for transaction offloading."
        },
        "Control Tables Usage": {
            "queries": [changeProcessTuning.control_tables],
            "notes": "Displays the use of control tables."
        },
        "LOB Size Summary": {
            "queries": [changeProcessTuning.lob_size],
            "notes": "Lists configuration for Large Object sizes."
        },
        "DDL Handling Policy": {
            "queries": [handlingPolicy.ddl_handling],
            "notes": "Shows how DDL operations are handled."
        },
        "General Handling Policy": {
            "queries": [handlingPolicy.handling_policy],
            "notes": "Displays general data handling policies."
        },
        "Error Handling Policy": {
            "queries": [handlingPolicy.error_handling],
            "notes": "Lists error handling configuration."
        },
        "LogStream Child Task Summary": {
            "queries": [logStream.totalChildTasksForEachParent],
            "notes": "Shows the number of child tasks under each log stream."
        },
        "Replication of same Table to Multiple Targets": {
            "queries": [tablesData.duplicate_replication_multiple_targets],
            "notes": "Lists of Tables replicate more than once to same or more target DB's."
        },
        "Replication of same Table to Same Targets": {
            "queries": [tablesData.duplicate_replication_same_targets],
            "notes": "Lists of Tables replicate more than once to same target DB's."
        },
    }

    # Run queries and collect summaries
    summary_tables = []
    for title, content in queries.items():
        logging.info(f"Running: {title}")
        df = run_queries(content["queries"])
        print(f"\n{title}:\n", df.to_string(index=False))  # Optional CLI output

        summary_tables.append({
            "title": title,
            "notes": content["notes"],
            "df": df
        })

    # Export to Word
    # export_tables_to_word(summary_tables, output_docx_path, title="QLik Replicate - Task Summary",logo_path=r"C:\Users\VIT\OneDrive - QlikTech Inc\QlikVit\UDocs\Qlik New Logo.png")
    # print(summary_tables)


if __name__ == "__main__":
    main()
