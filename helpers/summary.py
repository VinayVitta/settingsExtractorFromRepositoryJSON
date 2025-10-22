import pandas as pd
import duckdb
from helpers.queries import tasksCounts, changeProcessTuning, handlingPolicy, logStream, tablesData
import os
from helpers.logger_config import setup_logger
from helpers.docx.docCreation import export_tables_to_word
from helpers.utils import apply_state_filter  # Your helper from Option 1

# Configure Logging
logging = setup_logger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
logo_path = os.path.join(BASE_DIR, "docx", "QlikNewLogo.png")

def read_csv(path):
    """Read CSV file into a Pandas DataFrame."""
    if not os.path.exists(path):
        logging.error(f"CSV file not found: {path}")
        raise FileNotFoundError(path)
    logging.info(f"Reading CSV from {path}")
    return pd.read_csv(path)


def run_queries(query_list, include_all_states=False):
    """Run a list of DuckDB queries with optional state filtering."""
    dataframes = []
    for q in query_list:
        filtered_query = apply_state_filter(q, include_all_states)
        df = duckdb.query(filtered_query).to_df()
        dataframes.append(df)
    return pd.concat(dataframes, ignore_index=True)


def create_summary(csv_result_file_path, output_docx_path, include_all_states=False):
    """
    Creates a Word summary report based on task configuration and metrics.
    :param csv_result_file_path: Path to the CSV export.
    :param output_docx_path: Path for output Word file.
    :param include_all_states: If True, includes all qem_State values. If False, filters for running only.
    """
    # Load data
    data_df = read_csv(csv_result_file_path)
    data_df = data_df.astype(str)
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
        "Multiple LogStream Connecting to Same Source Server": {
            "queries": [logStream.multipleLogStreamSameSourceDB],
            "notes": "Shows the number of LogStream tasks connecting to same source database."
        },
        "Replication of the same Table to Multiple Targets with different Replicate servers": {
            "queries": [tablesData.duplicate_replication_multiple_targets_with_diff_replicate_server],
            "notes": "Lists of Tables replicate more than once to the same or more target Databases and are hosted on different Replicate servers."
        },
        "Replication of same Table to Same Targets": {
            "queries": [tablesData.duplicate_replication_same_targets],
            "notes": "Lists of Tables replicate more than once to same target DB's."
        },
        "LogStream tasks with NO Child/Replication tasks": {
            "queries": [logStream.losgtreamwithNoChild],
            "notes": "Lists of all Running LogStream tasks with NO Child/Replicate tasks."
        },
    }

    # Run queries and collect summaries
    summary_tables = []
    for title, content in queries.items():
        logging.info(f"Running: {title}")
        df = run_queries(content["queries"], include_all_states)
        summary_tables.append({
            "title": title,
            "notes": content["notes"],
            "df": df
        })

    # Export to Word
    export_tables_to_word(
        summary_tables,
        output_docx_path,
        title="QLik Replicate - Task Summary",
        logo_path=logo_path
    )

    logging.info(f"Summary document created: {output_docx_path}")


def main():
    csv_file_path = r"C:\Users\VIT\OneDrive - QlikTech Inc\QlikVit\Customers\Ally\HealthCheck\LatestFIles\run_output_20250626_172717\exportRepositoryCSV_20250626_172717.csv"
    output_docx_path = r"C:\Users\VIT\OneDrive - QlikTech Inc\QlikVit\Customers\EdwardJones\PlatformReview\filecloud-20250430192131\run_output_20250528_111756\full_task_summary.docx"

    # You can toggle this dynamically from your UI
    include_all_states = True  # or False for "Running-only"

    create_summary(csv_file_path, output_docx_path, include_all_states)


if __name__ == "__main__":
    main()
