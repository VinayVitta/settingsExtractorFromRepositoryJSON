from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd


def write_to_bigquery(
        df: pd.DataFrame,
        project_id: str,
        dataset_id: str,
        table_id: str,
        json_key_path: str,
        write_mode: str = "append"  # or 'replace' or 'fail'
):
    """
    Write a DataFrame to a BigQuery table.

    Args:
        df (pd.DataFrame): The data to upload.
        project_id (str): GCP project ID.
        dataset_id (str): BigQuery dataset ID.
        table_id (str): BigQuery table name.
        json_key_path (str): Path to the service account JSON key.
        write_mode (str): 'append', 'replace', or 'fail'.

    Returns:
        bigquery.job.LoadJob: The load job result.
    """
    try:
        print(f"▶️ Uploading {len(df)} rows to BigQuery table {dataset_id}.{table_id}...")

        credentials = service_account.Credentials.from_service_account_file(json_key_path)
        client = bigquery.Client(credentials=credentials, project=project_id)

        full_table_id = f"{project_id}.{dataset_id}.{table_id}"

        job_config = bigquery.LoadJobConfig(
            write_disposition={
                'append': bigquery.WriteDisposition.WRITE_APPEND,
                'replace': bigquery.WriteDisposition.WRITE_TRUNCATE,
                'fail': bigquery.WriteDisposition.WRITE_EMPTY
            }[write_mode],
            autodetect=True,
        )

        job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
        job.result()

        print(f"✅ Loaded {job.output_rows} rows into {full_table_id}")
        return job

    except Exception as e:
        print(f"❌ Failed to load data to BigQuery: {e}")
        raise


def main():
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"]
    })

    job = write_to_bigquery(
        df=df,
        project_id="jitencse",
        dataset_id="hackathon_2025",
        table_id="test_table",
        json_key_path=r"C:\Users\VIT\Downloads\jitencse-dee6a1c81168 1.json",
        write_mode="replace"  # or 'append'
    )


if __name__ == "__main__":
    main()
# print("writing data to GBQ")
# bigQueryWriteData.write_to_bigquery(
#    df=combined_task_settings_df,
#    project_id="jitencse",
#    dataset_id="hackathon_2025",
#    table_id="combined_task_settings_df",
#    json_key_path=r"C:\Users\VIT\Downloads\jitencse-dee6a1c81168 1.json",
#    write_mode="replace"  # or 'append'
#)
