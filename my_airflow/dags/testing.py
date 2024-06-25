from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

DAG_ID = "bigquery_check_table_existence"
PROJECT_ID = "cloudace-project-demo"
DATASET_ID = "crypto_data"
TABLE_ID = "btc_price_24h_day_boxplot"

TABLE_FULL_NAME = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

with DAG(
    DAG_ID,
    schedule_interval='0 0 * * *',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "bigquery"],
) as dag:

    # Example query to check table existence
    query = f"SELECT * FROM `{TABLE_FULL_NAME}` LIMIT 1"

    # Use BigQueryInsertJobOperator to attempt inserting job (which will fail if table doesn't exist)
    check_table_existence = BigQueryInsertJobOperator(
        task_id="check_table_existence",
        configuration={
            "query": {
                "query": query,
                "useLegacySql": False,
            }
        },
    )

    # BashOperator to echo the result based on task success or failure
    table_result = BashOperator(
        task_id="table_result",
        bash_command=(
            "if [ $? -eq 0 ]; then "
            "echo 'Table exists'; "
            "else "
            "echo 'Table does not exist'; "
            "fi"
        ),
    )

    check_table_existence >> table_result
