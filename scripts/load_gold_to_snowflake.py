import pandas as pd
import snowflake.connector
from airflow.hooks.base import BaseHook

def run_load_gold_to_snowflake(**context):
    gold_file = context["ti"].xcom_pull(
        key="gold_file",
        task_ids="gold_aggregate"
    )

    if not gold_file:
        raise ValueError("Gold file path not found in XCom")

    execution_date = context["data_interval_start"].strftime("%Y-%m-%d-%H-%M-%S")

    df = pd.read_csv(gold_file)

    conn = BaseHook.get_connection("flight_snowflake")

    sf_conn = 

    