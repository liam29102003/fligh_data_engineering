import pandas as pd
from pathlib import Path

def run_gold_aggregate(**context):
    silver_file = context["ti"].xcom_pull(
        key="silver_file",
        task_ids="silver_transform"
    )

    if not silver_file:
        raise ValueError("Silver file path not found in XCom")

    gold_path = Path("/opt/airflow/data/gold")
    gold_path.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(silver_file)

    agg_df = df.groupby("origin_country").agg(
        total_flights=pd.NamedAgg(column="icao24", aggfunc="count"),
        avg_velocity=pd.NamedAgg(column="velocity", aggfunc="mean"),
        pct_on_ground=pd.NamedAgg(column="on_ground", aggfunc=lambda x: (x.sum() / len(x)) * 100)
    ).reset_index()

    output_file = gold_path / "flights_gold_aggregate.csv"
    agg_df.to_csv(output_file, index=False)

    context["ti"].xcom_push(
        key="gold_file",
        value=str(output_file)
    )
