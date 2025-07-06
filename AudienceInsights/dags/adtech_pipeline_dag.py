from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {"start_date": datetime(2023, 1, 1)}

with DAG("adtech_pipeline",
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:

    extract = BashOperator(
        task_id="extract_clickstream",
        bash_command="spark-submit /opt/airflow/jobs/extract_clickstream.py"
    )

    transform = BashOperator(
        task_id="clean_transform",
        bash_command="spark-submit /opt/airflow/jobs/clean_transform_clicks.py"
    )

    enrich = BashOperator(
        task_id="enrich_with_metadata",
        bash_command="spark-submit /opt/airflow/jobs/enrich_with_metadata.py"
    )

    geo = BashOperator(
        task_id="geo_enrich_ip",
        bash_command="spark-submit /opt/airflow/jobs/geo_enrich_ip.py"
    )

    aggregate = BashOperator(
        task_id="aggregate_insights",
        bash_command="spark-submit /opt/airflow/jobs/aggregate_insights.py"
    )

    export = BashOperator(
        task_id="export_kpis",
        bash_command="spark-submit /opt/airflow/jobs/export_kpis.py"
    )

    validate = BashOperator(
        task_id="validate_output",
        bash_command="spark-submit /opt/airflow/jobs/validate_pipeline_output.py"
    )

    extract >> transform >> enrich >> geo >> aggregate >> export >> validate
