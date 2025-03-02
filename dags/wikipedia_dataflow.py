from airflow import DAG
from datetime import datetime
import os
import sys
from airflow.operators.python import PythonOperator
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines.wikipedia_pipeline import extract_data, transform_data, write_data

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 30),
}

with DAG(dag_id='wikipedia_dataflow',default_args=default_args,schedule='@daily',catchup=False) as dag:

    extract_data_from_wikipedia=PythonOperator(
        task_id='extract_data_from_wikipedia',
        python_callable=extract_data,
        provide_context=True,
        op_kwargs={"url":"https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity"},
        dag=dag
    )

    transform_data_from_wikipedia=PythonOperator(
        task_id='transform_data_from_wikipedia',
        python_callable=transform_data,
        provide_context=True,
        dag=dag
    )

    write_wikipedia_data=PythonOperator(
        task_id='write_wikipedia_data',
        python_callable=write_data,
        provide_context=True,
        dag=dag
    )
extract_data_from_wikipedia >> transform_data_from_wikipedia >> write_wikipedia_data

