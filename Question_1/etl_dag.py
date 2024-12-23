# -*- coding: utf-8 -*-
"""
Created on Wed Dec 18 11:41:26 2024

@author: odanielz
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import your ETL functions
from extraction_files.extract import extract_clickup_data, extract_float_data
from transformation_files.transform import (
    transform_clickup_data,
    transform_float_data,
    retrieve_task_data,
    create_fact_and_meeting_hour_dataframes,
    transform_fact_hours_dataframe,
    transform_meeting_hours_dataframe,
)
from loading_files.load import write_to_dim_table, write_to_fact_table, write_to_meeting_fact_table

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG
with DAG(
    dag_id='etl_airflow_dag',
    default_args=default_args,
    description='ETL process for ClickUp and Float data',
    schedule_interval='@daily',  # Run daily
    start_date=datetime(2024, 1, 1),
    catchup=False,  # Prevent backfilling for old dates
) as dag:

    # Define the ETL tasks
    
    # Extract Tasks
    extract_clickup = PythonOperator(
        task_id='extract_clickup_data',
        python_callable=extract_clickup_data,
    )

    extract_float = PythonOperator(
        task_id='extract_float_data',
        python_callable=extract_float_data,
    )

    # Transform Tasks
    def transform_float_task(float_df):
        return transform_float_data(float_df)

    def transform_clickup_task(clickup_df):
        return transform_clickup_data(clickup_df)

    transform_float = PythonOperator(
        task_id='transform_float_data',
        python_callable=transform_float_task,
        op_kwargs={'float_df': '{{ ti.xcom_pull(task_ids="extract_float_data") }}'},
    )

    transform_clickup = PythonOperator(
        task_id='transform_clickup_data',
        python_callable=transform_clickup_task,
        op_kwargs={'clickup_df': '{{ ti.xcom_pull(task_ids="extract_clickup_data") }}'},
    )

    def retrieve_task_task(transformed_clickup_df, transformed_float_df):
        return retrieve_task_data(transformed_clickup_df, transformed_float_df)

    retrieve_task = PythonOperator(
        task_id='retrieve_task_data',
        python_callable=retrieve_task_task,
        op_kwargs={
            'transformed_clickup_df': '{{ ti.xcom_pull(task_ids="transform_clickup_data") }}',
            'transformed_float_df': '{{ ti.xcom_pull(task_ids="transform_float_data") }}',
        },
    )

    def create_fact_and_meeting_task(transformed_clickup_df, transformed_float_df):
        return create_fact_and_meeting_hour_dataframes(transformed_clickup_df, transformed_float_df)

    create_fact_and_meeting = PythonOperator(
        task_id='create_fact_and_meeting_hour_dataframes',
        python_callable=create_fact_and_meeting_task,
        op_kwargs={
            'transformed_clickup_df': '{{ ti.xcom_pull(task_ids="transform_clickup_data") }}',
            'transformed_float_df': '{{ ti.xcom_pull(task_ids="transform_float_data") }}',
        },
    )

    def transform_fact_task(fact_hours):
        return transform_fact_hours_dataframe(fact_hours)

    transform_fact = PythonOperator(
        task_id='transform_fact_hours_dataframe',
        python_callable=transform_fact_task,
        op_kwargs={'fact_hours': '{{ ti.xcom_pull(task_ids="create_fact_and_meeting_hour_dataframes")["fact_hours"] }}'},
    )

    def transform_meeting_task(meeting_hours):
        return transform_meeting_hours_dataframe(meeting_hours)

    transform_meeting = PythonOperator(
        task_id='transform_meeting_hours_dataframe',
        python_callable=transform_meeting_task,
        op_kwargs={'meeting_hours': '{{ ti.xcom_pull(task_ids="create_fact_and_meeting_hour_dataframes")["meeting_hours"] }}'},
    )

    # Load Tasks
    write_dim = PythonOperator(
        task_id='write_to_dim_table',
        python_callable=write_to_dim_table,
        op_kwargs={
            'transformed_float_df': '{{ ti.xcom_pull(task_ids="transform_float_data") }}',
            'transformed_clickup_df': '{{ ti.xcom_pull(task_ids="transform_clickup_data") }}',
            'task_df': '{{ ti.xcom_pull(task_ids="retrieve_task_data") }}',
        },
    )

    write_fact = PythonOperator(
        task_id='write_to_fact_table',
        python_callable=write_to_fact_table,
        op_kwargs={'transformed_fact_hours': '{{ ti.xcom_pull(task_ids="transform_fact_hours_dataframe") }}'},
    )

    write_meeting_fact = PythonOperator(
        task_id='write_to_meeting_fact_table',
        python_callable=write_to_meeting_fact_table,
        op_kwargs={'transformed_meeting_hours': '{{ ti.xcom_pull(task_ids="transform_meeting_hours_dataframe") }}'},
    )

    # TASK DEPENDENCIES
    # Extract -> Transform -> Load
    [extract_clickup, extract_float] >> [transform_clickup, transform_float]
    [transform_clickup, transform_float] >> retrieve_task >> create_fact_and_meeting
    create_fact_and_meeting >> [transform_fact, transform_meeting]

    # Dimension table must load before fact tables
    [transform_clickup, transform_float, retrieve_task] >> write_dim
    write_dim >> [write_fact, write_meeting_fact]