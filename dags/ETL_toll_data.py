import os
import sqlite3
from datetime import timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from utils import unzip_tolldata, extract_csv_data, extract_tsv_data, extract_fixed_width_data, consolidate_data_extracted, transform_load_data, load_data_to_sqlite, query_and_print_data


# Declaring known values
DESTINATION = "data"
source = os.path.join(DESTINATION, "tolldata.tgz")
vehicle_data = os.path.join(DESTINATION, "vehicle-data.csv")
tollplaza_data = os.path.join(DESTINATION, "tollplaza-data.tsv")
payment_data = os.path.join(DESTINATION, "payment-data.txt")
csv_data = os.path.join(DESTINATION, "csv_data.csv")
tsv_data = os.path.join(DESTINATION, "tsv_data.csv")
fixed_width_data = os.path.join(DESTINATION, "fixed_width_data.csv")
extracted_data = os.path.join(DESTINATION, "extracted_data.csv")
transformed_data = os.path.join(DESTINATION, "transformed_data.csv")
db_name = 'toll.db'

default_arg = {
    'owner': 'mmagdy841',
    'start_date': days_ago(0),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'ETL_toll_data',
    schedule_interval = timedelta(days=1),
    default_args = default_arg,
    description = 'Highway Traffic Data Pipeline'
)

##### Define tasks #####

# Create a task to create table
create_table = PythonOperator(
    task_id='create_table',
    python_callable=lambda: sqlite3.connect('database.db').execute("""
        CREATE TABLE IF NOT EXISTS toll_data (
            Rowid INT,
            Timestamp DATETIME,
            Anonymized_Vehicle_number TEXT,
            Vehicle_type TEXT,
            Number_of_axles INT,
            Tollplaza_id INT,
            Tollplaza_code TEXT,
            Type_of_Payment_code TEXT,
            Vehicle_Code TEXT
        )
    """).close(),
    dag=dag
)

# Create a task to unzip data
unzip_data = PythonOperator(
    task_id='unzip_data',
    python_callable=unzip_tolldata,
    op_args=[source, DESTINATION],
    dag=dag
)

# Create a task to extract data from csv file
extract_data_from_csv = PythonOperator(
    task_id='extract_data_from_csv',
    python_callable=extract_csv_data,
    op_args=[vehicle_data, csv_data],
    dag=dag
)

# Create a task to extract data from tsv file
extract_data_from_tsv = PythonOperator(
    task_id='extract_data_from_tsv',
    python_callable=extract_tsv_data,
    op_args=[tollplaza_data, tsv_data],
    dag=dag
)

# Create a task to extract data from fixed width file
extract_data_from_fixed_width = PythonOperator(
    task_id='extract_data_from_fixed_width',
    python_callable=extract_fixed_width_data,
    op_args=[payment_data, fixed_width_data],
    dag=dag
)

# Create a task to consolidate data extracted from previous tasks
consolidate_data = PythonOperator(
    task_id='consolidate_data',
    python_callable=consolidate_data_extracted,
    op_args=[[csv_data, tsv_data, fixed_width_data], extracted_data],
    dag=dag
)

# Transform and load the data
transform_data = PythonOperator(
    task_id="transform_data",
    python_callable=transform_load_data,
    op_args=[extracted_data, transformed_data],
    dag=dag
)

load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_data_to_sqlite,
    op_args=[transformed_data, db_name],
    dag=dag
)

query_data = PythonOperator(
    task_id='query_data',
    python_callable=query_and_print_data,
    op_args=[db_name],
    dag=dag
)

# Define the task pipeline
create_table >> unzip_data >> [extract_data_from_csv, extract_data_from_tsv, extract_data_from_fixed_width] >> consolidate_data >> transform_data >> load_data >> query_data
