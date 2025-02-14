from datetime import datetime
import pandas as pd
import sqlite3
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable

import logging
import warnings

# Ignore all warnings
warnings.filterwarnings("ignore")

logging.basicConfig(
    level=logging.INFO,  # Set logging level to INFO
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)


def extract_csv_to_xcom(**kwargs):
    # Read CSV files using pandas
    logging.info("Extracting data from CSV files.")

    trips_df = pd.read_csv('/opt/airflow/data/trips.csv')
    stations_df = pd.read_csv('/opt/airflow/data/stations.csv')

    logging.info("Data extraction complete, sent to XCom.")

    # Push data to XCom
    kwargs['ti'].xcom_push(key='trips_df', value=trips_df)
    kwargs['ti'].xcom_push(key='stations_df', value=stations_df)



def transform_data(**kwargs):
    # Retrieve data from XCom
    ti = kwargs['ti']
    trips_df = ti.xcom_pull(key='trips_df', task_ids='extract_task')
    stations_df = ti.xcom_pull(key='stations_df', task_ids='extract_task')

    logging.info("Transforming data!")
    logging.info("Filling missing values")
    # Handle missing values and transformations
    # Fill missing duration values with the mean duration
    mean_duration = trips_df['duration'].mean()
    trips_df['duration'].fillna(mean_duration, inplace=True)

    # Fill missing subscription type with 'Unknown'
    trips_df['sub_type'].fillna('Unknown', inplace=True)

    # Fill missing zip codes with 'Unknown'
    trips_df['zip_code'].fillna('Unknown', inplace=True)

    # Fill missing birth dates with default value 1990
    trips_df['birth_date'].fillna(1990, inplace=True)

    # Fill missing gender with 'Unknown'
    trips_df['gender'].fillna('Unknown', inplace=True)


    # Perform joins
    logging.info("Joining trips and stations data")
    merged_df = pd.merge(trips_df, stations_df, how='inner', left_on='start_station', right_on='id')

    # Push the transformed data to XCom
    ti.xcom_push(key='transformed_df', value=merged_df)

    logging.info("Transformation complete, sent to XCom.")

def load_and_sql_operations(**kwargs):
    # Retrieve transformed data from XCom
    ti = kwargs['ti']
    transformed_df = ti.xcom_pull(key='transformed_df', task_ids='transform_task')

    # Create SQLite database
    logging.info("Creating sqlite database")
    conn = sqlite3.connect("/opt/airflow/trips_and_stations.db")
    cursor = conn.cursor()


    # Create tables
    logging.info("Creating Tables")
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS trips (
            id INTEGER PRIMARY KEY,
            duration INTEGER,
            start_date TEXT,
            start_station INTEGER,
            end_date TEXT,
            end_station INTEGER,
            bike_number INTEGER,
            sub_type TEXT,
            zip_code TEXT,
            birth_date INTEGER,
            gender TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stations (
            id INTEGER PRIMARY KEY,
            station TEXT,
            municipality TEXT,
            lat REAL,
            lng REAL
        )
    ''')

    # Insert data from DataFrame into SQLite
    logging.info("Inseting data into the tables")
    transformed_df.to_sql('trips', conn, if_exists='replace', index=False)
    stations_df = ti.xcom_pull(key='stations_df', task_ids='extract_task')
    stations_df.to_sql('stations', conn, if_exists='replace', index=False)

    # Perform SQL queries
    logging.info("Performing analysis")
    cursor.execute("SELECT MAX(duration) FROM trips")
    longest_trip = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM trips WHERE sub_type = 'Registered'")
    registered_user_count = cursor.fetchone()[0]

    cursor.execute("SELECT AVG(duration) FROM trips")
    avg_duration = cursor.fetchone()[0]

    cursor.execute("SELECT sub_type, AVG(duration) FROM trips GROUP BY sub_type")
    user_type_duration = cursor.fetchall()

    cursor.execute("SELECT bike_number, COUNT(*) FROM trips GROUP BY bike_number ORDER BY COUNT(*) DESC LIMIT 1")
    most_used_bike = cursor.fetchone()[0]

    cursor.execute("SELECT AVG(duration) FROM trips WHERE birth_date < 1995")
    avg_duration_30 = cursor.fetchone()[0]

    cursor.execute('SELECT stations.station, COUNT(*) FROM trips INNER JOIN stations ON trips.start_station = stations.id GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 5')
    frequent_starting_station = cursor.fetchone()

    cursor.execute('SELECT stations.station, COUNT(*) FROM trips INNER JOIN stations ON trips.start_station = stations.id WHERE trips.start_station = trips.end_station GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 5')
    frequent_round_trip_station = cursor.fetchone()

    # Log results
    logging.info("Here are the results")
    logging.info(f"Longest trip duration: {longest_trip}")
    logging.info(f"Number of registered users: {registered_user_count}")
    logging.info(f"Average trip duration: {avg_duration}")
    logging.info(f"User type trip durations: {user_type_duration}")
    logging.info(f"Most used bike number: {most_used_bike}")
    logging.info(f"Average duration of trips for users over 30: {avg_duration_30}")
    logging.info(f"Which station is the most frequent starting point?: {frequent_starting_station}")
    logging.info(f"which stations are most frequently used for round trips?: {frequent_round_trip_station}")

    # Close the connection
    conn.close()

# Define the Airflow DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 29),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'email': ['cshuvro@gmail.com'],
    'catchup': False
}

dag = DAG(
    'etl_airflow_sqlite',
    default_args=default_args,
    description='ETL pipeline for trip and station data',
    schedule_interval=None, #'@daily',
    catchup=False,
)

# Success Email Task
success_send_email = EmailOperator(
    task_id='email_success',
    to='cshuvro@gmail.com',
    subject='ETL Pipeline Success ✅',
    html_content="<h3>The ETL pipeline completed successfully!</h3>",
    trigger_rule=TriggerRule.ALL_SUCCESS,  # Runs only if etl_task succeeds
    dag=dag
)

# Failed Email Task
failed_send_email = EmailOperator(
    task_id='email_failure',
    to='cshuvro@gmail.com',
    subject='ETL Pipeline Failed ❌',
    html_content="<h3>The ETL pipeline has failed!</h3>",
    trigger_rule=TriggerRule.ONE_FAILED,  # Runs if etl_task fails
    dag=dag
)

# Define tasks
extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_csv_to_xcom,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_and_sql_operations_task = PythonOperator(
    task_id='load_and_sql_operations',
    python_callable=load_and_sql_operations,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> load_and_sql_operations_task 
load_and_sql_operations_task >> failed_send_email
load_and_sql_operations_task >> success_send_email