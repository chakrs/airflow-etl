#!/bin/bash

# Initialize Airflow database if not already initialized
if [ ! -f "$AIRFLOW_HOME/airflow.db" ]; then
    echo "Initializing Airflow DB..."
    airflow db init
fi

# Create Admin User if not exists
airflow users list | grep "admin" || \
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# Start Airflow Webserver & Scheduler
airflow webserver -p 8080 & airflow scheduler
