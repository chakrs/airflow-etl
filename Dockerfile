# Use official Python image
FROM python:3.10-slim

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow
ENV DATA_DIR=/opt/airflow/data
ENV AIRFLOW__CORE__DISABLE_WARNINGS=True
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__SQL_ALCHEMY_WARN_0=False
ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True

# Install Airflow, pandas, pendulum, and compatible versions of MarkupSafe and Jinja2
RUN pip install --no-cache-dir apache-airflow \
    pandas \
    pendulum \
    markupsafe \
    jinja2 \
    apache-airflow-providers-sendgrid

# Create directories for airflow_home, data, and logs
RUN mkdir -p $AIRFLOW_HOME/dags $AIRFLOW_HOME/logs $AIRFLOW_HOME/plugins $DATA_DIR

# Set working directory
WORKDIR $AIRFLOW_HOME

# Copy DAGs and Data from host machine to container
COPY airflow_home/dags/ $AIRFLOW_HOME/dags/
COPY airflow_home/data/ $DATA_DIR/

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Expose Airflow webserver port
EXPOSE 8080

ENTRYPOINT ["/entrypoint.sh"]
