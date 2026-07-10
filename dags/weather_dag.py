from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv('/opt/airflow/.env')

# --- 1. EXTRACT TASK ---
def extract_weather_data(**kwargs):
    print("Extracting data from Open-Meteo API...")
    url = "https://api.open-meteo.com/v1/forecast?latitude=14.6042&longitude=120.9822&daily=temperature_2m_max,temperature_2m_min&timezone=Asia%2FSingapore"
    response = requests.get(url)
    
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception("API Extraction Failed")

# --- 2. TRANSFORM TASK ---
def transform_data(**kwargs):
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(task_ids='extract_weather_data')
    
    daily_data = raw_data.get('daily', {})
    
    transformed_record = {
        'location': 'Manila',
        'date': daily_data.get('time', [])[0],
        'max_temp': daily_data.get('temperature_2m_max', [])[0],
        'min_temp': daily_data.get('temperature_2m_min', [])[0]
    }
    
    return transformed_record

# --- 3. LOAD TASK ---
def load_data(**kwargs):
    ti = kwargs['ti']
    record = ti.xcom_pull(task_ids='transform_data')
    
    conn = psycopg2.connect(
        host="postgres_db",
        port="5432",
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
    cursor = conn.cursor()
    
    insert_query = """
        INSERT INTO daily_temperatures (location_name, record_date, max_temp_celsius, min_temp_celsius)
        VALUES (%s, %s, %s, %s);
    """
    cursor.execute(insert_query, (record['location'], record['date'], record['max_temp'], record['min_temp']))
    conn.commit()
    
    cursor.close()
    conn.close()
    print("Success! Data loaded via Airflow.")

# --- 4. DEFINE THE DAG ---
default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'daily_weather_etl',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily', # Run automatically once a day
    catchup=False
) as dag:

    task_extract = PythonOperator(
        task_id='extract_weather_data',
        python_callable=extract_weather_data,
        provide_context=True
    )

    task_transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True
    )

    task_load = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True
    )

    # Set the pipeline execution order
    task_extract >> task_transform >> task_load