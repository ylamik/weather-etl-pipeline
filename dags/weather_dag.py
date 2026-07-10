from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests

# ==========================================
# SECTION 1: Configuration & Static Data
# ==========================================

# Complete list of NCR Cities and Municipality (Pateros) with coordinates
NCR_CITIES = {
    "Manila": {"lat": 14.5995, "lon": 120.9842},
    "Quezon City": {"lat": 14.6760, "lon": 121.0437},
    "Caloocan": {"lat": 14.6488, "lon": 120.9713},
    "Las Piñas": {"lat": 14.4445, "lon": 120.9939},
    "Makati": {"lat": 14.5547, "lon": 121.0244},
    "Malabon": {"lat": 14.6621, "lon": 120.9570},
    "Mandaluyong": {"lat": 14.5794, "lon": 121.0359},
    "Marikina": {"lat": 14.6507, "lon": 121.1029},
    "Muntinlupa": {"lat": 14.3907, "lon": 121.0450},
    "Navotas": {"lat": 14.6666, "lon": 120.9427},
    "Parañaque": {"lat": 14.4793, "lon": 121.0198},
    "Pasay": {"lat": 14.5378, "lon": 121.0014},
    "Pasig": {"lat": 14.5764, "lon": 121.0851},
    "San Juan": {"lat": 14.6042, "lon": 121.0300},
    "Taguig": {"lat": 14.5176, "lon": 121.0509},
    "Valenzuela": {"lat": 14.7011, "lon": 120.9830},
    "Pateros": {"lat": 14.5454, "lon": 121.0687}
}

# ==========================================
# SECTION 2: Extract and Transform Task
# ==========================================

def extract_and_transform_weather(**kwargs):
    """
    Loops through the NCR dictionary, extracts live data from Open-Meteo, 
    transforms it into a structured dictionary, and prepares it for loading.
    """
    all_city_data = []
    
    for city, coords in NCR_CITIES.items():
        lat = coords["lat"]
        lon = coords["lon"]
        
        # Open-Meteo API URL strictly formatted for this specific location
        api_url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&daily=temperature_2m_max,temperature_2m_min&timezone=Asia%2FSingapore"
        
        response = requests.get(api_url)
        data = response.json()
        
        # Isolate the required metrics
        daily_record = {
            "location_name": city,
            "latitude": lat,
            "longitude": lon,
            "record_date": data['daily']['time'][0],
            "max_temp_celsius": data['daily']['temperature_2m_max'][0],
            "min_temp_celsius": data['daily']['temperature_2m_min'][0]
        }
        
        all_city_data.append(daily_record)
        
    # Push the transformed list of 17 cities to XCom so the load task can access it
    kwargs['ti'].xcom_push(key='weather_data', value=all_city_data)

# ==========================================
# SECTION 3: Load Task
# ==========================================

def load_weather_to_postgres(**kwargs):
    """
    Pulls the transformed data from XCom and executes an executemany 
    SQL insertion into the PostgreSQL data warehouse.
    """
    # Pull data from the previous task
    weather_data = kwargs['ti'].xcom_pull(key='weather_data', task_ids='extract_transform_task')
    
    # Establish connection using Airflow's built-in PostgresHook
    # Note: Make sure your Airflow connection ID is named 'postgres_default' in the UI
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Prepare the SQL query map
    insert_query = """
        INSERT INTO daily_temperatures 
        (location_name, latitude, longitude, record_date, max_temp_celsius, min_temp_celsius)
        VALUES (%s, %s, %s, %s, %s, %s);
    """
    
    # Convert list of dictionaries into a list of tuples for psycopg2 insertion
    records_to_insert = [
        (
            record['location_name'], 
            record['latitude'], 
            record['longitude'], 
            record['record_date'], 
            record['max_temp_celsius'], 
            record['min_temp_celsius']
        )
        for record in weather_data
    ]
    
    # Execute batch insert for optimal performance
    pg_hook.insert_rows(
        table="daily_temperatures",
        rows=records_to_insert,
        target_fields=["location_name", "latitude", "longitude", "record_date", "max_temp_celsius", "min_temp_celsius"],
        replace=False
    )

# ==========================================
# SECTION 4: DAG Definition & Orchestration
# ==========================================

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ncr_weather_etl_pipeline',
    default_args=default_args,
    description='Extracts weather for NCR cities and loads to Postgres',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Define Tasks
    extract_transform_task = PythonOperator(
        task_id='extract_transform_task',
        python_callable=extract_and_transform_weather,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load_weather_to_postgres,
        provide_context=True
    )

    # Define Workflow Dependencies
    extract_transform_task >> load_task