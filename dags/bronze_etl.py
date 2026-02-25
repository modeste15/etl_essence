from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import gzip
import io
import psycopg2
import os

DB_HOST = "postgres"
DB_NAME = "airflow"
DB_USER = "airflow"
DB_PASSWORD = "airflow"
DB_PORT = 5432

def download_and_store():
    url = "https://donnees.roulez-eco.fr/opendata/instantane"
    
    response = requests.get(url)
    response.raise_for_status()
    
    if url.endswith(".gz"):
        with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f:
            xml_content = f.read().decode("utf-8")
    else:
        xml_content = response.text
    
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )
    cur = conn.cursor()
    cur.execute("INSERT INTO bronze_traffic (data) VALUES (%s)", (xml_content,))
    conn.commit()
    cur.close()
    conn.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'bronze_etl',
    default_args=default_args,
    schedule_interval='*/30 * * * *',  
    catchup=False
) as dag:
    task_download = PythonOperator(
        task_id='download_and_store',
        python_callable=download_and_store
    )