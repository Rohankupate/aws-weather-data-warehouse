from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from datetime import datetime, timedelta
import pandas as pd
import requests

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 14),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'openweather_api_dag',
    default_args=default_args,
    schedule_interval="@once",
    catchup=False
) as dag:
    
    def extract_openweather_data(**kwargs):
        print("Extracting started")
        ti = kwargs['ti']
        api_endpoint = "https://api.openweathermap.org/data/2.5/forecast"
        api_params = {
            "q": "Toronto,Canada",
            "appid": Variable.get("openweather_api_key")
        }
        response = requests.get(api_endpoint, params=api_params)
        data = response.json()
        print(data)
        df = pd.json_normalize(data['list'])
        print(df)
        return df.to_csv(index=False)

    extract_api_data = PythonOperator(
        task_id='extract_api_data',
        python_callable=extract_openweather_data,
        provide_context=True,
    )

    upload_to_s3 = S3CreateObjectOperator(
        task_id="upload_to_S3",
        aws_conn_id='aws_default',
        s3_bucket='etls3glueredshift',
        s3_key='raw/weather_api_data.csv',
        data="{{ ti.xcom_pull(task_ids='extract_api_data') }}",
    )

    extract_api_data >> upload_to_s3