from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import variable
from sqlalchemy import create_engine
import psycopg2 as pg

from psycopg2.extras import execute_values

def mysql_connection():
    try:
        username = 'root'
        password = 'Pass@981!'
        host = '172.18.0.1'
        port = '3307'
        database = 'mysql-data-staging'

        engine = create_engine(f'mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database}')
        engine_conn = engine.connect()
        print('Connect Engine MySQL')
        return True  # Return True if connection is successful
    except Exception as e:
        print(f'Error connecting to MySQL: {str(e)}')
        return False
    
def postgres_connection():
    try:
        # connect to server 172.18.0.4
        host = "172.18.0.1"
        port = "5434"
        database = "postgres-dwh"
        user = "fai-project-6"
        password = "Pass@981!"
        conn = None
        curr = None
        setting1 = f"dbname={database} user={user} host={host} port={port} password={password}"

        # Set a connection
        conn = pg.connect(setting1) 
        print('Connect PostgreSQL using psycopg2')
        return True  # Return True if connection is successful
    except pg.OperationalError as e:
        print(f'Error connecting to PostgreSQL: {str(e)}')
        return False
    finally:
        if conn:
            conn.close()


with DAG(
    dag_id='database_connection_testing_mysql_postgres_7',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(seconds=10)
    }
) as dag:
    
    start = EmptyOperator(task_id='start')

    mysql_connection = PythonOperator(
        task_id = 'mysql_conn_check',
        python_callable= mysql_connection,
        execution_timeout=timedelta(seconds=10)
    )

    postgres_connection = PythonOperator(
        task_id = 'postgres_conn_check',
        python_callable = postgres_connection,
        execution_timeout=timedelta(seconds=10)
    )

    end = EmptyOperator(task_id='end')

    start >> [mysql_connection,postgres_connection] >> end