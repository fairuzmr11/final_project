from datetime import datetime, timedelta
import requests
# import subprocess
# import json 
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import psycopg2 as pg
# from partialjson.json_parser import JSONParser

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

def mysql_connection():
    try:
        username = 'root'
        password = 'Pass@981!'
        host = '172.18.0.1' #'172.18.0.1'
        port = '3307'
        database = 'mysql-data-staging'

        engine = create_engine(f'mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database}')
        print('Connect Engine MySQL')
        return engine
    except Exception as e:
        print(f'Error connecting to MySQL: {str(e)}')
        return None  # Return None if the connection fails

def postgres_connection():
    try:
        host = "172.18.0.1"
        port = "5434"
        database = "postgres-dwh"
        user = "fai-project-6"
        password = "Pass@981!"
        conn = pg.connect(dbname=database, user=user, host=host, port=port, password=password)
        print('Connect PostgreSQL using psycopg2')
        return conn
    except pg.OperationalError as e:
        print(f'Error connecting to PostgreSQL: {str(e)}')
        return None  # Return None if the connection fails

def api_url_request():
    try:
        mysql_engine = mysql_connection()
        # postgres_conn = postgres_connection()

        if mysql_engine:
            api_url = "http://103.150.197.96:5005/api/v1/rekapitulasi_v2/jabar/harian?level=kab%22"
            response = requests.get(api_url)

            if response.status_code == 200:
                data = response.json()
                raw_content = data['data']['content']
                df = pd.DataFrame(raw_content)

                # Insert into MySQL
                df.to_sql(name='raw_covid_data', con=mysql_engine, if_exists='replace', index=False)
                print('DataFrame inserted into MySQL')

                return True
            else:
                print(f"Error: {response.status_code}")
        else:
            print("MySQL connection failed.")
            return False
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")

def generate_data():
    engine = mysql_connection()

    try:
        # Create a connection
        conn = engine.connect()

        # Execute the query
        result = conn.execute("SELECT * FROM raw_covid_data")

        # Fetch the result if needed
        rows = result.fetchall()
        # print(rows)
        df = pd.DataFrame(rows, columns=result.keys())

        # Print the DataFrame
        print(df.head())
        # (key='dataset_files', value=list_name)
        df.to_csv('dags/sql/raw_data.csv', index=False)
        return True
    except Exception as e:
        print(f"Error executing query: {str(e)}")
    finally:
        # Close the connection
        conn.close()

def insert_into_postgres(conn, table_name, df):
    cursor = conn.cursor()
    for _, row in df.iterrows():
        cursor.execute(f"INSERT INTO {table_name} VALUES {tuple(row)} ON CONFLICT DO NOTHING")
    conn.commit()

def insert_into_mysql(engine, table_name, df):
    with engine.connect() as conn:
        for _, row in df.iterrows():
            conn.execute(f"INSERT INTO {table_name} VALUES {tuple(row)}")

def dim_table_creation():
    df = pd.read_csv('dags/sql/raw_data.csv')
    
    # province

    df_province = df[['kode_prov','nama_prov']].drop_duplicates()
    print(df_province)

    # district
    df_district = df[['kode_kab', 'kode_prov', 'nama_kab']].drop_duplicates()
    df_district = df_district.sort_values(by='kode_kab')
    df_district = df_district.reset_index(drop=True)

    print(df_district.head(1))

    # case table

    df_column = df.columns.to_list()
    df_column = [item for item in df_column if 'suspect' in item.lower() or 'closecontact' in item.lower() or 'probable' in item.lower()]
    df_column = df_column [3:]
    df_column = pd.DataFrame(df_column, columns=['status_name'])
    df_split = df_column['status_name'].str.split('_', n=1,expand=True)
    df_split['status_detail'] = df_split[0] + '_' + df_split[1]
    df_split = df_split.rename(columns= {0 : 'status_name'})
    df_split.insert(0,'case_id',np.arange(1, df_split.shape[0]+1))
    df_split = df_split[['case_id','status_name','status_detail']]
    print(df_split.head())

    # save to pdf
    df_province.to_csv('dags/sql/df_province.csv',index=False)
    df_district.to_csv('dags/sql/df_district.csv',index=False)
    df_split.to_csv('dags/sql/df_case.csv', index=False)

    # Insert into PostgreSQL
    conn_postgres = postgres_connection()
    if conn_postgres:
        insert_into_postgres(conn_postgres, 'province_table', df_province)
        insert_into_postgres(conn_postgres, 'district_table', df_district)
        insert_into_postgres(conn_postgres, 'case_table', df_split)

    # Insert into MySQL
    engine_mysql = mysql_connection()
    if engine_mysql:
        # insert_into_mysql(engine_mysql, 'province_table', df_province)
        # insert_into_mysql(engine_mysql, 'district_table', df_district)
        insert_into_mysql(engine_mysql, 'case_table', df_split)

def aggregation_prov():
    df = pd.read_csv('dags/sql/raw_data.csv')
    df_split = pd.read_csv('dags/sql/df_case.csv')

    # df cleaning
    df['clean_date'] = pd.to_datetime(df['tanggal'])
    df['year_month'] = df['clean_date'].dt.strftime('%Y-%m')
    df['year'] = df['clean_date'].dt.strftime('%Y')
    df = df.drop(columns='clean_date')

    # df hari melting
    df_harian = df.melt(id_vars=['tanggal', 'kode_prov'], var_name='status', value_name='total').sort_values(['tanggal', 'kode_prov', 'status'])
    df_harian = df_harian.reset_index(drop=True)
    df_harian = df_harian.rename(columns={'status':'status_detail'})

    # df hari merge & groupings
    df_harian_merge = pd.merge(df_harian,df_split, how='inner', on='status_detail')
    df_harian_merge = df_harian_merge[['kode_prov','case_id','tanggal','total']]
    df_harian_merge = df_harian_merge.groupby(by=['kode_prov','case_id','tanggal']).sum()
    df_harian_merge = df_harian_merge.reset_index()
    df_harian_merge.insert(0,'id',np.arange(1, df_harian_merge.shape[0]+1))
    print(df_harian_merge.head(1))
    
    #  df bulan melting
    df_bulan = df.melt(id_vars=['year_month', 'kode_prov'], var_name='status', value_name='total').sort_values(['year_month', 'kode_prov', 'status'])
    df_bulan = df_bulan.reset_index(drop=True)
    df_bulan = df_bulan.rename(columns={'status':'status_detail'})

    # df bulan merge & groupings
    df_bulan_merge = pd.merge(df_bulan, df_split, how='inner', on='status_detail')
    df_bulan_merge = df_bulan_merge[['kode_prov','case_id','year_month','total']]
    df_bulan_merge = df_bulan_merge.groupby(by=['kode_prov','case_id','year_month']).sum()
    df_bulan_merge = df_bulan_merge.reset_index()
    df_bulan_merge.insert(0,'id',np.arange(1, df_bulan_merge.shape[0]+1))
    print(df_bulan_merge.head(1))
    
    # df tahun melting
    df_tahun = df.melt(id_vars=['year', 'kode_prov'], var_name='status', value_name='total').sort_values(['year', 'kode_prov', 'status'])
    df_tahun = df_tahun.reset_index(drop=True)
    df_tahun = df_tahun.rename(columns={'status' : 'status_detail'})

    # df tahun merge & groupings
    df_tahun_merge = pd.merge(df_tahun, df_split, how='inner', on='status_detail')
    df_tahun_merge = df_tahun_merge[['kode_prov','case_id','year','total']]
    df_tahun_merge = df_tahun_merge.groupby(by=['kode_prov','case_id','year']).sum()
    df_tahun_merge = df_tahun_merge.reset_index()
    df_tahun_merge.insert(1, 'id',df_tahun_merge.shape[0]+1)
    print(df_tahun_merge.head(1))

    # Insert into PostgreSQL
    conn_postgres = postgres_connection()
    if conn_postgres:
        insert_into_postgres(conn_postgres, 'daily_province', df_harian_merge)
        insert_into_postgres(conn_postgres, 'monthly_province', df_bulan_merge)
        insert_into_postgres(conn_postgres, 'yearly_province', df_tahun_merge)

def aggregation_district():

    df = pd.read_csv('dags/sql/raw_data.csv')
    df_split = pd.read_csv('dags/sql/df_case.csv')

    # df cleaning
    df['clean_date'] = pd.to_datetime(df['tanggal'])
    df['year_month'] = df['clean_date'].dt.strftime('%Y-%m')
    df['year'] = df['clean_date'].dt.strftime('%Y')
    df = df.drop(columns='clean_date')


    # district monthly
    district_monthly = df.melt(id_vars=['year_month', 'kode_kab'], var_name='status', value_name='total').sort_values(['year_month', 'kode_kab', 'status'])
    district_monthly = district_monthly.rename(columns={'status':'status_detail'})

    district_monthly_merge = pd.merge(district_monthly,df_split, how='inner', on='status_detail')
    district_monthly_merge = district_monthly_merge[['kode_kab','case_id','year_month','total']]
    district_monthly_merge = district_monthly_merge.groupby(by=['kode_kab','case_id','year_month']).sum()
    district_monthly_merge = district_monthly_merge.reset_index()
    district_monthly_merge.insert(0,'id',np.arange(1, district_monthly_merge.shape[0]+1))
    print(district_monthly_merge.head(1))
    print(district_monthly_merge['total'].unique())

    # district yearly
    district_yearly = df.melt(id_vars=['year', 'kode_kab'], var_name='status', value_name='total').sort_values(['year', 'kode_kab', 'status'])
    district_yearly = district_yearly.rename(columns={'status':'status_detail'})

    district_yearly_merge = pd.merge(district_yearly,df_split, how='inner', on='status_detail')
    district_yearly_merge = district_yearly_merge[['kode_kab','case_id','year','total']]
    district_yearly_merge = district_yearly_merge.groupby(by=['kode_kab','case_id','year']).sum()
    district_yearly_merge = district_yearly_merge.reset_index()
    district_yearly_merge.insert(0,'id',np.arange(1, district_yearly_merge.shape[0]+1))
    print(district_yearly_merge.head(1))

    # Insert into PostgreSQL
    conn_postgres = postgres_connection()
    print(conn_postgres)
    if conn_postgres:
        insert_into_postgres(conn_postgres, 'monthly_district', district_monthly_merge)
        insert_into_postgres(conn_postgres, 'yearly_district', district_yearly_merge)


# DAG definition
with DAG(
    dag_id='project_6_30',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(seconds=10)
    }
) as dag:
    
    start = EmptyOperator(task_id='start')

    split_task = EmptyOperator(task_id='split_task')

    api_url_request_task = PythonOperator(
        task_id='api_url_request',
        python_callable=api_url_request,
        provide_context=True,
    )

    dim_postgres = PostgresOperator (
        task_id = 'dim_postgres',
        postgres_conn_id = 'postgres_dwh',
        sql = 'sql/dim_postgres.sql'
    )

    dim_mysql = MySqlOperator(
        task_id='dim_mysql',
        mysql_conn_id='mysql_data_staging',
        sql='sql/dim_mysql.sql'
    )

    generate_data = PythonOperator (
        task_id = 'generate_data',
        python_callable= generate_data,
        provide_context=True,
    )

    dim_table_creation = PythonOperator (
        task_id = 'dim_table_creation',
        python_callable = dim_table_creation,
        provide_context = True
    )

    split_task_2 = EmptyOperator(task_id='split_task_2')

    aggregation_prov = PythonOperator (
        task_id = 'aggregation_prov',
        python_callable= aggregation_prov,
        provide_context = True
    )

    aggregation_district = PythonOperator (
        task_id = 'aggregation_district',
        python_callable = aggregation_district,
        provide_context = True
    )

    end = EmptyOperator(task_id = 'end')
    
    start >> api_url_request_task >> split_task >> [dim_postgres, generate_data]
    generate_data >> dim_table_creation
    dim_postgres >> dim_mysql
    [dim_mysql, dim_table_creation] >> split_task_2 >> [aggregation_prov, aggregation_district] >> end
    

