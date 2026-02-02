import requests
import psycopg2

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

def pre_staging():
    hook = PostgresHook(postgres_conn_id='postgres-dwh', database='dwh')
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE SCHEMA IF NOT EXISTS staging;

    DROP TABLE IF EXISTS staging.securities;

    CREATE TABLE IF NOT EXISTS staging.securities (
        "code" varchar(10) NOT NULL, 
        "open" numeric(25) NULL,
        "close" numeric(25) NULL,
        "high" numeric(25) NULL,
        "low" numeric(25) NULL,
        "value" numeric(25) NULL,
        "volume" numeric(25) NULL,
        "begin" timestamp NULL,
        "end" timestamp NULL
    );""")
    conn.commit()

    cur.close()
    conn.close()



def get_sber_and_imoex_info_task(start_date, end_date):
    # TODO: 
    # - Separate functions for each security call
    URL_SBER  = f'https://iss.moex.com/iss/engines/stock/markets/shares/securities/SBER/candles.json?from={start_date}&till={end_date}&interval=24&start=0'
    URL_IMOEX = f'https://iss.moex.com/iss/engines/stock/markets/index/boards/MOEX/securities/IMOEX2/candles.json?from={start_date}&to={end_date}&interval=24&start=0'

    hook = PostgresHook(postgres_conn_id='postgres-dwh', database='dwh')
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute('TRUNCATE TABLE staging.securities;')

    # SBER loading
    data = requests.get(URL_SBER)
    column_names = ['code'] + data.json().get('candles').get('columns')

    for row_data in data.json().get('candles').get('data'):
        sql_header = f'INSERT INTO staging.securities({', '.join(["\""+str(column_name)+"\"" for column_name in column_names])})'
        sql_values = f'VALUES ({', '.join(["\'"+str(column_data)+"\'" for column_data in ["SBER"] + row_data])});'
        sql = sql_header + '\n' + sql_values

        cur.execute(sql)
        conn.commit()

    # IMOEX loading
    data = requests.get(URL_IMOEX)
    column_names = ['code'] + data.json().get('candles').get('columns')

    for row_data in data.json().get('candles').get('data'):
        sql_header = f'INSERT INTO staging.securities({', '.join(["\""+str(column_name)+"\"" for column_name in column_names])})'
        sql_values = f'VALUES ({', '.join(["\'"+str(column_data)+"\'" for column_data in ["IMOEX"] + row_data])});'
        sql = sql_header + '\n' + sql_values

        cur.execute(sql)
        conn.commit()

    cur.close()
    conn.close()



with DAG(
    default_args={"owner": "airflow"},
    dag_id="get_securities_info_into_staging",
    schedule="@once",
) as dag:
    pre_staging = PythonOperator(task_id="pre_staging", python_callable=pre_staging)
    get_sber_and_imoex_info_task = PythonOperator(task_id="get_sber_and_imoex_info_task", python_callable=get_sber_and_imoex_info_task, op_kwargs={"start_date": "2025-01-01", "end_date": "2025-12-31"})

    pre_staging >> get_sber_and_imoex_info_task
