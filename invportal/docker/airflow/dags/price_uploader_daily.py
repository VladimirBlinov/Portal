from datetime import datetime, timedelta
import logging
import os
import pandas as pd
from finam import Exporter, Market, Timeframe

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

from etlutils.insrtuments_helper import get_hourly_data

DEFAULT_ARGS = {
    'owner': 'airflow',
}

DAG_ID = 'price_uploader_daily_dag'
TAG = 'price_uploader_daily'
TEMP_DIR = '/tmp'
WORK_DIR = os.path.join(TEMP_DIR, DAG_ID)
if not os.path.exists(WORK_DIR):
    os.mkdir(WORK_DIR)
EXTRACTED_FILE_PATH = os.path.join(WORK_DIR, f'extracted_{DAG_ID}.csv')
TRANSFORMED_FILE_PATH = os.path.join(WORK_DIR, f'transformed_{DAG_ID}.csv')
TIMEFRAME_ID = 7

with DAG(
        DAG_ID,
        default_args=DEFAULT_ARGS,
        schedule_interval='@daily',
        start_date=datetime(2021, 12, 24),
        catchup=False,
        tags=[TAG]) as dag:

    def extract():
        start_time = datetime.now().date() - timedelta(days=1)
        pg_hook = PostgresHook('airflow_database')
        sql = """SELECT "InstrumentID", "Instrument", "Ticker", "MarketplaceID" FROM public."Instrument";"""
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute(sql)
                except Exception as e:
                    logging.info('Exception:', e)
        if cursor.rowcount > 0:
            for row in cursor:
                try:
                    get_hourly_data(row, start_time)
                except Exception as e:
                    logging.info('Exception:', e)


    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract,
    )





        instruments = get_instruments()
        logging.info(EXTRACTED_FILE_PATH)
        instruments.to_csv(EXTRACTED_FILE_PATH, index_label=False, header=False, sep=';', mode='w')


    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract,
    )


    def transform():
        pg_hook = PostgresHook('airflow_database')
        reference_df = pd.DataFrame()
        transformed_df = pd.DataFrame()
        extracted_df = get_extracted(EXTRACTED_FILE_PATH)
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute("""SELECT "InstrumentID", "Instrument", "Ticker", "MarketplaceID"
                                FROM public."Instrument";""")
                    for row in cursor:
                        reference_df = reference_df.append(pd.Series(row), ignore_index=True)
                    if not reference_df.empty:
                        transformed_df = compare_df(reference_df, extracted_df)
                    else:
                        transformed_df = extracted_df
                    transformed_df.index = transformed_df.iloc[:, 0]
                    transformed_df = transformed_df.drop([0], axis=1)
                    logging.info(TRANSFORMED_FILE_PATH)
                    logging.info(transformed_df)
                    transformed_df.to_csv(TRANSFORMED_FILE_PATH, index_label=False, header=False, sep=';', mode='w')
                except Exception as e:
                    logging.info('Exception:', e)


    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform
    )

    load_task = PostgresOperator(
        task_id='load_task',
        postgres_conn_id='airflow_database',
        sql="sql/load_instruments.sql",
        # sql=f"""COPY public."Instrument" FROM '{TRANSFORMED_FILE_PATH}' DELIMITER ';'
        #     """,
        params={"path": TRANSFORMED_FILE_PATH}
    )

    extract_task >> transform_task >> load_task
    # extract_task >> load_task >> pg_load
