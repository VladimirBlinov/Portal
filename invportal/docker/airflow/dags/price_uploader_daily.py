from datetime import datetime, timedelta, date
import logging
import os
import pandas as pd

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

from etlutils.insrtuments_helper import get_daily_data, list_to_csv_as_row, get_extracted

DEFAULT_ARGS = {
    'owner': 'airflow',
}

DAG_ID = 'price_uploader_daily_dag'
TAG = 'price_uploader_daily'
TEMP_DIR = '/tmp'
WORK_DIR = os.path.join(TEMP_DIR, DAG_ID)
if not os.path.exists(WORK_DIR):
    os.mkdir(WORK_DIR)
TRANSFORMED_FILE_PATH = os.path.join(WORK_DIR, f'transformed_{DAG_ID}_{date.today() - timedelta(days=1)}.csv')
DATE_FORMAT = "%Y%m%d"

with DAG(
        DAG_ID,
        default_args=DEFAULT_ARGS,
        schedule_interval='@daily',
        start_date=datetime(2021, 12, 27),
        catchup=True,
        tags=[TAG]) as dag:

    def extract(**kwargs):
        logging.info('start')
        logging.info(f'{kwargs}')
        execution_date = kwargs['data_interval_start']
        logging.info(f'execution_date: {execution_date}')
        logging.info(f'execution_date: {execution_date.strftime(DATE_FORMAT)}')
        extracted_file_path = os.path.join(WORK_DIR, f'extracted_{DAG_ID}_{execution_date.strftime(DATE_FORMAT)}.csv')
        logging.info(f'extracted_file_path: {extracted_file_path}')
        open(extracted_file_path, 'w').close()
        start_time = execution_date
        end_time = start_time + timedelta(days=1)
        logging.info(f'start_time, end_time: {start_time} {end_time}')
        pg_hook = PostgresHook('airflow_database')
        sql = """SELECT "InstrumentID", "Instrument", "Ticker", "MarketplaceID" FROM public."Instrument";"""
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute(sql)
                    if cursor.rowcount > 0:
                        for row in cursor:
                            try:
                                instrument_data = get_daily_data(row, start_time, end_time)
                                if instrument_data:
                                    list_to_csv_as_row(extracted_file_path, instrument_data)
                            except Exception as e:
                                logging.info('Exception:', e)
                except Exception as e:
                    logging.info('Exception:', e)
        ti = kwargs['ti']
        ti.xcom_push(value=extracted_file_path, key='extracted_file_path')
        ti.xcom_push(value=execution_date, key='execution_date')


    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract,
        provide_context=True
    )


    def load(**kwargs):
        ti = kwargs['ti']
        extracted_file_path = ti.xcom_pull(key='extracted_file_path', task_ids='extract_task')
        execution_date = ti.xcom_pull(key='execution_date', task_ids='extract_task')
        logging.info(f'execution_date: {execution_date.strftime(DATE_FORMAT)}')
        logging.info(f'extracted_file_path: {extracted_file_path}')
        extracted_df = get_extracted(extracted_file_path)
        for idx in range(extracted_df.shape[0]):
            instrument_id = extracted_df.iloc[idx, 0]
            open = extracted_df.iloc[idx, 1]
            high = extracted_df.iloc[idx, 2]
            low  = extracted_df.iloc[idx, 3]
            close, volume, date_time, timeframe_id, marketplace
            pg_hook = PostgresHook('airflow_database')
            with pg_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    try:
                        sql = """IF EXISTS (SELECT "InstrumentPriceID" FROM public."InstrumentPrice"
                                    WHERE "InstrumentID" = ? AND "DateTime" = ? AND "TimeFrameID" = ? 
                                    AND "MarketplaceID" = ?);"""
                        params =
                        cursor.execute()
                        # "InstrumentPriceID", "InstrumentID", "Open", "High", "Low", "Close", "Volume", "DateTime", "TimeFrameID"
                        for row in cursor:
                            reference_df = reference_df.append(pd.Series(row), ignore_index=True)

                    except Exception as e:
                        logging.info('Exception:', e)


    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load,
        provide_context=True
    )

    #
    # load_task = PostgresOperator(
    #     task_id='load_task',
    #     postgres_conn_id='airflow_database',
    #     sql="sql/load_instruments.sql",
    #     # sql=f"""COPY public."Instrument" FROM '{TRANSFORMED_FILE_PATH}' DELIMITER ';'
    #     #     """,
    #     params={"path": TRANSFORMED_FILE_PATH}
    # )

    # extract_task >> transform_task >> load_task
    extract_task
