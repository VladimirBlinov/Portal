from finam import Exporter, Market, LookupComparator, Timeframe
import logging
import os
import psycopg2
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime, timedelta, date
from plugins.etlutils.insrtuments_helper import list_to_csv_as_row, get_extracted

load_dotenv(r'D:\Django\portal\invportal\docker\airflow\database.env')
extracted_file_path = r'\\wsl$\docker-desktop-data\version-pack-data\community\docker\volumes\airflow_tmp\_data\price_uploader_daily_dag\extracted_price_uploader_daily_dag_20211227.csv'

POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_DB = os.getenv('POSTGRES_DB')
LOCAL_POSTGRES_HOST = os.getenv('LOCAL_POSTGRES_HOST')
LOCAL_POSTGRES_PORT = os.getenv('LOCAL_POSTGRES_PORT')


def get_instruments(market=25):
    exporter = Exporter()
    market_instruments = exporter.lookup(market=market)
    return market_instruments


def connect_db(query, params, dbname=POSTGRES_DB, user_name=POSTGRES_USER, pwd=POSTGRES_PASSWORD, host=LOCAL_POSTGRES_HOST,
               port=LOCAL_POSTGRES_PORT):
    conn = psycopg2.connect(dbname=dbname, user=user_name,
                            password=pwd, host=host, port=port)
    cursor = conn.cursor()
    try:
        cursor.execute(query, params)
        return cursor
    except Exception as e:
        print(f'Exception: {e}')
        cursor.close()
        conn.close()


def get_df_from_db(query, dbname=POSTGRES_DB, user_name=POSTGRES_USER, pwd=POSTGRES_PASSWORD,
                   host=LOCAL_POSTGRES_HOST, port=LOCAL_POSTGRES_PORT):
    conn = psycopg2.connect(dbname=dbname, user=user_name,
                            password=pwd, host=host, port=port)
    try:
        df = pd.read_sql(query, con=conn)
        return df
    except Exception as e:
        print('Exception:', e)
        conn.close()


def compare_df(ref_df, new_df):
    set_ref = set(ref_df.iloc[:, 2])
    set_new = set(new_df.iloc[:, 2])
    set_diff = set_new - set_ref
    diff_df = new_df[new_df.iloc[:, 2].isin(set_diff)]
    logging.info(f'{diff_df}')
    return diff_df


def get_daily_data(row, start_time, end_time):
    TIMEFRAME_ID = 7
    ticker_data = pd.DataFrame()
    instrument_id = row[0]
    marketplace = row[3]
    exporter = Exporter()
    try:
        ticker_data = exporter.download(instrument_id, market=Market.USA, start_date=start_time,
                                        end_date=end_time, timeframe=Timeframe.DAILY)
    except Exception as e:
        logging.info('Exception:', e)

    if not ticker_data.empty:
        date_time = ticker_data.iloc[0, 0]
        open = ticker_data.iloc[0, 2]
        high = ticker_data.iloc[0, 3]
        low = ticker_data.iloc[0, 4]
        close = ticker_data.iloc[0, 5]
        volume = ticker_data.iloc[0, 6]
        return instrument_id, open, high, low, close, volume, date_time, TIMEFRAME_ID, marketplace


if __name__ == '__main__':
    extracted_df = get_extracted(extracted_file_path)
    for idx in range(extracted_df.shape[0]):
        instrument_id, open, high, low, close, volume, date_time, timeframe_id, marketplace = extracted_df.iloc[idx, :].tolist()
        instrument_id = int(instrument_id)
        volume = int(volume)
        date_time = datetime.strptime(str(int(date_time)), '%Y%m%d')
        timeframe_id = int(timeframe_id)
        marketplace = int(marketplace)

        sql = """SELECT EXISTS (SELECT "InstrumentPriceID" FROM public."InstrumentPrice"
                                            WHERE "InstrumentID" = %s AND "DateTime" = %s AND "TimeFrameID" = %s 
                                            AND "MarketPlaceID" = %s);"""
        params = (instrument_id, date_time, timeframe_id, marketplace)
        try:
            cursor = connect_db(sql, params)
            id_exists = cursor.fetchone()[0]
        except Exception as e:
            logging.info('Exception:', e)
            id_exists = None
        print(f'{*params,}')

