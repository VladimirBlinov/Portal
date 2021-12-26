from finam import Exporter, Market, LookupComparator, Timeframe
import logging
import os
import psycopg2
from dotenv import load_dotenv
import pandas as pd

load_dotenv(r'D:\Django\portal\invportal\docker\airflow\database.env')

POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_DB = os.getenv('POSTGRES_DB')
LOCAL_POSTGRES_HOST = os.getenv('LOCAL_POSTGRES_HOST')
LOCAL_POSTGRES_PORT = os.getenv('LOCAL_POSTGRES_PORT')


def get_instruments(market=25):
    exporter = Exporter()
    market_instruments = exporter.lookup(market=market)
    return market_instruments


def connect_db(query, dbname=POSTGRES_DB, user_name=POSTGRES_USER, pwd=POSTGRES_PASSWORD, host=LOCAL_POSTGRES_HOST,
               port=LOCAL_POSTGRES_PORT):
    conn = psycopg2.connect(dbname=dbname, user=user_name,
                            password=pwd, host=host, port=port)
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        return cursor
    except Exception as e:
        print('Exception:', e)
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


def get_df_pg_hook(query, conn_id='airflow_database'):
    pg_hook = PostgresHook(conn_id)
    conn = pg_hook.get_conn()
    try:
        df = pd.read_sql(query, con=conn)
        return df
    except Exception as e:
        print('Exception:', e)
        conn.close()


def get_extracted(file_path):
    df = pd.read_csv(file_path, sep=';', header=None)
    return df


def compare_df(ref_df, new_df):
    set_ref = set(ref_df.iloc[:, 2])
    set_new = set(new_df.iloc[:, 2])
    set_diff = set_new - set_ref
    diff_df = new_df[new_df.iloc[:, 2].isin(set_diff)]
    logging.info(f'{diff_df}')
    return diff_df


if __name__ == '__main__':

    exporter = Exporter()
    sql = """SELECT "InstrumentID", "Instrument", "Ticker", "MarketplaceID" FROM public."Instrument";"""
    cursor = connect_db(sql)
    for row in cursor:
        instrument_id = row[0]
        ticker = row[2]
        marketplace = row[3]
        # ticker_lookup = exporter.lookup(market=marketplace, code=ticker, name_comparator=LookupComparator.CONTAINS)
        ticker_data = exporter.download(instrument_id, market=marketplace, start_date=start_date,
                                        end_date=end_date, timeframe=Timeframe.HOURLY)
        logging.info(ticker_data)
