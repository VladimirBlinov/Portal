from finam import Exporter, Market, LookupComparator, Timeframe
import logging
import os
import psycopg2
from dotenv import load_dotenv
import pandas as pd
import datetime
from datetime import timedelta
from csv import writer

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


def list_to_csv_as_row(file_name, list_of_elem):
    # Open file in append mode
    with open(file_name, 'a+', newline='') as write_obj:
        # Create a writer object from csv module
        csv_writer = writer(write_obj)
        # Add contents of list as last row in the csv file
        csv_writer.writerow(list_of_elem)


if __name__ == '__main__':
    TIMEFRAME_ID = 7
    start_time = datetime.datetime.now().date() - timedelta(days=1)
    end_time = start_time + timedelta(days=1)
    print(start_time, end_time)
    exporter = Exporter()
    sql = """SELECT "InstrumentID", "Instrument", "Ticker", "MarketplaceID" FROM public."Instrument";"""
    cursor = connect_db(sql)
    for row in cursor:
        try:
            instrument_data = get_daily_data(row, start_time, end_time)
            print(instrument_data)
        except Exception as e:
            logging.info('Exception:', e)
