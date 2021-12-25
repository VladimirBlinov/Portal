from finam import Exporter
import json
import logging
import os
import psycopg2
from dotenv import load_dotenv
import pandas as pd


load_dotenv(r'D:\Django\portal\invportal\docker\airflow\database.env')
file = r'\\wsl$\docker-desktop-data\version-pack-data\community\docker\volumes\airflow_tmp\_data\instruments.csv'

POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_DB = os.getenv('POSTGRES_DB')
LOCAL_POSTGRES_HOST = os.getenv('LOCAL_POSTGRES_HOST')
LOCAL_POSTGRES_PORT = os.getenv('LOCAL_POSTGRES_PORT')


def get_instruments(market=25):
    exporter = Exporter()
    market_instruments = exporter.lookup(market=market)
    market_instruments.to_csv('instr.csv', index_label=False, header=False, sep=';')
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
        return None


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
        return None

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
    query = """SELECT "InstrumentID", "Instrument", "Ticker", "MarketplaceID"
                                FROM public."Instrument";"""
    extracted_df = get_extracted(file)
    # reference_df = get_df_from_db(query)
    reference_df = pd.DataFrame()
    cursor = connect_db(query)
    for row in cursor:
        reference_df = reference_df.append(pd.Series(row), ignore_index=True)
    transformed_df = compare_df(reference_df, extracted_df)
    print(transformed_df)


