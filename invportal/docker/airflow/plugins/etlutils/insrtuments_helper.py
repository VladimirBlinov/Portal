from finam import Exporter, Market, LookupComparator, Timeframe
import pandas as pd
import logging


def get_instruments(market=25):
    exporter = Exporter()
    market_instruments = exporter.lookup(market=market)
    return market_instruments


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


def get_hourly_data(row, start_time, target_file):
    TIMEFRAME_ID = 7
    instrument_id = row[0]
    marketplace = row[3]
    exporter = Exporter()
    ticker_data = exporter.download(instrument_id, market=Market.USA, start_date=start_time,
                                    end_date=start_time, timeframe=Timeframe.DAILY)
    if not ticker_data.empty:
        date_time = ticker_data.iloc[0, 0]
        open = ticker_data.iloc[0, 2]
        high = ticker_data.iloc[0, 3]
        low = ticker_data.iloc[0, 4]
        close = ticker_data.iloc[0, 5]
        volume = ticker_data.iloc[0, 6]
        return instrument_id, open, high, low, close, volume, date_time, TIMEFRAME_ID, marketplace
