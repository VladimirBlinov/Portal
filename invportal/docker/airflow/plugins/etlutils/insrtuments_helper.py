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

