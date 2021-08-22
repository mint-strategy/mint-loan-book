import logging
from pathlib import Path

import pandas as pd
import pytest
from appdirs import user_data_dir

import mint_strategy.loan_book.session as session
from mint_strategy.loan_book.load_df import load
from mint_strategy.loan_book.timer import Timer

logging.getLogger().setLevel('DEBUG')


def to_csv_gz(df: pd.DataFrame):
    import gzip
    with gzip.open('data.csv.gz', 'wb') as f:
        df.to_csv(f)


def read_csv_gz():
    import gzip
    with gzip.open('data.csv.gz', 'rb') as f:
        pd.read_csv(f)


@pytest.skip
def test_io():
    s = session.Session("d8718147-54fa-4448-aac5-0271717d5c19", Path(user_data_dir('mint_strategy')))
    df = load(s, slice(0, 1))

    Timer('to_csv').with_func(df.to_csv, 'data.csv')
    Timer('read_csv').with_func(pd.read_csv, 'data.csv')

    Timer('to_csv.gz').with_func(to_csv_gz, df)
    Timer('read_csv.gz').with_func(read_csv_gz)

    Timer('to_pickle').with_func(df.to_pickle, 'data.pickle')
    Timer('read_pickle').with_func(pd.read_pickle, 'data.pickle')

    Timer('to_pickle.gz').with_func(df.to_pickle, 'data.pickle.gz')
    Timer('read_pickle.gz').with_func(pd.read_pickle, 'data.pickle.gz')
