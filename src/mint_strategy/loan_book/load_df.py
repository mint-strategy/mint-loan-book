import logging
import logging.handlers
import pathlib
import time
import typing
import zipfile

import pandas as pd

import mint_strategy.loan_book.session as s
import mint_strategy.loan_book.timer as t

log = logging.getLogger(__name__)
CACHING = True


def yes_no_bool(yes_no: str) -> bool:
    return yes_no == 'Yes'


def percent(percent: int) -> float:
    return percent / 100


data_types = {
    'Id': 'string',
    'Issue Date': 'datetime64[ns]',
    'Closing Date': 'datetime64[ns]',
    'Listing Date': 'datetime64[ns]',
    'Country': 'category',
    'Loan Originator': 'category',
    'Mintos Risk Score': 'category',
    'Loan Type': 'category',
    'Term': 'int',
    'Loan Status': 'category',
    'Buyback reason': 'category',
    'Currency': 'category',
    'Loan Originator Status': 'category',
}

converters = {
    'Loan Rate Percent': percent,
    'Collateral': yes_no_bool,
    'Initial LTV': percent,
    'LTV': percent,
    'Buyback': yes_no_bool,
    'Extendable schedule': yes_no_bool,
    'In Recovery': yes_no_bool,
}


def _load_file(sess: s.Session, zipf: zipfile.ZipFile, zipinfo: zipfile.ZipInfo) -> pd.DataFrame:
    cached_path = sess.cache_dir / get_cached_name(zipinfo.filename)
    if CACHING and cached_path.exists():
        with t.Timer(f"read {cached_path.name}") as timer:
            import os.path
            new_df = pd.read_pickle(cached_path)
            timer.bytes_processed = os.path.getsize(cached_path)
    else:
        new_df = read_zipfile(zipf, zipinfo)
        if CACHING:
            with t.Timer(f'write {cached_path.name}') as timer:
                import os.path
                new_df.to_pickle(cached_path)
                timer.bytes_processed = os.path.getsize(cached_path)
    new_df.set_index('Id', inplace=True)
    return new_df


def load(session: s.Session, last: int = 0):
    with zipfile.ZipFile(session.zipped) as zipf:
        all_files = zipf.infolist()[:]
        all_files.sort(key=lambda zi: zi.date_time)

        if last:
            read_files = all_files[-last:]
        else:
            read_files = all_files

        log.debug("loading %s files", len(read_files))
        dfs = [_load_file(session, zipf, zi) for zi in read_files]

    if not len(dfs):
        return pd.DataFrame()

    if len(dfs) > 1:
        df = pd.concat(dfs, verify_integrity=True)
        df[['Country', 'Loan Originator', 'Mintos Risk Score', 'Loan Type', 'Loan Status', 'Buyback reason',
            'Currency', 'Loan Originator Status']] = df[
            ['Country', 'Loan Originator', 'Mintos Risk Score', 'Loan Type', 'Loan Status', 'Buyback reason',
             'Currency', 'Loan Originator Status']] \
            .astype('category')
        return df
    elif len(dfs) == 1:
        df = dfs[0]

    return cleanup(df)


def read_zipfile(zipf, zipinfo) -> pd.DataFrame:
    with t.Timer(f'Read {zipinfo.filename}') as timer:
        with zipf.open(zipinfo.filename, 'r') as excel_buf:
            new_df = pd.read_excel(excel_buf, dtype=data_types, engine='xlrd', converters=converters)
            timer.bytes_processed = zipinfo.file_size
            return new_df


def get_cached_name(name: str) -> str:
    return pathlib.Path(name).stem + '.pickle.gz'
