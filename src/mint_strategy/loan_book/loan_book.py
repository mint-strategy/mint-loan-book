import logging
import pathlib
import zipfile
from tempfile import TemporaryDirectory

import pandas as pd


async def load(input_zip: pathlib.Path, temp_dir: pathlib.Path = None) -> pd.DataFrame:
    if temp_dir is None:
        async with TemporaryDirectory() as temp_dir:
            return await _load(input_zip, temp_dir)
    else:
        return await _load(input_zip, temp_dir)


async def _load(input_zip: pathlib.Path, temp_dir: pathlib.Path = None) -> pd.DataFrame:
    unzip_target=temp_dir / 'loan_book'
    await unzip(input_zip, unzip_target)


async def unzip(zip_path: pathlib.Path, target: pathlib.Path) -> None:
    logging.info("Unzip %s to %s", zip_path, target)
    async with zipfile.ZipFile(zip_path) as zipf:
        async for zipinfo in zipf.filelist:
            name = zipinfo.filename
            target_file = target / name

            act_target_file = pathlib.Path(zipf.extract(name, target))
            assert target_file == act_target_file
