import logging
import pathlib
from tempfile import TemporaryDirectory

import pytest

from mint_strategy.loan_book.dataframe import load

logging.basicConfig(level=logging.DEBUG)


@pytest.mark.asyncio
async def test_load():
    with TemporaryDirectory() as tempdir:
        await load(pathlib.Path(tempdir))
    assert True
