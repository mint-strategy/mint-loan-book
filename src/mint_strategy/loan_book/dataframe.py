import logging
import logging.handlers
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

data_types = {
    'Buyback reason': 'category',
    'Closing Date': 'datetime64',
    'Country': 'category',
    'Currency': 'category',
    'Issue Date': 'datetime64',
    'Listing Date': 'datetime64',
    'Loan Type': 'category',
    'Loan Status': 'category',
    'Loan Originator': 'category',
    'Mintos Rating': 'category',
}


def yes_no_bool(yes_no: str) -> bool:
    return yes_no == 'Yes'


converters = {
    'Buyback': yes_no_bool,
    'Collateral': yes_no_bool,
    'Extendable schedule': yes_no_bool,
}


async def load(input_dir: Path) -> pd.DataFrame:
    logging.debug('%s %s',input_dir, input_dir.exists())
    for path in input_dir.iterdir():
        logging.info('child: %s',path)
        # df = pandas.read_excel(in_p, dtype=data_types, converters=converters)

    return None
