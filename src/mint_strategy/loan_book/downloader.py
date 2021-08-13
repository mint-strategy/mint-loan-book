import asyncio
import logging
import time
from typing import Mapping

import aiohttp
import humanize

allow_override = [
    'accept',
    'accept-encoding',
    'accept-language',
    'user-agent',
]

static_headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:91.0) Gecko/20100101 Firefox/91.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Referer': 'https://www.mintos.com/webapp/en/invest-en/primary-market/?sort_field=interest&sort_order=DESC'
               + '&currencies%5B%5D=978&referrer=https%3A%2F%2Fwww.mintos.com&hash=',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Sec-GPC': '1',
}


class Downloader:
    def __init__(self, client_session: aiohttp.ClientSession):
        self.client_session = client_session
        self.in_session = False

    async def download(self, cookies: Mapping[str, str], headers: Mapping[str, str]) -> bool:
        if self.in_session:
            return False

        self.in_session = True

        try:
            loop = asyncio.get_event_loop()
            logging.info('start download')
            task = loop.create_task(self._download(cookies, headers))
            logging.info('returning response')
            return True
        except:
            return False
        finally:
            self.in_session = False

    async def _download(self, cookies: Mapping[str, str], headers: Mapping[str, str]) -> None:
        headers = {**static_headers, **{k: v for k, v in headers.items() if k in allow_override}}

        time_start = time.perf_counter()
        async with self.client_session.get('https://www.mintos.com/en/loan-book/download',
                                           cookies=cookies,
                                           headers=headers,
                                           timeout=aiohttp.ClientTimeout(
                                               total=60 * 60,
                                               sock_read=aiohttp.client.DEFAULT_TIMEOUT
                                           )
                                           ) as response:
            logging.info('downloading')
            with open('loan_book.zip', 'wb') as fd:
                while True:
                    chunk = await response.content.read(2 ** 20)
                    if not chunk:
                        break
                    fd.write(chunk)
                    data_len = fd.tell()
        time_end = time.perf_counter()
        elapsed = time_end - time_start;
        speed_bps = data_len / elapsed
        speed = humanize.naturalsize(speed_bps)
        logging.info('download done %s/s', speed)
