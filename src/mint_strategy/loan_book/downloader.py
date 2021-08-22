import asyncio
import logging
import typing
from typing import Mapping

import aiohttp

import mint_strategy.loan_book.session as s
import mint_strategy.loan_book.timer as t


log = logging.getLogger(__name__)

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
    def __init__(
            self,
            client_session: aiohttp.ClientSession,
            sess_factory: s.SessionFactory
    ):
        self.client_session = client_session
        self._sess_factory = sess_factory
        self._current_sess:s.Session = None

    async def download(self, cookies: typing.Mapping[str, str], headers: typing.Mapping[str, str]) -> bool:
        if self._current_sess:
            return False

        self._current_sess = self._sess_factory()

        async def defer():
            try:
                await self._download(cookies, headers)
            finally:
                self._current_sess = None

        try:
            loop = asyncio.get_event_loop()
            task = loop.create_task(defer())

            return True
        except:
            return False

    async def _download(self, cookies: typing.Mapping[str, str], headers: Mapping[str, str]) -> None:
        headers = static_headers | {k: v for k, v in headers.items() if k in allow_override}

        with t.Timer('Download loan_book') as timer:
            async with self.client_session.get(
                    'https://www.mintos.com/en/loan-book/download',
                    cookies=cookies,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(
                        total=60 * 60,
                        sock_read=5 * 60
                    )
            ) as response:
                log.info('downloading to %s', self._current_sess.home)
                with open(self._current_sess.zipped, 'wb') as fd:
                    while True:
                        chunk = await response.content.read(2 ** 20)
                        if not chunk:
                            break
                        fd.write(chunk)
                    data_len = fd.tell()
            timer.bytes_processed = data_len
