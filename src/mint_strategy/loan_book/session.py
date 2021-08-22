import pathlib
import typing
import uuid


class Session:
    def __init__(self, session_id: typing.Union[str, uuid.UUID], root: pathlib.Path):
        self._session_id = session_id
        self._home_created = False
        self._home = root / str(session_id)

    @property
    def zipped(self) -> pathlib.Path:
        return self.home / 'loan_book.zip'

    @property
    def unzipped(self) -> pathlib.Path:
        return self.home / 'loan_book'

    @property
    def home(self) -> pathlib.Path:
        if not self._home_created and not self._home.exists():
            self._home.mkdir(parents=True, exist_ok=False)

        return self._home

    @property
    def cache_dir(self) -> pathlib.Path:
        cache_dir = self._home / 'cache'
        if not cache_dir.exists():
            cache_dir.mkdir(parents=True)
        return cache_dir

    def __repr__(self) -> str:
        return f"Session[{self._session_id}]"


SessionFactory = typing.Callable[[], Session]
