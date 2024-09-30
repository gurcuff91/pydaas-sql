from typing import Type, TypeVar

import sqlmodel as sm
from sqlalchemy.ext.asyncio.engine import AsyncEngine, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel.ext.asyncio.session import AsyncSession


class Database:
    def __init__(self) -> None:
        self._engine = None
        self._session_cls = None

    def init(self, db_url: str, echo: bool = True):
        self._engine = create_async_engine(db_url, echo=echo)  # type: ignore[assignment]

    @property
    def engine(self) -> AsyncEngine:
        if not self._engine:
            raise Exception("Database uninitialized. Call db.init(...) method")
        return self._engine

    @property
    def Session(self) -> Type[AsyncSession]:
        if not self._session_cls:
            self._session_cls = sessionmaker(
                self.engine, class_=AsyncSession, expire_on_commit=False
            )  # type: ignore[call-overload]
        return self._session_cls  # type: ignore[return-value]


db = Database()


class Filter:
    def __init__(self, *expressions) -> None:
        self._expressions = expressions

    def and_(self, *expressions) -> "Filter":
        return Filter(*(*self._expressions, *expressions))

    def or_(self, *expressions) -> "Filter":
        ors = []
        if self._expressions:
            ors.append(sm.and_(*self._expressions))
        if expressions:
            ors.append(sm.and_(*expressions))
        return Filter(sm.or_(*ors)) if ors else Filter()

    def apply(self, stm):
        return stm.where(*self._expressions)

    def __repr__(self) -> str:
        return f"Filter{repr(self._expressions)}"

    def __getitem__(self, index):
        return self._expressions[index]


class Sorting:
    def __init__(self, *expressions) -> None:
        self._expressions = expressions

    def asc_(self, *expressions) -> "Sorting":
        return Sorting(*(*self._expressions, *expressions))

    def desc_(self, *expressions) -> "Sorting":
        return Sorting(*(*self._expressions, *(sm.desc(i) for i in expressions)))

    def apply(self, stm):
        return stm.order_by(*self._expressions)

    def __repr__(self) -> str:
        return f"Sorting{repr(self._expressions)}"

    def __getitem__(self, index):
        return self._expressions[index]


class Pagination:
    def __init__(
        self, page: int = 1, page_size: int = 20, fetch_one_more: bool = False
    ) -> None:
        self._page = max(page, 1)
        self._page_size = max(page_size, 1)
        self._fetch_one_more = fetch_one_more

    def page_(self, page: int) -> "Pagination":
        return Pagination(page, self._page_size, self._fetch_one_more)

    def page_size_(self, page_size: int) -> "Pagination":
        return Pagination(self._page, page_size, self._fetch_one_more)

    def fetch_one_more_(self, fetch_one_more: bool) -> "Pagination":
        return Pagination(self._page, self._page_size, fetch_one_more)

    def apply(self, stm):
        offset = (self._page - 1) * self._page_size
        limit = self._page_size + 1 if self._fetch_one_more else self._page_size
        return stm.offset(offset).limit(limit)

    def __repr__(self) -> str:
        return f"Pagination{self._page, self._page_size, self._fetch_one_more}"
