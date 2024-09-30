import asyncio
from typing import AsyncIterator, Generic, Sequence, Type, TypeVar

from sqlmodel import SQLModel, select
from sqlmodel.ext.asyncio.session import AsyncSession

from pydaas_sql.database import Filter, Pagination, Sorting

T = TypeVar("T", bound=SQLModel)


class DataService(Generic[T]):

    Model: Type[T] = None  # type: ignore[assignment]

    def __init__(self, db: AsyncSession) -> None:
        assert self.Model is not None, "DataService.Model is required"
        self.db = db

    def _create_model(self, data: SQLModel | dict) -> T:
        is_dict = isinstance(data, dict)
        return (
            self.Model.model_validate(data)
            if is_dict
            else self.Model(**data.model_dump(exclude_unset=True))  # type: ignore[union-attr]
        )

    def _update_model(self, instance: T, values: SQLModel | dict) -> T:
        if not isinstance(values, dict):
            values = values.model_dump(exclude_unset=True, exclude_defaults=True)
        sentinel = object()
        for field in self.Model.__fields__:  # type: ignore[attr-defined]
            value = values.get(field, sentinel)
            if value is not sentinel:
                setattr(instance, field, value)
        return instance

    async def _get_model_chunks(
        self, filter: Filter | None = None, chunk_size: int = 100
    ) -> AsyncIterator[Sequence[T]]:
        page = 1
        has_next = True
        while has_next:
            chunk = await self.search(
                filter,
                pagination=Pagination(page, page_size=chunk_size, fetch_one_more=True),
            )
            has_next = len(chunk) == chunk_size + 1
            yield chunk[:-1] if has_next else chunk
            page = page + 1

    async def search(
        self,
        filter: Filter | None = None,
        sorting: Sorting | None = None,
        pagination: Pagination | None = None,
    ) -> Sequence[T]:
        stm = select(self.Model)
        if filter:
            stm = filter.apply(stm)
        if sorting:
            stm = sorting.apply(stm)
        if pagination:
            stm = pagination.apply(stm)

        res = await self.db.exec(stm)
        return res.all()

    async def create(self, data: SQLModel | dict) -> T:
        instance = self._create_model(data)
        self.db.add(instance)
        await self.db.commit()
        return instance

    async def get(self, filter: Filter) -> T | None:
        stm = select(self.Model)
        stm = filter.apply(stm)
        res = await self.db.exec(stm)
        return res.one_or_none()

    async def update(self, instance: T, data: SQLModel | dict) -> T:
        self._update_model(instance, data)
        self.db.add(instance)
        await self.db.commit()
        return instance

    async def delete(self, instance: T):
        await self.db.delete(instance)

    async def get_by_id(self, id) -> T | None:
        id = [id] if not isinstance(id, (tuple, list)) else id
        filter = Filter()
        for i, col in enumerate(self.Model.__table__.primary_key.columns):  # type: ignore[attr-defined]
            filter = filter.and_(getattr(self.Model, col.name) == id[i])
        return await self.get(filter)

    async def update_by_id(self, id, data: SQLModel | dict) -> T | None:
        instance = await self.get_by_id(id)
        if not instance:
            return None
        return await self.update(instance, data)

    async def delete_by_id(self, id) -> bool:
        instance = await self.get_by_id(id)
        if not instance:
            return False

        await self.delete(instance)
        return True

    async def bulk_create(
        self, values: list[SQLModel | dict], chunk_size: int = 100
    ) -> int:
        count = 0
        for data in values:
            instance = self._create_model(data)
            self.db.add(instance)
            count += 1
            if count % chunk_size == 0:
                await self.db.commit()
        if count % chunk_size != 0:
            await self.db.commit()
        return count

    async def bulk_update(
        self, filter: Filter, data: SQLModel, chunk_size: int = 100
    ) -> int:
        count = 0
        async for chunk in self._get_model_chunks(filter, chunk_size):
            chunk = [self._update_model(i, data) for i in chunk]
            self.db.add_all(chunk)
            await self.db.commit()
            count += len(chunk)
        return count

    async def bulk_delete(self, filter: Filter, chunk_size: int = 100) -> int:
        count = 0
        async for chunk in self._get_model_chunks(filter, chunk_size):
            ops = [self.delete(i) for i in chunk]
            await asyncio.gather(*ops)
            count += len(chunk)
        return count
