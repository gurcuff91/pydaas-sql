from typing import Type

from sqlmodel import Field, SQLModel


class IdModel(SQLModel):
    id: int | None = Field(None, primary_key=True)


def base_search(items_cls: Type[SQLModel]) -> Type[SQLModel]:
    class _BaseSearch(SQLModel):
        items: list[items_cls]  # type: ignore[valid-type]
        prev_page: int | None = Field(schema_extra={"examples": [None]})
        next_page: int | None = Field(schema_extra={"examples": [2]})

    return _BaseSearch
