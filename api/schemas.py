"""Common API response envelopes.

Every API response wraps its payload in :class:`APIResponse` to keep the shape
consistent on the frontend (rule 5 in CLAUDE.md):

```
{
  "data": <payload>,
  "meta": { "total": int, "page": int, "per_page": int },
  "errors": []
}
```
"""

from __future__ import annotations

from typing import Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field

T = TypeVar("T")


class Meta(BaseModel):
    total: int | None = None
    page: int | None = None
    per_page: int | None = None
    extra: dict[str, object] | None = None


class APIError(BaseModel):
    code: str
    message: str
    field: str | None = None


class APIResponse(BaseModel, Generic[T]):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    data: T | None = None
    meta: Meta = Field(default_factory=Meta)
    errors: list[APIError] = Field(default_factory=list)


def ok(data: T, *, meta: Meta | None = None) -> APIResponse[T]:
    return APIResponse[T](data=data, meta=meta or Meta(), errors=[])
