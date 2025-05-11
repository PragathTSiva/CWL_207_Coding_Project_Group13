# src/utils.py
import re
from collections.abc import Iterable
from typing import Iterator, TypeVar

T = TypeVar("T")

def slugify(text: str) -> str:
    """`My Cool Name` → `my_cool_name`."""
    return re.sub(r"[^A-Za-z0-9]+", "_", text).strip("_").lower()

def chunked(iterable: Iterable[T], size: int) -> Iterator[list[T]]:
    """Yield `size`-length chunks from any iterable."""
    it = list(iterable)
    for i in range(0, len(it), size):
        yield it[i : i + size]

def strip_cat_prefix(title: str) -> str:
    """Remove the leading 'Category:' if present."""
    return title[9:] if title.startswith("Category:") else title
