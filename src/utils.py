# src/utils.py
import re
from collections.abc import Iterable
from typing import Iterator, TypeVar

T = TypeVar("T")

def slugify(text: str) -> str:
    """`My Cool Name` → `my_cool_name`."""
    # Converts a title to a URL/filename friendly format
    return re.sub(r"[^A-Za-z0-9]+", "_", text).strip("_").lower()

def chunked(iterable: Iterable[T], size: int) -> Iterator[list[T]]:
    """Yield `size`-length chunks from any iterable."""
    # Helper to split large lists into chunks - used for batched API calls
    it = list(iterable)
    for i in range(0, len(it), size):
        yield it[i : i + size]

def strip_cat_prefix(title: str) -> str:
    """Remove the leading 'Category:' if present."""
    # MediaWiki categories have prefix that needs to be removed in some contexts
    return title[9:] if title.startswith("Category:") else title
