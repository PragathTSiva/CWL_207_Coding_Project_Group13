# src/wikimedia_api.py
"""
Thin wrappers around MediaWiki & Wikidata endpoints.
Doing all network I/O here keeps the scraper logic in `scrape_wiki.py`
easy to read and test.
"""
from __future__ import annotations
import time, requests, asyncio, aiohttp, urllib.parse
from datetime import datetime
from collections import defaultdict
from typing import Any

import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
from tqdm.auto import tqdm

from src.config import (
    MEDIAWIKI_API,
    WIKIDATA_ENDPOINT,
    HEADERS,
    MAX_CONCURRENCY,
)

# ---------- SPARQL ----------
# Initialize SPARQL wrapper for Wikidata queries
sparql = SPARQLWrapper(WIKIDATA_ENDPOINT)
sparql.setReturnFormat(JSON)

def run_sparql(query: str, retries: int = 3, backoff: float = 2.0) -> list[dict[str, str]]:
    """Run a SPARQL query and return bindings as dicts."""
    for attempt in range(1, retries + 1):
        try:
            sparql.setQuery(query)
            results = sparql.query().convert()
            return [
                {k: v.get("value") for k, v in row.items()}
                for row in results["results"]["bindings"]
            ]
        except Exception as e:
            if attempt == retries:
                raise
            sleep_time = backoff * attempt
            print(f"SPARQL query failed, retrying in {sleep_time}s... (Attempt {attempt}/{retries})")
            time.sleep(sleep_time)

# ---------- MediaWiki helpers ----------
def _safe_request(params: dict[str, Any],
                  retries: int = 3,
                  backoff: float = 1.5) -> dict[str, Any]:
    # Helper function to make API requests with retry logic
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(MEDIAWIKI_API,
                                params=params,
                                headers=HEADERS,
                                timeout=30)
            resp.raise_for_status()
            return resp.json()
        except (requests.exceptions.RequestException, ValueError):
            if attempt == retries:
                raise
            time.sleep(backoff * attempt)

def fetch_category_members(cat_title: str,
                           cmtype: str = "subcat",
                           limit: int | str = "max") -> list[str]:
    """List pages (or sub-cats) inside a category title (without 'Category:' prefix)."""
    members, cont_token = [], ""
    while True:
        params = {
            "action"  : "query",
            "format"  : "json",
            "list"    : "categorymembers",
            "cmtitle" : f"Category:{cat_title}",
            "cmtype"  : cmtype,
            "cmlimit" : limit,
            "cmcontinue": cont_token,
        }
        data = _safe_request(params)
        members.extend(m["title"] for m in data["query"]["categorymembers"])
        cont_token = data.get("continue", {}).get("cmcontinue", "")
        if not cont_token:
            break
    return members

# ---------- Async summaries ----------
# Utilizing async to speed up summary fetching (major performance bottleneck otherwise)
async def _fetch_summary(session: aiohttp.ClientSession, title: str) -> str | None:
    url = f"https://en.wikipedia.org/api/rest_v1/page/summary/{urllib.parse.quote(title, safe='')}"
    async with session.get(url, headers=HEADERS) as resp:
        if resp.status != 200:
            return None
        data = await resp.json()
        return data.get("extract")

async def gather_summaries(titles: list[str]) -> dict[str, str | None]:
    # Uses semaphore to limit concurrent connections based on MAX_CONCURRENCY
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    async with aiohttp.ClientSession() as session:
        async def worker(t):
            async with sem:
                return t, await _fetch_summary(session, t)

        tasks = [asyncio.create_task(worker(t)) for t in titles]
        out   = {}
        for fut in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Summaries"):
            title, summary = await fut
            out[title] = summary
    return out

# ---------- Batched Wikidata metadata ----------
def query_wikidata_batch(qids: list[str]) -> dict[str, dict[str, str | int | None]]:
    """Resolve up to ~200 Q-IDs at once; returns imdb_id, year, people list."""
    # Construct SPARQL query with VALUES clause for batching efficiency
    values = " ".join(f"wd:{q}" for q in qids)
    q = f"""
    SELECT ?film ?imdb ?date ?personLabel WHERE {{
      VALUES ?film {{ {values} }}
      OPTIONAL {{ ?film wdt:P345 ?imdb. }}
      OPTIONAL {{ ?film wdt:P577 ?date. }}
      OPTIONAL {{ ?film ?prop ?person.
                 VALUES ?prop {{ wdt:P57 wdt:P161 wdt:P162 wdt:P58 }} }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    """
    rows = run_sparql(q)
    out = {}
    for row in rows:
        qid = row["film"].split("/")[-1]
        e   = out.setdefault(qid, {"imdb_id": None, "year": None, "people": set()})
        e["imdb_id"] = e["imdb_id"] or row.get("imdb")
        if "date" in row and e["year"] is None:
            try:
                e["year"] = datetime.fromisoformat(row["date"].split("T")[0]).year
            except Exception:
                pass
        if "personLabel" in row:
            e["people"].add(row["personLabel"])
    for e in out.values():
        e["people"] = "; ".join(sorted(e["people"])) if e["people"] else None
    return out
