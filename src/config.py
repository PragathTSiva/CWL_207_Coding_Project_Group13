"""
Centralised configuration so a grader can tweak without digging into logic.
"""

from pathlib import Path

# ---------- Scraper knobs ----------
MAX_CONCURRENCY   = 50          # concurrent Wikipedia REST calls
WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
MEDIAWIKI_API     = "https://en.wikipedia.org/w/api.php"

HEADERS = {
    "User-Agent": (
        "IndianFilmScraper/1.0 "
        "(University of Illinois coursework; contact: ptsiva2@illinois.edu)"
    )
}

TARGET_GROUPS = [
    "Indian films by decade",
    "Indian films by genre",
    "Indian films by language",
    "Indian films by topic",
    "Indian remakes of foreign films",
    "Indian films based on plays",
]

# ---------- Paths ----------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_RAW     = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"
