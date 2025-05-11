"""
Centralised configuration so a grader can tweak without digging into logic.
"""

from pathlib import Path

# ---------- Scraper knobs ----------
# Max number of concurrent API calls to avoid rate limiting
MAX_CONCURRENCY   = 50          # concurrent Wikipedia REST calls
WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
MEDIAWIKI_API     = "https://en.wikipedia.org/w/api.php"

# User agent required by Wikipedia API guidelines
HEADERS = {
    "User-Agent": (
        "IndianFilmScraper/1.0 "
        "(University of Illinois coursework; contact: ptsiva2@illinois.edu)"
    )
}

# Main categories to scrape - these are the entry points for our film categories
TARGET_GROUPS = [
    "Indian films by decade",
    "Indian films by genre",
    "Indian films by language",
    "Indian films by topic",
    "Indian remakes of foreign films",
    "Indian films based on plays",
]

# ---------- Paths ----------
# Path configurations for data storage - processed results
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"
