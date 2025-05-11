# src/scrape_wiki.py
"""
Command-line entry-point that:
1. Builds category ➜ film-title maps
2. Fetches Wikidata + Wikipedia metadata
3. Writes per-group CSVs into data/processed/
"""

from __future__ import annotations
import asyncio
import time
import argparse
import json
import os
from collections import defaultdict
from pathlib import Path

import pandas as pd
from tqdm.auto import tqdm

from src.config import TARGET_GROUPS, DATA_PROCESSED
from src.utils import strip_cat_prefix, slugify, chunked
from src.wikimedia_api import (
    fetch_category_members,
    gather_summaries,
    query_wikidata_batch,
    run_sparql,
    _safe_request,
)
from src.data_cleaning import (
    clean_dataset, 
    enrich_dataset, 
    add_language_column,
    generate_data_quality_report
)

# Initialize checkpoint directories
CHECKPOINT_DIR = Path("data/checkpoints")
CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

# Also create a directory for quality reports
REPORTS_DIR = Path("data/reports")
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

# ------------------------------------------------------------------
def build_subcats() -> dict[str, list[str]]:
    """Step 1: Build subcategory maps"""
    checkpoint_file = CHECKPOINT_DIR / "subcats.json"
    
    # Check if checkpoint exists
    if checkpoint_file.exists():
        print(f"Loading subcategories from checkpoint: {checkpoint_file}")
        with open(checkpoint_file, 'r') as f:
            return json.load(f)
    
    print("Building subcategories...")
    subcats = {}
    for group in TARGET_GROUPS:
        subcats[group] = fetch_category_members(group, cmtype="subcat")
    
    # Save checkpoint
    with open(checkpoint_file, 'w') as f:
        json.dump(subcats, f)
    
    return subcats

def build_films(subcats_map: dict[str, list[str]]) -> dict[str, set[str]]:
    """Step 2: Build film maps from subcategories"""
    checkpoint_file = CHECKPOINT_DIR / "films.json"
    
    # Check if checkpoint exists
    if checkpoint_file.exists():
        print(f"Loading films from checkpoint: {checkpoint_file}")
        with open(checkpoint_file, 'r') as f:
            # Convert lists back to sets
            films_data = json.load(f)
            return {k: set(v) for k, v in films_data.items()}
    
    print("Building film maps...")
    films_map: dict[str, set[str]] = defaultdict(set)
    for group, subcats in tqdm(subcats_map.items(), desc="Groups"):
        films_map[group].update(fetch_category_members(group, cmtype="page"))
        for subcat in tqdm(subcats, leave=False, desc=group):
            films_map[group].update(
                fetch_category_members(strip_cat_prefix(subcat), cmtype="page")
            )
    
    # Save checkpoint (convert sets to lists for JSON serialization)
    with open(checkpoint_file, 'w') as f:
        json.dump({k: list(v) for k, v in films_map.items()}, f)
    
    return films_map

def resolve_qids(titles, group) -> dict[str, str]:
    """Step 3: Resolve Wikidata Q-IDs for titles"""
    checkpoint_file = CHECKPOINT_DIR / f"qids_{slugify(group)}.json"
    
    # Check if checkpoint exists
    if checkpoint_file.exists():
        print(f"Loading Q-IDs from checkpoint: {checkpoint_file}")
        with open(checkpoint_file, 'r') as f:
            return json.load(f)
    
    print(f"Resolving Q-IDs for {len(titles)} titles...")
    qid_map = {}
    for batch in tqdm(list(chunked(sorted(titles), 50)), desc="Q-IDs"):
        params = {
            "action": "query", "format": "json",
            "titles": "|".join(batch), "prop": "pageprops",
        }
        data = _safe_request(params)
        for page in data["query"]["pages"].values():
            qid = page.get("pageprops", {}).get("wikibase_item")
            if qid: qid_map[page["title"]] = qid
    
    # Save checkpoint
    with open(checkpoint_file, 'w') as f:
        json.dump(qid_map, f)
    
    return qid_map

def fetch_metadata(qid_map, group) -> dict[str, dict]:
    """Step 4: Fetch Wikidata metadata for Q-IDs"""
    checkpoint_file = CHECKPOINT_DIR / f"metadata_{slugify(group)}.json"
    
    # Check if checkpoint exists
    if checkpoint_file.exists():
        print(f"Loading metadata from checkpoint: {checkpoint_file}")
        with open(checkpoint_file, 'r') as f:
            return json.load(f)
    
    print(f"Fetching metadata for {len(qid_map)} Q-IDs...")
    meta_map = {}
    batches = list(chunked(list(qid_map.values()), 200))
    for i, q_batch in enumerate(tqdm(batches, desc="SPARQL")):
        meta_map.update(query_wikidata_batch(q_batch))
        # Add a delay between batches to avoid rate limiting
        if i < len(batches) - 1:
            time.sleep(2)  # 2 second delay between batches
    
    # Save checkpoint (convert any non-serializable types)
    serializable_meta = {}
    for qid, data in meta_map.items():
        serializable_meta[qid] = {
            k: str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v
            for k, v in data.items()
        }
    
    with open(checkpoint_file, 'w') as f:
        json.dump(serializable_meta, f)
    
    return meta_map

def fetch_summaries(qid_map, group):
    """Step 5: Fetch Wikipedia summaries for titles"""
    checkpoint_file = CHECKPOINT_DIR / f"summaries_{slugify(group)}.json"
    
    # Check if checkpoint exists
    if checkpoint_file.exists():
        print(f"Loading summaries from checkpoint: {checkpoint_file}")
        with open(checkpoint_file, 'r') as f:
            return json.load(f)
    
    print(f"Fetching summaries for {len(qid_map)} titles...")
    summaries = asyncio.run(gather_summaries(list(qid_map.keys())))
    
    # Save checkpoint
    with open(checkpoint_file, 'w') as f:
        # Handle None values for JSON
        json_safe_summaries = {k: v if v is not None else "" for k, v in summaries.items()}
        json.dump(json_safe_summaries, f)
    
    return summaries

def assemble_csv(qid_map, meta_map, summaries, group):
    """Step 6: Assemble and write CSV file"""
    # Create output directory if it doesn't exist
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    
    print(f"Assembling CSV for {group}...")
    rows = []
    for title, qid in qid_map.items():
        m = meta_map.get(qid, {})
        rows.append({
            "title"  : title,
            "imdb_id": m.get("imdb_id"),
            "year"   : m.get("year"),
            "summary": summaries.get(title),
            "people" : m.get("people"),
        })
    df = pd.DataFrame(rows)
    
    # Apply data cleaning and enrichment
    print(f"Cleaning and enriching data for {group}...")
    df = clean_dataset(df)
    df = enrich_dataset(df)
    df = add_language_column(df)
    
    # Generate and save data quality report
    report = generate_data_quality_report(df)
    report_file = REPORTS_DIR / f"quality_report_{slugify(group)}.json"
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    print(f"Data quality report saved to {report_file}")
    
    # Save the CSV
    out_file = DATA_PROCESSED / f"indian_films_{slugify(group)}.csv"
    df.to_csv(out_file, index=False)
    print(f"✅  Saved {len(df):5} rows → {out_file}")
    
    return df

# ------------------------------------------------------------------
def main(steps=None, specific_group=None):
    """Main function with optional step selection"""
    if steps is None:
        steps = ["all"]
    
    run_all = "all" in steps
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)

    # Step 1 & 2: Build category structure
    if run_all or "subcats" in steps:
        subcats_map = build_subcats()
    else:
        # Load from checkpoint if available
        checkpoint_file = CHECKPOINT_DIR / "subcats.json"
        if checkpoint_file.exists():
            with open(checkpoint_file, 'r') as f:
                subcats_map = json.load(f)
        else:
            print("Subcategories checkpoint not found. Run with --steps subcats first.")
            return

    if run_all or "films" in steps:
        films_map = build_films(subcats_map)
    else:
        # Load from checkpoint if available
        checkpoint_file = CHECKPOINT_DIR / "films.json"
        if checkpoint_file.exists():
            with open(checkpoint_file, 'r') as f:
                films_data = json.load(f)
                films_map = {k: set(v) for k, v in films_data.items()}
        else:
            print("Films checkpoint not found. Run with --steps films first.")
            return

    # Filter to specific group if requested
    if specific_group:
        if specific_group in films_map:
            films_map = {specific_group: films_map[specific_group]}
        else:
            print(f"Group '{specific_group}' not found. Available groups: {', '.join(films_map.keys())}")
            return

    # Process each group
    for group, titles in films_map.items():
        print(f"\n🚀  Processing {group} ({len(titles)} films)")
        
        # Step 3: Resolve Q-IDs
        if run_all or "qids" in steps:
            qid_map = resolve_qids(titles, group)
        else:
            checkpoint_file = CHECKPOINT_DIR / f"qids_{slugify(group)}.json"
            if checkpoint_file.exists():
                with open(checkpoint_file, 'r') as f:
                    qid_map = json.load(f)
            else:
                print(f"Q-IDs checkpoint for {group} not found. Run with --steps qids first.")
                continue
        
        # Step 4: Fetch metadata
        if run_all or "metadata" in steps:
            meta_map = fetch_metadata(qid_map, group)
        else:
            checkpoint_file = CHECKPOINT_DIR / f"metadata_{slugify(group)}.json"
            if checkpoint_file.exists():
                with open(checkpoint_file, 'r') as f:
                    meta_map = json.load(f)
            else:
                print(f"Metadata checkpoint for {group} not found. Run with --steps metadata first.")
                continue
        
        # Step 5: Fetch summaries
        if run_all or "summaries" in steps:
            summaries = fetch_summaries(qid_map, group)
        else:
            checkpoint_file = CHECKPOINT_DIR / f"summaries_{slugify(group)}.json"
            if checkpoint_file.exists():
                with open(checkpoint_file, 'r') as f:
                    summaries = json.load(f)
            else:
                print(f"Summaries checkpoint for {group} not found. Run with --steps summaries first.")
                continue
        
        # Step 6: Assemble CSV
        if run_all or "csv" in steps:
            assemble_csv(qid_map, meta_map, summaries, group)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape Wikipedia/Wikidata for film information")
    parser.add_argument(
        "--steps", 
        nargs="+", 
        choices=["all", "subcats", "films", "qids", "metadata", "summaries", "csv", "clean"],
        default=["all"],
        help="Specify which steps to run"
    )
    parser.add_argument(
        "--group",
        type=str,
        help="Process only a specific group"
    )
    parser.add_argument(
        "--clean-only",
        action="store_true",
        help="Only clean existing CSV files without scraping new data"
    )
    args = parser.parse_args()
    
    if args.clean_only:
        # Only clean existing files
        for csv_file in DATA_PROCESSED.glob("*.csv"):
            group_name = csv_file.stem.replace("indian_films_", "")
            print(f"Cleaning existing file: {csv_file}")
            df = pd.read_csv(csv_file)
            df = clean_dataset(df)
            df = enrich_dataset(df)
            df = add_language_column(df)
            
            # Generate and save data quality report
            report = generate_data_quality_report(df)
            report_file = REPORTS_DIR / f"quality_report_{group_name}.json"
            with open(report_file, 'w') as f:
                json.dump(report, f, indent=2)
            print(f"Data quality report saved to {report_file}")
            
            # Save the cleaned CSV
            df.to_csv(csv_file, index=False)
            print(f"✅  Saved {len(df):5} cleaned rows → {csv_file}")
    else:
        main(args.steps, args.group)
