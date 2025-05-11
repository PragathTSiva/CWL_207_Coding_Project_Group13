# CWL_207_Coding_Project_Group13

# Indian Film Dataset Scraper

This project provides a comprehensive data collection pipeline for scraping and processing information about Indian films from Wikipedia and Wikidata. It organizes films into different categories and collects metadata including IMDB IDs, release years, summaries, and people associated with each film.

## Project Overview

This project scrapes data from Wikipedia/Wikidata for Indian films across several categories:
- Indian films by decade
- Indian films by genre
- Indian films by language
- Indian films by topic
- Indian remakes of foreign films
- Indian films based on plays

For each film, we collect:
- Title
- IMDB ID (when available)
- Release year
- Wikipedia summary
- People associated with the film (directors, actors, etc.)

## Directory Structure

```
project/
├── README.md          - This file
├── requirements.txt   - Python dependencies
├── src/               - Source code
│   ├── __init__.py    - Package marker
│   ├── config.py      - Configuration constants
│   ├── utils.py       - Utility functions
│   ├── wikimedia_api.py - API interaction with Wikipedia/Wikidata
│   ├── data_cleaning.py - Data cleaning and enrichment functions
│   └── scrape_wiki.py - Main pipeline script
├── data/
│   ├── raw/           - Raw data (empty, used for consistency)
│   ├── processed/     - Final CSV outputs
│   ├── reports/       - Data quality reports
│   └── checkpoints/   - Intermediate data saved during processing
└── .gitignore         - Files to ignore in version control
```

## Installation

1. Clone the repository:
```bash
git clone https://github.com/PragathTSiva/CWL_207_Coding_Project_Group13.git
cd CWL_207_Coding_Project_Group13
```

2. Create and activate a virtual environment (optional but recommended):
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

The main script is `src/scrape_wiki.py`, which can be run in different ways:

### Running the Full Pipeline

To run all steps for all film categories:

```bash
python -m src.scrape_wiki
```

### Running Specific Steps

You can run specific steps of the pipeline using the `--steps` argument:

```bash
python -m src.scrape_wiki --steps subcats films
```

Available steps:
- `subcats`: Build subcategory maps
- `films`: Build film maps from subcategories
- `qids`: Resolve Wikidata Q-IDs for titles
- `metadata`: Fetch Wikidata metadata for Q-IDs
- `summaries`: Fetch Wikipedia summaries for titles
- `csv`: Assemble and write CSV files
- `clean`: Apply data cleaning and enrichment

### Processing Specific Groups

You can process only a specific film category using the `--group` argument:

```bash
python -m src.scrape_wiki --steps qids metadata summaries csv --group "Indian films by language"
```

### Cleaning Existing Data

To apply data cleaning to existing CSV files without scraping new data:

```bash
python -m src.scrape_wiki --clean-only
```

## Data Cleaning

The data cleaning module (`src/data_cleaning.py`) performs several operations to improve the quality of the dataset:

### Basic Cleaning
- Standardizes IMDB IDs to ensure consistent format
- Normalizes years to valid integers
- Cleans and standardizes film titles
- Fixes formatting issues in summaries (newlines, quotes, etc.)
- Normalizes the people field by removing duplicates and ensuring consistent format

### Data Enrichment
- Adds a decade column based on the release year
- Adds a people_count column counting the number of people associated with each film
- Adds a has_summary indicator
- Extracts language information from film summaries where possible

### Data Quality Reports
For each processed category, a data quality report is generated in the `data/reports/` directory providing:
- Total number of rows
- Null counts and percentages for each column
- Number of duplicate entries
- Year range
- Decade distribution

## Output Data

The scraped data is saved as CSV files in the `data/processed/` directory, with one file per film category:

- `indian_films_indian_films_by_decade.csv`
- `indian_films_indian_films_by_genre.csv`
- `indian_films_indian_films_by_language.csv`
- `indian_films_indian_films_by_topic.csv`
- `indian_films_indian_remakes_of_foreign_films.csv`
- `indian_films_indian_films_based_on_plays.csv`

Each CSV file contains the following columns:
- `title`: The name of the film
- `imdb_id`: The IMDB ID (when available)
- `year`: The release year
- `summary`: A summary of the film from Wikipedia
- `people`: A semicolon-separated list of people associated with the film
- `decade`: The decade the film was released (derived from year)
- `people_count`: Number of people associated with the film
- `has_summary`: Boolean indicating if a summary is available
- `language`: The film's language (when extractable from summary)

## Features

1. **Modular Pipeline**: The scraping process is divided into separate steps with checkpoints between them
2. **Error Handling**: Robust error handling with retries for API requests
3. **Rate Limiting**: Includes delays between requests to avoid rate limiting by the APIs
4. **Checkpointing**: Saves intermediate results to allow resuming from any step
5. **Customizable**: Command-line arguments to control which steps to run and which groups to process
6. **Data Cleaning**: Comprehensive data cleaning and enrichment to improve data quality
7. **Quality Reports**: Generation of data quality reports to monitor dataset metrics

## Team Members

- Group 13

## License

This project is licensed under the MIT License - see the LICENSE file for details. 