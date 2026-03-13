# Avibase Language Query

A small scraper that collects Avibase language translations for bird species, writes them into a master CSV, and resumes progress when interrupted.

## Setup (Linux/WSL)
1. Ensure Python 3.11+ is installed.
2. From the repository root run:
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install --upgrade pip
   pip install -r requirements.txt
   ```
   The project currently depends on `requests` and `beautifulsoup4`.

## Running the scraper
1. Prepare your input CSV (see example in `example-dict.csv`). The first column must be the scientific name, and each additional column should be the language translation payload (column header should be the ISO code or language name).
2. Run in chunks of 100 species:
   ```bash
   source .venv/bin/activate
   python avibase-query.py birds2.csv
   ```
3. The script will write to `birds_global_names.csv`, keep track of the current chunk inside `avibase_query_progress.json`, and will resume from the next chunk the next time it is run.

## Notes
- The script retries HTTP requests up to five times per URL and estimates how long the remaining species will take.
- Existing translations in the output file are preserved; the scraper only fills empty cells.
