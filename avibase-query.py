import argparse
import csv
import json
import os
import re
import time
import unicodedata

import requests
from bs4 import BeautifulSoup, Tag

BASE_URL = "https://avibase.bsc-eoc.org/"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

CHUNK_SIZE = 100
MAX_RETRIES = 5
RETRY_DELAY = 2
FETCH_TIMEOUT = 15
OUTPUT_FILE = "birds_global_names.csv"
STATE_FILE = "avibase_query_progress.json"
LANG_NAME_SPAN = re.compile(r"margin-left:\s*20px")
ISO_CODE_PATTERN = re.compile(r'^[a-z]{2,3}(?:[_-][a-z]{2,3})?$')

LANGUAGE_NAME_TO_CODE = {
    "english": "en",
    "english (uk)": "en_gb",
    "english (us)": "en_us",
    "afrikaans": "af",
    "azerbaijani": "az",
    "arabic": "ar",
    "basque": "eu",
    "bengali": "bn",
    "bulgarian": "bg",
    "catalan": "ca",
    "chinese": "zh",
    "chinese (traditional)": "zh_tw",
    "chinese (simplified)": "zh_cn",
    "croatian": "hr",
    "czech": "cs",
    "danish": "dk",
    "dutch": "nl",
    "esperanto": "eo",
    "estonian": "et",
    "filipino": "fil",
    "finnish": "fi",
    "french": "fr",
    "german": "de",
    "greek": "el",
    "hebrew": "he",
    "hungarian": "hu",
    "icelandic": "is",
    "indonesian": "id",
    "italian": "it",
    "japanese": "jp",
    "japanese (romaji)": "jp_latn",
    "korean": "ko",
    "korean (romanized)": "ko_latn",
    "latvian": "lv",
    "lithuanian": "lt",
    "malay": "ms",
    "nepali": "ne",
    "norwegian": "no",
    "norwegian nynorsk": "nn",
    "polish": "pl",
    "portuguese": "pt",
    "portuguese (portugal)": "pt_pt",
    "portuguese (brazil)": "pt_br",
    "romanian": "ro",
    "russian": "ru",
    "serbian": "sr",
    "slovak": "sk",
    "slovenian": "sl",
    "spanish": "es",
    "spanish (spain)": "es_es",
    "swedish": "sv",
    "thai": "th",
    "turkish": "tr",
    "ukrainian": "uk",
    "vietnamese": "vi",
}

SCI_NAME_FIELD_NAMES = {"sci", "scientific name", "name"}


def slugify_language(raw_name: str) -> str:
    normalized = unicodedata.normalize('NFKD', raw_name)
    ascii_name = normalized.encode('ascii', 'ignore').decode('ascii')
    slug = re.sub(r'[^a-z0-9]+', '_', ascii_name.lower()).strip('_')
    return slug or 'lang'


def language_name_to_code(name: str, allow_slug: bool = True) -> str | None:
    if not name:
        return None
    cleaned = name.replace('\xa0', ' ').strip(': ').lower()
    cleaned = re.sub(r'\s+', ' ', cleaned)
    if not cleaned:
        return None
    if cleaned in LANGUAGE_NAME_TO_CODE:
        return LANGUAGE_NAME_TO_CODE[cleaned]
    if ISO_CODE_PATTERN.match(cleaned):
        return cleaned.replace('-', '_')
    if not allow_slug:
        return None
    slug = slugify_language(cleaned)
    LANGUAGE_NAME_TO_CODE[cleaned] = slug
    return slug


def is_blank(value: str | None) -> bool:
    return not value or not value.strip()


def load_state(input_path: str) -> dict:
    if not os.path.exists(STATE_FILE):
        return {"next_index": 0}
    try:
        with open(STATE_FILE, 'r', encoding='utf-8') as f:
            state = json.load(f)
        if os.path.abspath(state.get('input_path', '')) != os.path.abspath(input_path):
            return {"next_index": 0}
        return state
    except (json.JSONDecodeError, OSError):
        return {"next_index": 0}


def save_state(input_path: str, next_index: int) -> None:
    state = {
        "input_path": os.path.abspath(input_path),
        "next_index": next_index,
        "timestamp": time.time()
    }
    with open(STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(state, f)


def load_input_rows(input_csv: str) -> tuple[list[dict], list[tuple[str, str]], str]:
    with open(input_csv, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return [], [], ''
        header = [h.strip() for h in reader.fieldnames if h and h.strip()]
        sci_field = header[0]
        for candidate in header:
            if candidate.lower() in SCI_NAME_FIELD_NAMES:
                sci_field = candidate
                break
        input_fields = []
        for item in header:
            if item.lower() == sci_field.lower():
                continue
            input_fields.append((item.strip().lower(), item))
        return list(reader), input_fields, sci_field


def load_existing_output() -> tuple[list[dict], list[str], dict[str, dict]]:
    if not os.path.exists(OUTPUT_FILE):
        return [], [], {}
    with open(OUTPUT_FILE, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return [], [], {}
        column_map: dict[str, str] = {}
        language_columns: list[str] = []
        for field in reader.fieldnames:
            if field.lower() == 'scientific name':
                continue
            code = language_name_to_code(field, allow_slug=False)
            if not code:
                continue
            if code in language_columns:
                column_map[field] = code
                continue
            language_columns.append(code)
            column_map[field] = code
        rows: list[dict] = []
        lookup: dict[str, dict] = {}
        for raw in reader:
            sci = raw.get('Scientific Name', '').strip()
            if not sci:
                continue
            normalized_row = {'Scientific Name': sci}
            for field, code in column_map.items():
                value = raw.get(field, '').strip()
                normalized_row.setdefault(code, '')
                if value:
                    if is_blank(normalized_row[code]):
                        normalized_row[code] = value
                    else:
                        normalized_row[code] = f"{normalized_row[code]}; {value}"
            for code in language_columns:
                normalized_row.setdefault(code, '')
            rows.append(normalized_row)
            lookup[sci] = normalized_row
    return rows, language_columns, lookup


def ensure_language_column(code: str, rows: list[dict], columns: list[str], column_set: set[str]) -> str:
    canonical = code.strip().lower()
    if not canonical:
        return canonical
    if canonical in column_set:
        return canonical
    columns.append(canonical)
    column_set.add(canonical)
    for row in rows:
        row.setdefault(canonical, '')
    return canonical


def write_output(rows: list[dict], columns: list[str]) -> None:
    fieldnames = ['Scientific Name'] + columns
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            write_row = {key: row.get(key, '') for key in fieldnames}
            writer.writerow(write_row)


def format_duration(seconds: float) -> str:
    total = int(round(seconds))
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    parts = []
    if hours:
        parts.append(f"{hours}h")
    if minutes or hours:
        parts.append(f"{minutes}m")
    parts.append(f"{secs}s")
    return ' '.join(parts)


def fetch_with_retries(url: str) -> requests.Response | None:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, headers=HEADERS, timeout=FETCH_TIMEOUT)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as exc:
            print(f"Request attempt {attempt}/{MAX_RETRIES} failed for {url}: {exc}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
    return None


def get_bird_data(sci_name):
    """Returns a dictionary of language names for the requested scientific name."""
    search_url = f"{BASE_URL}search.jsp?qstr={sci_name.replace(' ', '+')}&lang=EN"
    bird_dict = {"Scientific Name": sci_name}

    try:
        response = fetch_with_retries(search_url)
        if not response:
            print(f"Skipping {sci_name}: search page failed after {MAX_RETRIES} attempts.")
            return bird_dict
        soup = BeautifulSoup(response.content, 'html.parser')

        if "species.jsp" not in response.url:
            links = soup.find_all('a', href=re.compile(r"changespecies"))
            if not links:
                return bird_dict  # Not found

            match = re.search(r"changespecies\('([A-F0-9]+)'\)", links[0]['href'])
            if not match:
                return bird_dict  # ID failed

            avibase_id = match.group(1)
            species_url = f"{BASE_URL}species.jsp?avibaseid={avibase_id}&lang=EN"
            species_response = fetch_with_retries(species_url)
            if not species_response:
                print(f"Skipping {sci_name}: species page failed after {MAX_RETRIES} attempts.")
                return bird_dict
            response = species_response
            soup = BeautifulSoup(response.content, 'html.parser')

        language_container = None
        for div in soup.find_all('div', class_='col-lg-7'):
            if div.find('span', style=LANG_NAME_SPAN):
                language_container = div
                break

        if language_container:
            for b_tag in language_container.find_all('b'):
                lang = b_tag.get_text(strip=True).replace(':', '')
                if not lang:
                    continue

                name_span = None
                node = b_tag.next_sibling
                while node:
                    if isinstance(node, Tag):
                        if node.name == 'b':
                            break
                        style = node.get('style', '')
                        if node.name == 'span' and LANG_NAME_SPAN.search(style):
                            name_span = node
                            break
                    node = node.next_sibling

                if not name_span:
                    continue

                name = name_span.get_text(strip=True)
                if lang in bird_dict:
                    bird_dict[lang] = f"{bird_dict[lang]}; {name}"
                else:
                    bird_dict[lang] = name
    except Exception as e:
        print(f"Error: {e}")

    return bird_dict


def main():
    parser = argparse.ArgumentParser(description="Scrape bird names in chunks and append to the master CSV.")
    parser.add_argument("input_csv", help="Input CSV file with scientific names and translation columns.")
    args = parser.parse_args()

    try:
        input_rows, input_lang_fields, sci_field = load_input_rows(args.input_csv)
    except FileNotFoundError:
        print(f"Error: Could not find file '{args.input_csv}'")
        return

    if not input_rows:
        print("No species defined in the input file.")
        return

    state = load_state(args.input_csv)
    start_index = state.get('next_index', 0)
    if start_index >= len(input_rows):
        print("All species already processed.")
        return

    rows, language_columns, lookup = load_existing_output()
    language_column_set = {col.lower() for col in language_columns}

    for code, _ in input_lang_fields:
        ensure_language_column(code, rows, language_columns, language_column_set)

    chunk_end = min(start_index + CHUNK_SIZE, len(input_rows))
    print(f"Processing species {start_index + 1} through {chunk_end} of {len(input_rows)}...")

    processed_count = 0
    total_duration = 0.0

    for idx in range(start_index, chunk_end):
        item = input_rows[idx]
        sci_name = item.get(sci_field, '').strip()
        if not sci_name:
            continue

        processed_count += 1
        iteration_start = time.time()
        progress_label = f"[{idx + 1}/{len(input_rows)}]"
        print(f"{progress_label} Querying {sci_name}...")

        target_row = lookup.get(sci_name)
        if not target_row:
            target_row = {'Scientific Name': sci_name}
            for code in language_columns:
                target_row.setdefault(code, '')
            rows.append(target_row)
            lookup[sci_name] = target_row
        else:
            for code in language_columns:
                target_row.setdefault(code, '')

        for code, header in input_lang_fields:
            value = item.get(header, '')
            if not value:
                continue
            normalized = value.strip()
            if not normalized:
                continue
            if is_blank(target_row.get(code)):
                target_row[code] = normalized

        bird_data = get_bird_data(sci_name)
        for lang_name, translation in bird_data.items():
            if lang_name == 'Scientific Name' or is_blank(translation):
                continue
            code = language_name_to_code(lang_name)
            if not code:
                continue
            ensure_language_column(code, rows, language_columns, language_column_set)
            if is_blank(target_row.get(code)):
                target_row[code] = translation

        languages_count = sum(1 for key, value in target_row.items()
                              if key != 'Scientific Name' and not is_blank(value))
        print(f"{progress_label} {sci_name}: {languages_count} languages captured.")
        time.sleep(1.5)
        total_duration += time.time() - iteration_start

    write_output(rows, language_columns)
    save_state(args.input_csv, chunk_end)
    if processed_count:
        avg = total_duration / processed_count
        remaining = max(0, len(input_rows) - chunk_end)
        eta = remaining * avg
        print(f"Chunk complete. Progress saved, next index {chunk_end}."
              f" Chunk took {format_duration(total_duration)} for {processed_count} species (avg {avg:.1f}s)."
              f" Remaining input ETA: {format_duration(eta)}.")
    else:
        print(f"Chunk complete. Progress saved, next index {chunk_end}. No valid species processed in this chunk.")


if __name__ == "__main__":
    main()
