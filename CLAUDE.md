# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python 3.13 pipeline that synchronizes Czech National Library (NK ČR) authority records with Wikidata. It runs as the bot account **Frettiebot**. The pipeline converts MARC XML authority dumps into CSV (via Catmandu, an external Perl tool), then processes each record to add/update Wikidata items with identifiers, occupations, places, dates, and languages.

## Running the Pipeline

```bash
# Activate virtualenv
source .venv/bin/activate

# Run the main pipeline (requires output.csv from Catmandu)
python main.py --input output.csv

# Run the test pipeline (requires test.csv from Catmandu, mostly one line)
python main.py --input test.csv
# better to setup in config.py
## debug=True
## use_json_database=True (use JSON files)

# Full pipeline: download XML, convert to CSV, run Python
./local_run.sh
```

The `local_run.sh` script downloads `aut.xml.gz` from NK ČR, extracts it, converts via Catmandu to `output.csv`, then runs `main.py`. Note: it uses hardcoded paths to `/Users/jirisedlacek/`.

## Running Tests

```bash
pytest test_cleaners.py
pytest test_property_processor.py -v
```

Note: `test_property_processor.py` makes live Wikidata API calls (tests against real item Q555628).

## Key Dependencies

WikibaseIntegrator (0.12.13.dev0), Pywikibot, pandas, python-rapidjson. No `requirements.txt` exists — dependencies are installed directly in `.venv/`.

## Architecture

### Data Flow

```
NK ČR MARC XML → Catmandu (Perl) → output.csv → main.py → Wikidata API
```

### Core Modules

- **`main.py`** — Entry point. Initializes WBI login, loads data via `Loader`, iterates CSV rows, delegates property processing to `Processor`, and writes changed items to Wikidata with `Czech-Authorities-Sync` tag.
- **`sources.py` (`Loader`)** — Loads all reference data at startup: SPARQL queries for existing Wikidata items (non-deprecated NK ČR authorities, occupations, languages, places, field-of-work), language dictionary from CSV, and the NK ČR CSV in 30,000-row chunks.
- **`processor.py` (`Processor`)** — Coordinates processing of a single authority record. Dispatches to specialized property processors based on MARC field column. Checks blacklists and instance types before processing.
- **`config.py` (`Config`)** — Central configuration: QID blacklist, excluded occupations/instances, and the MARC-to-Wikidata property mapping.
- **`cleaners.py`** — Validation and data cleaning: QID/ORCID/ISNI format validation, occupation/place resolution via cached SPARQL lookups, date parsing from MARC fields.
- **`tools.py`** — Wikibase utility functions: SPARQL execution, claim extraction, NK ČR ID insertion, date formatting, bot password loading, debug logging.

### Property Processors (`property_processor/`)

Each MARC field has a dedicated processor inheriting from `PropertyProcessor` (base class):

| Processor | MARC Field | Wikidata Property |
|-----------|-----------|-------------------|
| `PropertyProcessorOne` | `0247a-isni`, `0247a-orcid` | P213, P496 |
| `PropertyProcessor374a` | `374a` (occupation) | P106 |
| `PropertyProcessor372a` | `372a` (field of work) | P101 |
| `PropertyProcessor370a` | `370a` (birthplace) | P19 |
| `PropertyProcessor370b` | `370b` (death place) | P20 |
| `PropertyProcessor370f` | `370f` (work location) | P937 |
| `PropertyProcessor377a` | `377a` (language) | P1412 |
| `PropertyProcessorDates` | `046f`, `046g`, `678a` | P569, P570 |

### Processing Logic (per CSV row)

1. If the row has a QID in `0247a-wikidata`, fetch the Wikidata item
2. Check instance type (P31) — skip non-person types (books, organizations, etc.)
3. Check QID blacklist — skip known problematic items
4. Add NK ČR authority ID (P691) if not already present
5. Process each property group: identifiers → occupations/fields → languages → places → dates
6. If any claims were added (new claim has no `id`), write the item with edit summary "Update NK ČR – [properties]"
7. Skip writing if the NK ČR authority is deprecated on the item

### Caching

The pipeline caches large SPARQL query results as JSON files: `non_deprecated_items.json`, `languages.json`, `occupations.json`, `field_of_work_and_occupation.json`. These are loaded at startup and can be 50-120MB each.

## Important Conventions

- Bot credentials are stored in `bot_password` (not committed to git)
- The `Config.debug` flag (default `False`) prevents writes to Wikidata when `True`
- All Wikidata edits use the tag `Czech-Authorities-Sync` and `is_bot=True`
- SPARQL queries target `https://query-main.wikidata.org/sparql`
- The project mixes Czech and English in comments and variable names
