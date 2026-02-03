# Code Analysis Report: nkcr_catmandu

## Summary

| Domain | Score | Findings |
|--------|-------|----------|
| **Code Quality** | 4/10 | Extensive duplication, dead code, module-level mutable state |
| **Security** | 3/10 | Hardcoded credentials in test file, SPARQL injection surface, no input sanitization at boundaries |
| **Performance** | 5/10 | Reasonable chunking strategy, but redundant SPARQL queries and no parallelism |
| **Architecture** | 5/10 | Decent property processor pattern, but tightly coupled modules with unclear boundaries |

---

## CRITICAL — Security

### 1. ~~Hardcoded Bot Password in Test File~~ FIXED
**File:** `test_property_processor.py:11`
**Severity:** CRITICAL

Replaced with `get_bot_password('bot_password')` to read from the `bot_password` file. The credential should still be rotated since the old password remains in git history.

### 2. ~~SPARQL Injection via String Concatenation~~ FIXED
**File:** `tools.py:272`, `tools.py:331`, `tools.py:441`, `tools.py:525`, `tools.py:619`, `tools.py:812-827`, `tools.py:1019`
**Severity:** MEDIUM

The `clean_qid` regex was anchored to `^Q[0-9]+$` (full match), rejecting any QID with trailing characters. `limit`/`offset` parameters are integers passed via `str()` — low risk.

### 3. ~~File Handles Not Properly Managed~~ FIXED
**File:** `tools.py:43-49`, `tools.py:62-63`, `tools.py:76-78`, `tools.py:903`
**Severity:** LOW

All file handles converted to `with` statements.

---

## HIGH — Code Quality

### 4. ~~Four Nearly Identical Property Processors (370a, 370b, 370f, 377a)~~ FIXED
**Files:** `property_processor_370a.py`, `property_processor_370b.py`, `property_processor_370f.py`, `property_processor_377a.py`
**Severity:** HIGH (maintainability)

These four files have **identical** `process()` methods — the same loop, same conditions, same call to `add_new_field_to_item_wbi`. They differ only in class name and docstring. This is pure copy-paste duplication.

**Recommendation:** Collapse into a single class (e.g., `PropertyProcessorSimpleList`) used for all four MARC fields, or just use the base class with a default `process()` method.

### 5. ~~Massive Duplication in SPARQL Query Functions~~ FIXED
**File:** `tools.py:255-1060`
**Severity:** HIGH

`get_all_non_deprecated_items`, `get_all_non_deprecated_items_occupation`, `get_all_non_deprecated_items_field_of_work_and_occupation`, `get_all_non_deprecated_items_places`, and `get_all_non_deprecated_items_languages` all follow the exact same pattern:
1. Build a SPARQL query string
2. Execute it with identical error handling (4 identical exception blocks)
3. Loop through results with identical `if/else` dictionary-building logic

~800 lines of code that could be ~150 with a single parameterized function.

### 6. ~~Module-Level Mutable Global State~~ FIXED
**File:** `cleaners.py:11-17`
**Severity:** HIGH

Replaced with explicit dependency injection via `PipelineContext` class (`context.py`). All shared state is now passed explicitly through the pipeline.

### 7. ~~`resolve_exist_claims` Uses Chained `if` Instead of `elif`~~ FIXED
**File:** `cleaners.py:438-461`
**Severity:** MEDIUM

Replaced with dictionary lookup, consistent with `prepare_column_of_content` pattern.

### 8. ~~Dead/Commented-Out Code Throughout~~ FIXED
**Files:** `main.py`, `tools.py`, `cleaners.py`, `processor.py`
**Severity:** MEDIUM

Large blocks of commented-out pywikibot code, unused variables (`d = ''` in `processor.py:388`), unused imports, and unreachable except clauses (e.g., `tools.py:285` — `requests.exceptions.ConnectionError` after bare `except Exception`).

### 9. ~~`type(x) is None` Bug~~ FIXED
**File:** `tools.py:466`, `tools.py:548`, `tools.py:643`, `tools.py:1033`
**Severity:** MEDIUM (logic bug)

```python
if type(data_non_deprecated) is None:
```

This is always `False`. `type(None)` is `<class 'NoneType'>`, not `None`. The intended check is `if data_non_deprecated is None:`.

### 10. ~~`type(x) == str` Instead of `isinstance()`~~ FIXED
**Files:** `cleaners.py:104`, `cleaners.py:149`, `cleaners.py:192`, `cleaners.py:309`, `cleaners.py:334`
**Severity:** LOW

Using `type(x) == str` instead of `isinstance(x, str)` prevents subclass matching and is non-idiomatic Python.

### 11. ~~Import Inside Function~~ FIXED
**File:** `tools.py:1098`, `tools.py:1135`
**Severity:** LOW

`import re` is done inside `first_name()` and `last_name()` despite `re` being imported at module level in `cleaners.py`. Should be a top-level import.

---

## MEDIUM — Performance

### 12. Redundant SPARQL Queries at Startup
**File:** `sources.py:118-156`
**Severity:** MEDIUM

The `Loader.load()` method executes 6 separate large SPARQL queries sequentially, each iterating through all NK ČR items on Wikidata. Several of these could potentially be combined (e.g., occupations + field of work + places could be a single query with more OPTIONAL clauses), reducing the number of round-trips to the SPARQL endpoint.

### 13. `is_item_subclass_of_wbi` Makes Individual SPARQL Queries
**File:** `tools.py:791-867`
**Severity:** MEDIUM

For every occupation in every CSV row, this function fires a SPARQL query to check subclass relationships. Even with the caching dictionary, the first encounter of each occupation triggers a network round-trip. With thousands of unique occupations, this creates significant latency.

**Recommendation:** Pre-fetch the subclass hierarchy for `Q12737077` (occupation) in bulk at startup, similar to how other lookups are cached.

### 14. JSON Cache Files Written Every Run
**File:** `tools.py:930-933`
**Severity:** LOW

When `use_json_database=False` (the default), the code always queries SPARQL and then writes the full result to JSON. On subsequent runs, it queries again (ignoring the JSON file unless `use_json_database=True`). The caching strategy is confusing — the JSON is written but only used when explicitly opted in.

---

## MEDIUM — Architecture

### 15. `Processor` Class is a Mutable State Bag
**File:** `processor.py`
**Severity:** MEDIUM

`Processor` requires 7 setter calls (`set_nkcr_aut`, `set_qid`, `set_wbi`, `reset_instances_from_item`, `set_item`, `set_row`, `set_enabled_columns`) before each `process_occupation_type` call. This is error-prone — forgetting a setter or calling them in wrong order produces subtle bugs.

**Recommendation:** Pass all required data as method parameters or use a dataclass/namedtuple for the per-row context.

### 16. Mixed Use of Pywikibot and WikibaseIntegrator
**Files:** `tools.py`, `mySparql.py`, `pywikibot_extension.py`
**Severity:** MEDIUM

The project uses two competing Wikidata libraries simultaneously:
- **WikibaseIntegrator** — for item reads/writes (the primary library)
- **Pywikibot** — for SPARQL queries (via `mySparql.MySparqlQuery` which subclasses `pywikibot.data.sparql.SparqlQuery`) and in the legacy `pywikibot_extension.py`

This dual dependency increases maintenance burden and creates confusion. `pywikibot_extension.py` appears to be legacy code — its `MyDataSite` and `MyItemPage` classes are not referenced from any other module.

### 17. `main.py` Has Too Much Inline Logic
**File:** `main.py:63-284`
**Severity:** MEDIUM

The main processing loop is a 220-line deeply nested block (4-5 levels of indentation) with inline exception handling for 8 different exception types. The loop body mixes concerns: CSV iteration, item fetching, blacklist checking, property processing, change detection, and Wikidata writes.

---

## LOW — Test Coverage

### 18. Minimal Test Coverage
**Files:** `test_cleaners.py`, `test_property_processor.py`
**Severity:** LOW

- `test_cleaners.py` — Tests 4 functions (QID, ORCID, ISNI, comma cleaning). Does not test occupation/place/language/date preparation functions.
- `test_property_processor.py` — Tests against live Wikidata API, making tests non-deterministic and slow. No mocking.
- No tests for: `main.py`, `processor.py`, `tools.py` (any function), `sources.py`, `cleaners.resolve_exist_claims`, `cleaners.prepare_column_of_content`, date parsing.
- `test_cleaners.py:28` calls `prepare_orcid_from_nkcr(orcid)` with 1 arg, but the function signature requires 2 args (`orcid, column`). This test likely fails or the function signature was changed.

---

## Prioritized Recommendations

1. **~~Rotate the bot password~~** ~~immediately and remove it from `test_property_processor.py`~~ — FIXED (password removed from code; rotation still recommended)
2. **~~Fix the `type(x) is None` bug~~** ~~— this silently skips error handling for failed SPARQL queries~~ — FIXED
3. **Consolidate the 4 identical property processors** into one class
4. **Parameterize SPARQL query functions** to eliminate ~650 lines of duplication in `tools.py`
5. **~~Replace module-level global state~~** ~~in `cleaners.py` with a context object passed through the pipeline~~ — FIXED
6. **~~Validate SPARQL inputs~~** ~~with strict regex to prevent injection~~ — FIXED
7. **Add unit tests** for date parsing, occupation resolution, and the processor logic with mocked Wikidata responses
