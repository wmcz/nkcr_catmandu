# Oprava načítání pojmů, parsování polí a report nenalezených

Datum: 2026-08-16
Navazuje na: [2026-08-16-analyza-nenalezenych-pojmu.md](2026-08-16-analyza-nenalezenych-pojmu.md)
Stav: hotovo, testy procházejí

## 1. Načítání slovníku pojmů (`tools.py`, `sources.py`)

`get_occupations()` + `load_sparql_query_by_chunks()` nahrazeny funkcemi
`load_names_from_csv()`, `load_names_to_qid()` a `load_name_dictionary()`.

Původní řešení tahalo z Wikidat všech ~200 tisíc dvojic „P1810 řetězec → QID“
stránkováním přes `LIMIT/OFFSET` **bez `ORDER BY`**. SPARQL v takovém případě
negarantuje pořadí, takže každý běh dostal jinou podmnožinu — v logu velikost
slovníku kolísala mezi 199 741 a 202 423 a vypadlé termíny se hlásily jako
nenalezené, ačkoli na Wikidatech napojené jsou. Doplnit `ORDER BY` nešlo:
endpoint pak musí seřadit celý výsledek a **spadne na 60s timeout** (ověřeno
dotazem, 65 s bez odpovědi).

Nově se ptáme jen na termíny, které jsou v CSV, dávkami po 1000 přes `VALUES`.
Naměřeno proti query-main: 100 jmen 1,4 s, 1000 jmen 1,5 s — cena je v requestu,
ne ve velikosti dávky. Na `test.csv` se napojí 6/6 termínů, na vzorku 300
termínů z `output.csv` 296/300 (zbytek jsou reálně chybějící varianty:
„Dvůr Králové (Česko)“, „Bratislava“, „Eschwege (Německo)“, „Podbrdsko (Česko)“).

## 2. Kódování POST dotazů (`tools.py`)

`execute_sparql_post()` posílal tělo bez uvedení charsetu. Endpoint ho pak čte
jako Latin-1 a **každý literál s diakritikou přestane matchovat** — „filozofie“
se našla, „Příbram (Česko)“ ne, ačkoli přes GET stejný dotaz odpověď vrátil.
Doplněno `charset=utf-8` do `Content-Type`. U dosavadních dotazů to nevadilo,
protože posílaly jen ASCII NK ČR identifikátory.

## 3. Parsování polí (`cleaners.py`)

Catmandu spojuje opakovaná podpole znakem `$` a opakovaná pole znakem `|`, takže
jedna buňka může obsahovat oba oddělovače. Kód dělil vždy jen podle jednoho:

- 377a se dělilo jen podle `$` → klíče typu `cze|cze`, `eng|eng`, `eng|cze`,
  které nemohly nikdy sednout (kódy `cze` i `eng` v `jazyky.csv` jsou);
- 370a/b/f dělilo podle `|` jen tehdy, když v hodnotě nebyl `$`.

Zavedeny sdílené funkce `split_field_values()`, `split_occupation_names()`,
`split_language_codes()`, `split_place_names()`. Používá je jak zpracování
záznamů, tak loader — díky tomu se dotazujeme přesně na ty tvary, které se pak
vyhledávají.

## 4. Report nenalezených (`context.py`, `tools.py`, `main.py`)

- Přibyl tracking jazyků (`not_found_languages`) — dosud se jen logovaly.
- Místo `print()` dvou neseřazených dictů se zapisuje
  `not_found_occupations.csv`, `not_found_places.csv`, `not_found_languages.csv`,
  seřazené podle počtu výskytů sestupně. Do gitu nejdou (`.gitignore` má `*.csv`).

## Co zbývá / mimo tento repozitář

`jazyky.csv` se stahuje z `wmcz/WMCZ-scripts`, lokální úpravy přepíše. K doplnění
tam (QID ověřená proti Wikidatům 16. 8. 2026):

| kód | návrh | poznámka |
|-----|-------|----------|
| mol | Q36392 (moldavština) | má P219 `mol` |
| gag | Q33457 (gagauzština) | ISO 639-3 |
| scr | Q6654 (chorvatština) | starý kód, dnes `hrv` |
| jap | Q5287 (japonština) | nestandardní, správně `jpn` |
| esp | Q143 (esperanto) | nestandardní, správně `epo` |
| mul | — | „mnoho jazyků“ (Q20923490) není jazyk, nemapovat |

Dále: po ostrém běhu projít reporty a založit triage nad nimi (bod 4 z analýzy).

## Poznámka k testům

`pytest` končí 5 selháními v `test_tools.py` (`get_claim_from_item_by_property`,
`is_item_subclass_of`) — ty jsou **preexistující**, test volá funkce, které
v `tools.py` neexistují. Ověřeno spuštěním nad čistým stromem před změnami.
Zbytek: 60 testů prochází, z toho 21 nových v `test_name_lookup.py`.
